mod common;

use common::{db, fact, invalidate};
use hivedb_core::{CurrentFactsState, HiveDB};
use serde_json::json;
use tempfile::tempdir;

#[test]
fn seq_is_monotonic_and_engine_assigned() {
    let db = db();
    let s1 = db.append(fact("A", "s1")).unwrap();
    let s2 = db.append(fact("A", "s1")).unwrap();
    let s3 = db.append(fact("B", "s2")).unwrap();

    assert!(s1 < s2 && s2 < s3);
    assert_eq!(s1, 1);
}

#[test]
fn log_has_no_mutation_api() {
    let db = db();
    let seq = db.append(fact("A", "s1")).unwrap();

    let ev = db.read(seq).unwrap();
    assert_eq!(ev.seq, seq);
    assert_eq!(ev.kind_tag(), "Fact");

    // HiveDB exposes no update/delete API; this test simply documents the contract.
    assert_eq!(db.log_len().unwrap(), 1);
}

#[test]
fn correction_is_a_new_invalidating_event() {
    let db = db();
    let fact_seq = db
        .append(fact("A", "s1").with_payload(json!({"price": 100})))
        .unwrap();

    let inval_seq = db.append(invalidate("A", fact_seq)).unwrap();

    // Original fact is still in the log, untouched.
    let ev = db.read(fact_seq).unwrap();
    assert_eq!(ev.payload, json!({"price": 100}));

    // But CurrentFacts projection no longer considers it valid.
    let current: CurrentFactsState = db.project::<hivedb_core::CurrentFacts>().unwrap();
    assert!(!current.contains(fact_seq));

    // The invalidating event has a higher sequence number.
    assert!(inval_seq > fact_seq);
}

#[test]
fn next_seq_survives_reopen_without_rescan() {
    let dir = tempdir().unwrap();

    let db = HiveDB::open(dir.path()).unwrap();
    let s1 = db.append(fact("A", "s1")).unwrap();
    drop(db);

    let db2 = HiveDB::open(dir.path()).unwrap();
    let s2 = db2.append(fact("A", "s1")).unwrap();

    assert_eq!(s2, s1 + 1);
    assert_eq!(db2.last_seq().unwrap(), s2);
}

#[test]
fn reopen_without_meta_falls_back_to_scan() {
    let dir = tempdir().unwrap();

    {
        let db = HiveDB::open(dir.path()).unwrap();
        db.append(fact("A", "s1")).unwrap();
    }

    // Simulate an older database that has no `next_seq` meta entry.
    let global_path = dir.path().join("shards/_global.redb");
    let redb = redb::Database::create(&global_path).unwrap();
    {
        let txn = redb.begin_write().unwrap();
        {
            let mut table = txn
                .open_table(redb::TableDefinition::<&str, u64>::new("meta"))
                .unwrap();
            table.remove("next_seq").unwrap();
        }
        txn.commit().unwrap();
    }
    drop(redb);

    let db = HiveDB::open(dir.path()).unwrap();
    let seq = db.append(fact("A", "s1")).unwrap();
    assert_eq!(seq, 2);
}
