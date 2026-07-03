mod common;

use common::{db, fact, invalidate};
use hivedb_core::CurrentFactsState;
use serde_json::json;

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
