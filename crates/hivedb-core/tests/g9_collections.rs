//! G10 — document collections: CRUD with optimistic versioning, secondary
//! indexes (equality + unique) and atomic batches.

use hivedb_core::{ColOp, HiveDB, PutOptions, ScanOptions};
use serde_json::{Value, json};

fn open_db(dir: &std::path::Path) -> HiveDB {
    HiveDB::open(dir).unwrap()
}

#[test]
fn put_get_roundtrip_with_versioning() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    let v1 = db
        .col_put(
            "agents",
            "a1",
            &json!({"name": "Atlas", "role": "worker"}),
            PutOptions::default(),
        )
        .unwrap();
    assert_eq!(v1, 1);

    let entry = db.col_get("agents", "a1").unwrap().unwrap();
    assert_eq!(entry.version, 1);
    assert_eq!(entry.doc["name"], "Atlas");

    let v2 = db
        .col_put(
            "agents",
            "a1",
            &json!({"name": "Atlas", "role": "coordinator"}),
            PutOptions::default(),
        )
        .unwrap();
    assert_eq!(v2, 2);
    let entry = db.col_get("agents", "a1").unwrap().unwrap();
    assert_eq!(entry.doc["role"], "coordinator");

    assert!(db.col_get("agents", "missing").unwrap().is_none());
}

#[test]
fn optimistic_version_check() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    // expected_version = 0 → create-only
    db.col_put(
        "t",
        "x",
        &json!({"a": 1}),
        PutOptions {
            expected_version: Some(0),
        },
    )
    .unwrap();
    let err = db
        .col_put(
            "t",
            "x",
            &json!({"a": 2}),
            PutOptions {
                expected_version: Some(0),
            },
        )
        .unwrap_err();
    assert!(err.to_string().contains("version conflict"), "{err}");

    // matching current version succeeds; stale version fails
    db.col_put(
        "t",
        "x",
        &json!({"a": 2}),
        PutOptions {
            expected_version: Some(1),
        },
    )
    .unwrap();
    let err = db
        .col_put(
            "t",
            "x",
            &json!({"a": 3}),
            PutOptions {
                expected_version: Some(1),
            },
        )
        .unwrap_err();
    assert!(err.to_string().contains("version conflict"), "{err}");
}

#[test]
fn delete_and_count() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    for i in 0..5 {
        db.col_put(
            "notes",
            &format!("n{i}"),
            &json!({"i": i}),
            PutOptions::default(),
        )
        .unwrap();
    }
    assert_eq!(db.col_count("notes").unwrap(), 5);

    assert!(db.col_delete("notes", "n2").unwrap());
    assert!(!db.col_delete("notes", "n2").unwrap());
    assert_eq!(db.col_count("notes").unwrap(), 4);
    assert!(db.col_get("notes", "n2").unwrap().is_none());
}

#[test]
fn scan_with_prefix_offset_limit_reverse() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    for id in ["u:1", "u:2", "u:3", "g:1", "g:2"] {
        db.col_put("things", id, &json!({"id": id}), PutOptions::default())
            .unwrap();
    }

    let all = db.col_scan("things", &ScanOptions::default()).unwrap();
    assert_eq!(all.len(), 5);
    // Ascending id order
    assert_eq!(all[0].id, "g:1");

    let users = db
        .col_scan(
            "things",
            &ScanOptions {
                prefix: Some("u:".into()),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(
        users.iter().map(|e| e.id.as_str()).collect::<Vec<_>>(),
        vec!["u:1", "u:2", "u:3"]
    );

    let page = db
        .col_scan(
            "things",
            &ScanOptions {
                offset: 1,
                limit: 2,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(
        page.iter().map(|e| e.id.as_str()).collect::<Vec<_>>(),
        vec!["g:2", "u:1"]
    );

    let rev = db
        .col_scan(
            "things",
            &ScanOptions {
                reverse: true,
                limit: 2,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(
        rev.iter().map(|e| e.id.as_str()).collect::<Vec<_>>(),
        vec!["u:3", "u:2"]
    );
}

#[test]
fn collections_are_isolated() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    db.col_put("a", "same-id", &json!({"from": "a"}), PutOptions::default())
        .unwrap();
    db.col_put("b", "same-id", &json!({"from": "b"}), PutOptions::default())
        .unwrap();

    assert_eq!(
        db.col_get("a", "same-id").unwrap().unwrap().doc["from"],
        "a"
    );
    assert_eq!(
        db.col_get("b", "same-id").unwrap().unwrap().doc["from"],
        "b"
    );
    assert_eq!(db.col_count("a").unwrap(), 1);
}

#[test]
fn secondary_index_find_by_and_maintenance() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    // Backfill: docs exist before the index is created.
    db.col_put(
        "convs",
        "c1",
        &json!({"thread_id": "t-1", "n": 1}),
        PutOptions::default(),
    )
    .unwrap();
    db.col_put(
        "convs",
        "c2",
        &json!({"thread_id": "t-1", "n": 2}),
        PutOptions::default(),
    )
    .unwrap();
    db.col_put(
        "convs",
        "c3",
        &json!({"thread_id": "t-2", "n": 3}),
        PutOptions::default(),
    )
    .unwrap();

    db.col_create_index("convs", "thread_id", false).unwrap();

    let hits = db
        .col_find_by(
            "convs",
            "thread_id",
            &Value::from("t-1"),
            &ScanOptions::default(),
        )
        .unwrap();
    assert_eq!(hits.len(), 2);

    // Update moves the doc to another value; index follows.
    db.col_put(
        "convs",
        "c2",
        &json!({"thread_id": "t-2", "n": 2}),
        PutOptions::default(),
    )
    .unwrap();
    let t1 = db
        .col_find_by(
            "convs",
            "thread_id",
            &Value::from("t-1"),
            &ScanOptions::default(),
        )
        .unwrap();
    let t2 = db
        .col_find_by(
            "convs",
            "thread_id",
            &Value::from("t-2"),
            &ScanOptions::default(),
        )
        .unwrap();
    assert_eq!(t1.len(), 1);
    assert_eq!(t2.len(), 2);

    // Delete removes the index entry.
    db.col_delete("convs", "c3").unwrap();
    let t2 = db
        .col_find_by(
            "convs",
            "thread_id",
            &Value::from("t-2"),
            &ScanOptions::default(),
        )
        .unwrap();
    assert_eq!(
        t2.iter().map(|e| e.id.as_str()).collect::<Vec<_>>(),
        vec!["c2"]
    );

    // find_by without an index is an explicit error.
    let err = db
        .col_find_by("convs", "n", &Value::from(1), &ScanOptions::default())
        .unwrap_err();
    assert!(err.to_string().contains("no index"), "{err}");
}

#[test]
fn unique_index_enforced_on_put_and_backfill() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    db.col_create_index("tokens", "hash", true).unwrap();
    db.col_put(
        "tokens",
        "t1",
        &json!({"hash": "abc"}),
        PutOptions::default(),
    )
    .unwrap();

    // Same value on a different id is rejected...
    let err = db
        .col_put(
            "tokens",
            "t2",
            &json!({"hash": "abc"}),
            PutOptions::default(),
        )
        .unwrap_err();
    assert!(err.to_string().contains("unique index"), "{err}");

    // ...but re-putting the same doc is fine.
    db.col_put(
        "tokens",
        "t1",
        &json!({"hash": "abc", "extra": 1}),
        PutOptions::default(),
    )
    .unwrap();

    // Backfill over duplicate data fails and leaves no index behind.
    db.col_put("dups", "d1", &json!({"k": "same"}), PutOptions::default())
        .unwrap();
    db.col_put("dups", "d2", &json!({"k": "same"}), PutOptions::default())
        .unwrap();
    let err = db.col_create_index("dups", "k", true).unwrap_err();
    assert!(err.to_string().contains("unique index"), "{err}");
    let err = db
        .col_find_by("dups", "k", &Value::from("same"), &ScanOptions::default())
        .unwrap_err();
    assert!(err.to_string().contains("no index"), "{err}");
}

#[test]
fn batch_is_atomic() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    db.col_put("acc", "a", &json!({"balance": 10}), PutOptions::default())
        .unwrap();

    // A failing op (version conflict) aborts the whole batch.
    let err = db
        .col_batch(&[
            ColOp::Put {
                collection: "acc".into(),
                id: "a".into(),
                doc: json!({"balance": 0}),
                expected_version: None,
            },
            ColOp::Put {
                collection: "acc".into(),
                id: "b".into(),
                doc: json!({"balance": 10}),
                expected_version: Some(7), // wrong on purpose
            },
        ])
        .unwrap_err();
    assert!(err.to_string().contains("version conflict"), "{err}");

    // Nothing committed: "a" keeps its balance, "b" does not exist.
    assert_eq!(db.col_get("acc", "a").unwrap().unwrap().doc["balance"], 10);
    assert!(db.col_get("acc", "b").unwrap().is_none());

    // A valid batch commits everything at once.
    db.col_batch(&[
        ColOp::Put {
            collection: "acc".into(),
            id: "a".into(),
            doc: json!({"balance": 0}),
            expected_version: None,
        },
        ColOp::Delete {
            collection: "acc".into(),
            id: "missing".into(),
        },
        ColOp::Put {
            collection: "acc".into(),
            id: "b".into(),
            doc: json!({"balance": 10}),
            expected_version: None,
        },
    ])
    .unwrap();
    assert_eq!(db.col_get("acc", "a").unwrap().unwrap().doc["balance"], 0);
    assert_eq!(db.col_get("acc", "b").unwrap().unwrap().doc["balance"], 10);
}

#[test]
fn collections_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();

    {
        let db = open_db(dir.path());
        db.col_create_index("users", "email", true).unwrap();
        db.col_put(
            "users",
            "u1",
            &json!({"email": "a@b.co", "name": "Ana"}),
            PutOptions::default(),
        )
        .unwrap();
    }

    let db = open_db(dir.path());
    let entry = db.col_get("users", "u1").unwrap().unwrap();
    assert_eq!(entry.doc["name"], "Ana");
    assert_eq!(entry.version, 1);

    // Index definitions are persistent: uniqueness still enforced after reopen.
    let err = db
        .col_put(
            "users",
            "u2",
            &json!({"email": "a@b.co"}),
            PutOptions::default(),
        )
        .unwrap_err();
    assert!(err.to_string().contains("unique index"), "{err}");

    let hits = db
        .col_find_by(
            "users",
            "email",
            &Value::from("a@b.co"),
            &ScanOptions::default(),
        )
        .unwrap();
    assert_eq!(hits.len(), 1);
}

#[test]
fn non_scalar_and_missing_fields_are_not_indexed() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    db.col_create_index("mixed", "tag", false).unwrap();
    db.col_put("mixed", "m1", &json!({"tag": "x"}), PutOptions::default())
        .unwrap();
    db.col_put("mixed", "m2", &json!({"tag": null}), PutOptions::default())
        .unwrap();
    db.col_put(
        "mixed",
        "m3",
        &json!({"tag": ["a", "b"]}),
        PutOptions::default(),
    )
    .unwrap();
    db.col_put("mixed", "m4", &json!({"other": 1}), PutOptions::default())
        .unwrap();

    let hits = db
        .col_find_by("mixed", "tag", &Value::from("x"), &ScanOptions::default())
        .unwrap();
    assert_eq!(
        hits.iter().map(|e| e.id.as_str()).collect::<Vec<_>>(),
        vec!["m1"]
    );

    // Numbers and bools are valid index values.
    db.col_create_index("mixed", "n", false).unwrap();
    db.col_put("mixed", "m5", &json!({"n": 42}), PutOptions::default())
        .unwrap();
    let hits = db
        .col_find_by("mixed", "n", &Value::from(42), &ScanOptions::default())
        .unwrap();
    assert_eq!(hits.len(), 1);
}
