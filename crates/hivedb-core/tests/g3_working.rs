mod common;

use common::{db, ttl_ms, value};
use serde_json::json;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[test]
fn working_memory_expires_and_is_not_logged() {
    let db = db();
    db.working_set("A", "draft", json!({"text": "hello"}), ttl_ms(50));

    assert!(db.working_get("A", "draft").is_some());
    assert_eq!(db.log_len().unwrap(), 0);

    thread::sleep(Duration::from_millis(80));

    assert!(db.working_get("A", "draft").is_none());
    assert_eq!(db.log_len().unwrap(), 0);
}

#[test]
fn working_memory_concurrent_writes_no_corruption() {
    let db = Arc::new(db());
    let handles: Vec<_> = (0..16)
        .map(|i| {
            let db = db.clone();
            thread::spawn(move || {
                for j in 0..1000 {
                    db.working_set("A", format!("k{i}-{j}"), value(), ttl_ms(10_000));
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(db.working_keys("A").len(), 16 * 1000);
}

#[test]
fn working_memory_keys_are_isolated_by_agent() {
    let db = db();
    db.working_set("A", "k1", json!(1), None);
    db.working_set("B", "k1", json!(2), None);

    assert_eq!(db.working_keys("A"), vec!["k1".to_string()]);
    assert_eq!(db.working_keys("B"), vec!["k1".to_string()]);
}
