mod common;

use common::{db, fact};
use hivedb_core::{AgentId, StreamId};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

#[test]
fn distinct_agents_write_without_blocking() {
    const N: usize = 1_000;

    let db_concurrent = Arc::new(db());

    // Concurrent writes for two distinct agents.
    let start = Instant::now();
    let db_a = Arc::clone(&db_concurrent);
    let db_b = Arc::clone(&db_concurrent);
    let handle_a = thread::spawn(move || {
        for _ in 0..N {
            db_a.append(fact("A", "stream-a")).unwrap();
        }
    });
    let handle_b = thread::spawn(move || {
        for _ in 0..N {
            db_b.append(fact("B", "stream-b")).unwrap();
        }
    });
    handle_a.join().unwrap();
    handle_b.join().unwrap();
    let concurrent_duration = start.elapsed();

    assert_eq!(db_concurrent.log_len().unwrap(), (2 * N) as u64);

    // Single-writer baseline: same number of events written sequentially.
    let db_base = db();
    let start_base = Instant::now();
    for _ in 0..(2 * N) {
        db_base.append(fact("A", "stream-a")).unwrap();
    }
    let single_duration = start_base.elapsed();

    assert!(
        concurrent_duration < single_duration * 7 / 10,
        "concurrent writes should be significantly faster than single-writer: {:?} vs {:?}",
        concurrent_duration,
        single_duration
    );
}

#[test]
fn same_agent_writes_preserve_causal_order() {
    let db = db();
    let s1 = db.append(fact("A", "stream-1")).unwrap();
    let s2 = db.append(fact("A", "stream-1")).unwrap();

    let seqs: Vec<_> = db
        .read_stream(&AgentId::from("A"), &StreamId::from("stream-1"))
        .unwrap()
        .into_iter()
        .map(|e| e.seq)
        .collect();

    assert_eq!(seqs, vec![s1, s2]);
}

#[cfg(loom)]
#[test]
fn no_data_race_on_seq_assignment() {
    use hivedb_core::{EventInput, EventKind, HiveDB};

    loom::model(|| {
        let db = Arc::new(HiveDB::open_in_memory().unwrap());
        let handles: Vec<_> = (0..2)
            .map(|i| {
                let db = Arc::clone(&db);
                loom::thread::spawn(move || {
                    db.append(EventInput::new(
                        format!("A{i}"),
                        StreamId::from("task-1"),
                        EventKind::Fact,
                    ))
                    .unwrap();
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(db.log_len().unwrap(), 2);
    });
}
