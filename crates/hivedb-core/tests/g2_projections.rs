mod common;

use common::{db, state_transition};
use hivedb_core::{EventInput, EventKind, HiveDB, Projection, TaskState, TaskStateState};
use proptest::prelude::*;
use serde_json::json;

#[test]
fn state_and_log_update_atomically() {
    let db = db();
    let seq = db
        .append(state_transition("A", "task-1", json!({"to": "done"})))
        .unwrap();

    let checkpoint = db.projection_checkpoint::<TaskState>().unwrap();
    assert_eq!(checkpoint, seq);

    let state: TaskStateState = db.project::<TaskState>().unwrap();
    assert_eq!(
        state.get(&"A".into(), &"task-1".into()),
        Some(&json!({"to": "done"}))
    );
}

#[test]
fn replay_from_zero_reconstructs_identical_state() {
    let db = db();
    seed_many_events(&db, 1000);
    let original_state: TaskStateState = db.project::<TaskState>().unwrap();

    // Wipe only the materialized projections, leave the log intact, and rebuild.
    db.wipe_projections_and_rebuild().unwrap();

    let reconstructed: TaskStateState = db.project::<TaskState>().unwrap();
    assert_eq!(original_state, reconstructed);
}

#[test]
fn committed_state_matches_manual_replay() {
    let db = db();
    seed_many_events(&db, 500);

    let state: TaskStateState = db.project::<TaskState>().unwrap();

    let mut replayed = TaskStateState::default();
    for seq in 1..=db.log_len().unwrap() {
        let event = db.read(seq).unwrap();
        TaskState::apply(&mut replayed, &event);
    }

    assert_eq!(state, replayed);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn projection_is_deterministic_fold(events in arb_event_sequence(20)) {
        let db1 = db();
        let db2 = db();

        for e in &events {
            db1.append(e.clone()).unwrap();
            db2.append(e.clone()).unwrap();
        }

        let state1: TaskStateState = db1.project::<TaskState>().unwrap();
        let state2: TaskStateState = db2.project::<TaskState>().unwrap();

        prop_assert_eq!(state1, state2);
    }

    #[test]
    fn wipe_and_rebuild_is_idempotent(seed in 1usize..100) {
        let db = db();
        seed_many_events(&db, seed);
        let before: TaskStateState = db.project::<TaskState>().unwrap();

        db.wipe_projections_and_rebuild().unwrap();
        let after: TaskStateState = db.project::<TaskState>().unwrap();

        prop_assert_eq!(before, after);
    }
}

fn seed_many_events(db: &HiveDB, n: usize) {
    for i in 0..n {
        let agent = if i % 2 == 0 { "A" } else { "B" };
        let stream = format!("task-{}", i % 5);
        db.append(state_transition(agent, stream.as_str(), json!({"step": i})))
            .unwrap();
    }
}

fn arb_event_sequence(max_len: usize) -> impl Strategy<Value = Vec<EventInput>> {
    prop::collection::vec(arb_event(), 0..=max_len)
}

fn arb_event() -> impl Strategy<Value = EventInput> {
    (any::<u8>(), any::<u8>(), any::<bool>()).prop_map(|(agent_idx, stream_idx, is_transition)| {
        let agent = format!("agent-{}", agent_idx % 4);
        let stream = format!("stream-{}", stream_idx % 4);
        let kind = if is_transition {
            EventKind::StateTransition
        } else {
            EventKind::Fact
        };
        EventInput::new(agent, stream, kind).with_payload(json!({"v": stream_idx}))
    })
}
