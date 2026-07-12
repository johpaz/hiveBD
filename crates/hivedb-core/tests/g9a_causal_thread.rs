use hivedb_core::{AgentId, AnomalyKind, EventInput, EventKind, HiveDB, StreamId, ToolOutcome};
use proptest::prelude::*;
use serde_json::json;
use uuid::Uuid;

fn decision(
    agent: impl Into<AgentId>,
    description: impl Into<String>,
    caused_by: Option<u64>,
) -> EventInput {
    let input = EventInput::new(agent, StreamId::from("task-1"), EventKind::StateTransition)
        .with_payload(json!({ "description": description.into() }));
    match caused_by {
        Some(seq) => input.with_causation(seq),
        None => input,
    }
}

fn decision_with_correlation(
    agent: impl Into<AgentId>,
    description: impl Into<String>,
    correlation: Uuid,
) -> EventInput {
    let mut input = EventInput::new(agent, StreamId::from("task-1"), EventKind::StateTransition)
        .with_payload(json!({ "description": description.into() }));
    input.correlation = Some(correlation);
    input
}

fn tool_call_caused_by(
    agent: impl Into<AgentId>,
    tool: impl Into<String>,
    caused_by: u64,
) -> EventInput {
    EventInput::new(
        agent,
        StreamId::from("task-1"),
        EventKind::ToolCall { tool: tool.into() },
    )
    .with_payload(json!({ "outcome": "Ok" }))
    .with_causation(caused_by)
}

fn tool_call_with_outcome(
    agent: impl Into<AgentId>,
    tool: impl Into<String>,
    outcome: ToolOutcome,
    caused_by: u64,
) -> EventInput {
    let payload = match outcome {
        ToolOutcome::Ok => json!({ "outcome": "Ok" }),
        ToolOutcome::Timeout => json!({ "outcome": "Timeout" }),
        ToolOutcome::Err(ref msg) => json!({ "outcome": { "Err": msg } }),
    };
    EventInput::new(
        agent,
        StreamId::from("task-1"),
        EventKind::ToolCall { tool: tool.into() },
    )
    .with_payload(payload)
    .with_causation(caused_by)
}

fn intent_logged(agent: impl Into<AgentId>, intent: impl Into<String>) -> EventInput {
    let agent = agent.into();
    EventInput::new(
        agent.clone(),
        StreamId::from("task-1"),
        EventKind::IntentLogged {
            actor: agent,
            intent: intent.into(),
            authorized_by: None,
        },
    )
}

#[allow(dead_code)]
fn error_caused_by(
    agent: impl Into<AgentId>,
    description: impl Into<String>,
    caused_by: u64,
) -> EventInput {
    EventInput::new(agent, StreamId::from("task-1"), EventKind::StateTransition)
        .with_payload(json!({ "description": description.into() }))
        .with_causation(caused_by)
}

#[test]
fn causal_thread_follows_causation_links() {
    let db = HiveDB::open_temp().unwrap();

    let d1 = db
        .append(decision("Architect", "usar microservicios", None))
        .unwrap();
    let t1 = db
        .append(tool_call_caused_by("Architect", "read_file", d1))
        .unwrap();
    let d2 = db
        .append(decision("Backend", "crear servicio de pagos", Some(t1)))
        .unwrap();
    let _t2 = db
        .append(tool_call_caused_by("Backend", "write_file", d2))
        .unwrap();

    let thread = db.causal_thread("task-1").unwrap();

    assert_eq!(thread.decisions.len(), 2);
    assert_eq!(thread.tool_calls.len(), 2);
    assert_eq!(thread.decisions[0].caused, vec![t1]);
    assert_eq!(thread.decisions[1].caused_by, Some(t1));
    assert_eq!(thread.tool_calls[1].caused_by, Some(d2));
}

#[test]
fn causal_thread_detects_error_loops() {
    let db = HiveDB::open_temp().unwrap();

    for _ in 0..3 {
        let d = db
            .append(decision("Backend", "compilar módulo", None))
            .unwrap();
        db.append(tool_call_with_outcome(
            "Backend",
            "cargo_build",
            ToolOutcome::Err("E0432".into()),
            d,
        ))
        .unwrap();
    }

    let thread = db.causal_thread("task-1").unwrap();

    assert!(!thread.anomalies.is_empty());
    let anomaly = &thread.anomalies[0];
    assert_eq!(anomaly.kind, AnomalyKind::ErrorLoop);
    assert_eq!(anomaly.repetitions, 3);
    assert_eq!(anomaly.tool, Some("cargo_build".into()));
}

#[test]
fn causal_thread_detects_objective_drift() {
    let db = HiveDB::open_temp().unwrap();

    let intent = db
        .append(intent_logged("PM", "implementar autenticación"))
        .unwrap();

    let other_corr = Uuid::new_v4();
    for _ in 0..10 {
        db.append(decision_with_correlation(
            "Backend",
            "refactorizar ORM",
            other_corr,
        ))
        .unwrap();
    }

    let thread = db.causal_thread("task-1").unwrap();

    let drift = thread
        .anomalies
        .iter()
        .find(|a| a.kind == AnomalyKind::ObjectiveDrift);
    assert!(drift.is_some());
    assert_eq!(drift.unwrap().original_intent_seq, Some(intent));
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]

    #[test]
    fn causal_thread_is_deterministic(events in arb_causal_event_sequence(20)) {
        let db1 = HiveDB::open_temp().unwrap();
        let db2 = HiveDB::open_temp().unwrap();

        for e in &events {
            db1.append(e.clone()).unwrap();
            db2.append(e.clone()).unwrap();
        }

        let t1 = db1.causal_thread("task-1").unwrap();
        let t2 = db2.causal_thread("task-1").unwrap();

        prop_assert_eq!(t1.decisions.len(), t2.decisions.len());
        prop_assert_eq!(t1.tool_calls.len(), t2.tool_calls.len());
        prop_assert_eq!(t1.anomalies.len(), t2.anomalies.len());
    }
}

#[test]
fn causal_thread_survives_projection_wipe_and_replay() {
    let dir = tempfile::tempdir().unwrap();
    let original_thread = {
        let db = HiveDB::open(dir.path()).unwrap();
        seed_causal_task(&db, "task-1", 50);
        db.causal_thread("task-1").unwrap()
    };

    {
        let db = HiveDB::open(dir.path()).unwrap();
        db.wipe_projections_and_rebuild().unwrap();
        let replayed = db.causal_thread("task-1").unwrap();
        assert_eq!(original_thread, replayed);
    }
}

fn seed_causal_task(db: &HiveDB, _stream_id: &str, n: usize) {
    let intent = db
        .append(intent_logged("PM", "implementar autenticación"))
        .unwrap();
    let mut last = intent;
    for i in 0..n {
        let d = db
            .append(decision("Backend", format!("decision-{i}"), Some(last)))
            .unwrap();
        last = db
            .append(tool_call_caused_by("Backend", "cargo_build", d))
            .unwrap();
    }
}

fn arb_causal_event_sequence(max_len: usize) -> impl Strategy<Value = Vec<EventInput>> {
    prop::collection::vec(arb_causal_event(), 0..=max_len)
}

fn arb_causal_event() -> impl Strategy<Value = EventInput> {
    (any::<u8>(), any::<u8>(), any::<bool>(), any::<bool>()).prop_map(
        |(agent_idx, _stream_idx, is_decision, is_error)| {
            let agent = format!("agent-{}", agent_idx % 4);
            if is_decision {
                decision(agent, "arb-decision", None)
            } else {
                let outcome = if is_error {
                    ToolOutcome::Err("E0001".into())
                } else {
                    ToolOutcome::Ok
                };
                tool_call_with_outcome(agent, "arb-tool", outcome, 0)
            }
        },
    )
}
