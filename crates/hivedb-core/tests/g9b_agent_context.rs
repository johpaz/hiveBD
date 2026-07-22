use hivedb_core::{
    AgentContextRequest, AnomalyConfig, ContextStrategy, EventInput, EventKind, HiveDB,
    OpenOptions, StreamId, VectorOptions,
};
use serde_json::json;

fn vector_db() -> HiveDB {
    HiveDB::open_temp_with_options(OpenOptions {
        vector: Some(VectorOptions::new(384, "test:384")),
    })
    .unwrap()
}

fn embed(text: &str) -> Vec<f32> {
    let mut v = vec![0.0; 384];
    match text {
        "null pointer pagos" | "NPE en pagos" => {
            v[0] = 1.0;
            v[1] = 1.0;
        }
        "optimización queries SQL" | "queries SQL" => {
            v[10] = 1.0;
        }
        "pagos" => {
            v[0] = 1.0;
        }
        _ => {
            v[100] = 1.0;
        }
    }
    v
}

fn decision(
    agent: impl Into<hivedb_core::AgentId>,
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

fn decision_with_phase(
    agent: impl Into<hivedb_core::AgentId>,
    description: impl Into<String>,
    phase: impl Into<String>,
) -> EventInput {
    EventInput::new(agent, StreamId::from("task-1"), EventKind::StateTransition).with_payload(
        json!({
            "description": description.into(),
            "phase": phase.into(),
        }),
    )
}

fn tool_call_with_outcome(
    agent: impl Into<hivedb_core::AgentId>,
    tool: impl Into<String>,
    outcome: hivedb_core::ToolOutcome,
    caused_by: u64,
) -> EventInput {
    let payload = match outcome {
        hivedb_core::ToolOutcome::Ok => json!({ "outcome": "Ok" }),
        hivedb_core::ToolOutcome::Timeout => json!({ "outcome": "Timeout" }),
        hivedb_core::ToolOutcome::Err(ref msg) => json!({ "outcome": { "Err": msg } }),
    };
    EventInput::new(
        agent,
        StreamId::from("task-1"),
        EventKind::ToolCall { tool: tool.into() },
    )
    .with_payload(payload)
    .with_causation(caused_by)
}

fn error_caused_by(
    agent: impl Into<hivedb_core::AgentId>,
    description: impl Into<String>,
    caused_by: u64,
) -> EventInput {
    EventInput::new(agent, StreamId::from("task-1"), EventKind::StateTransition)
        .with_payload(json!({ "description": description.into() }))
        .with_causation(caused_by)
}

fn seed_phase(db: &HiveDB, _task_id: &str, phase: &str, n: usize) {
    let mut last: Option<u64> = None;
    for i in 0..n {
        let mut input = decision_with_phase("Backend", format!("{phase}-{i}"), phase);
        if let Some(seq) = last {
            input = input.with_causation(seq);
        }
        last = Some(db.append(input).unwrap());
    }
}

fn seed_task_with_outcome(
    db: &HiveDB,
    task_id: &str,
    description: &str,
    _outcome: hivedb_core::ToolOutcome,
) {
    // Index an episode document for the past task.
    db.upsert_doc(
        &hivedb_core::IndexDoc::new(task_id)
            .with_body(description)
            .with_vector(embed(description))
            .with_filters(vec![hivedb_core::ScalarFilter::eq("kind", "episode")]),
    )
    .unwrap();
}

#[test]
fn build_context_never_exceeds_token_limit() {
    let db = vector_db();
    seed_phase(&db, "task-1", "implementation", 10_000);

    let ctx = db
        .build_agent_context(AgentContextRequest {
            task_id: "task-1".into(),
            current_phase: "implementation".into(),
            current_objective: "fix payment module".into(),
            max_tokens: 4096,
            strategy: ContextStrategy::default(),
        })
        .unwrap();

    assert!(ctx.estimated_tokens() <= 4096);
}

#[test]
fn causal_anchors_retrieves_distant_but_causally_connected_decisions() {
    let db = vector_db();

    let anchor = db
        .append(decision("Architect", "no validar nulos en pagos", None))
        .unwrap();

    for _ in 0..995 {
        db.append(decision("Other", "noise", None)).unwrap();
    }

    let current = db
        .append(error_caused_by("Backend", "NPE en pagos", anchor))
        .unwrap();

    let ctx = db
        .build_agent_context(AgentContextRequest {
            task_id: "task-1".into(),
            current_phase: "".into(),
            current_objective: "NPE en pagos".into(),
            max_tokens: 8192,
            strategy: ContextStrategy {
                causal_anchors: true,
                ..Default::default()
            },
        })
        .unwrap();

    assert!(ctx.contains_seq(anchor));
    assert!(ctx.contains_seq(current));
}

#[test]
fn completed_phases_are_compressed_not_dropped() {
    let db = vector_db();
    seed_phase(&db, "task-1", "planning", 200);
    seed_phase(&db, "task-1", "implementation", 50);

    let ctx = db
        .build_agent_context(AgentContextRequest {
            task_id: "task-1".into(),
            current_phase: "implementation".into(),
            current_objective: "".into(),
            max_tokens: 2048,
            strategy: ContextStrategy {
                compress_completed_phases: true,
                ..Default::default()
            },
        })
        .unwrap();

    let planning = ctx.phase_summary("planning").unwrap();
    assert!(planning.is_compressed);

    assert!(ctx.has_content_from_phase("implementation"));
}

#[test]
fn episodic_similarity_retrieves_past_relevant_episodes() {
    let db = vector_db();
    seed_task_with_outcome(
        &db,
        "task-past-1",
        "null pointer en módulo de pagos",
        hivedb_core::ToolOutcome::Ok,
    );
    seed_task_with_outcome(
        &db,
        "task-past-2",
        "optimización de queries SQL",
        hivedb_core::ToolOutcome::Ok,
    );

    let ctx = db
        .build_agent_context(AgentContextRequest {
            task_id: "task-current".into(),
            current_phase: "".into(),
            current_objective: "NPE en validación de pagos".into(),
            max_tokens: 8192,
            strategy: ContextStrategy {
                episodic_similarity: Some(hivedb_core::EpisodicConfig {
                    vector: embed("null pointer pagos"),
                    k: 3,
                }),
                ..Default::default()
            },
        })
        .unwrap();

    assert!(
        ctx.similar_episodes
            .iter()
            .any(|e| e.task_id == "task-past-1")
    );
    assert!(
        !ctx.similar_episodes
            .iter()
            .any(|e| e.task_id == "task-past-2")
    );
}

#[test]
fn recent_anomalies_always_included_in_context() {
    let db = vector_db();

    for _ in 0..100 {
        db.append(decision("Backend", "step", None)).unwrap();
    }

    // Inject an error loop in the last few events.
    for _ in 0..4 {
        let d = db.append(decision("Backend", "compilar", None)).unwrap();
        db.append(tool_call_with_outcome(
            "Backend",
            "cargo_build",
            hivedb_core::ToolOutcome::Err("E0432".into()),
            d,
        ))
        .unwrap();
    }

    let ctx = db
        .build_agent_context(AgentContextRequest {
            task_id: "task-1".into(),
            current_phase: "".into(),
            current_objective: "".into(),
            max_tokens: 8192,
            strategy: ContextStrategy {
                recent_anomalies: Some(AnomalyConfig { window_ms: 300_000 }),
                ..Default::default()
            },
        })
        .unwrap();

    assert!(!ctx.anomalies.is_empty());
    assert!(matches!(
        ctx.anomalies[0],
        hivedb_core::ContextItem::Anomaly { .. }
    ));
}

#[test]
fn build_context_is_idempotent() {
    let db = vector_db();
    seed_phase(&db, "task-1", "implementation", 500);

    let req = AgentContextRequest {
        task_id: "task-1".into(),
        current_phase: "implementation".into(),
        current_objective: "fix pagos".into(),
        max_tokens: 4096,
        strategy: ContextStrategy::default(),
    };

    let ctx1 = db.build_agent_context(req.clone()).unwrap();
    let ctx2 = db.build_agent_context(req.clone()).unwrap();

    assert_eq!(ctx1.content_hash(), ctx2.content_hash());
}
