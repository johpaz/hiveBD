use hivedb_core::{
    AgentContextRequest, AgentId, ContextStrategy, EventInput, EventKind, HarnessInput,
    HarnessLoop, HiveDB, StreamId, ToolOutcome,
};
use serde_json::json;

fn decision(
    agent: impl Into<AgentId>,
    stream_id: impl Into<StreamId>,
    description: impl Into<String>,
    caused_by: Option<u64>,
) -> EventInput {
    let input = EventInput::new(agent, stream_id, EventKind::StateTransition)
        .with_payload(json!({ "description": description.into() }));
    match caused_by {
        Some(seq) => input.with_causation(seq),
        None => input,
    }
}

fn decision_with_phase(
    agent: impl Into<AgentId>,
    stream_id: impl Into<StreamId>,
    description: impl Into<String>,
    phase: impl Into<String>,
    caused_by: Option<u64>,
) -> EventInput {
    let input = EventInput::new(agent, stream_id, EventKind::StateTransition).with_payload(json!({
        "description": description.into(),
        "phase": phase.into(),
    }));
    match caused_by {
        Some(seq) => input.with_causation(seq),
        None => input,
    }
}

fn tool_call_with_outcome(
    agent: impl Into<AgentId>,
    stream_id: impl Into<StreamId>,
    tool: impl Into<String>,
    outcome: ToolOutcome,
    caused_by: u64,
) -> EventInput {
    let payload = match outcome {
        ToolOutcome::Ok => json!({ "outcome": "Ok" }),
        ToolOutcome::Timeout => json!({ "outcome": "Timeout" }),
        ToolOutcome::Err(ref msg) => json!({ "outcome": { "Err": msg } }),
    };
    EventInput::new(agent, stream_id, EventKind::ToolCall { tool: tool.into() })
        .with_payload(payload)
        .with_causation(caused_by)
}

fn seed_work_session(db: &HiveDB, task_id: &str, session: &str, n: usize) {
    let mut last: Option<u64> = None;
    for i in 0..n {
        let mut input =
            decision_with_phase("Backend", task_id, format!("{session}-{i}"), session, last);
        if let Some(seq) = last {
            input = input.with_causation(seq);
        }
        last = Some(db.append(input).unwrap());
    }
}

fn seed_task_with_pattern_failure(db: &HiveDB, task_id: &str) -> String {
    for _ in 0..3 {
        let d = db
            .append(decision("Backend", task_id, "null pointer risk", None))
            .unwrap();
        db.append(tool_call_with_outcome(
            "Backend",
            task_id,
            "cargo_build",
            ToolOutcome::Err("null pointer".into()),
            d,
        ))
        .unwrap();
    }
    task_id.to_string()
}

fn seed_similar_task(db: &HiveDB, task_id: &str, _base_task_id: &str) -> String {
    // After applying the proposals from the first task, the swarm makes only
    // one isolated failure instead of a full loop.
    let d = db
        .append(decision("Backend", task_id, "null pointer risk", None))
        .unwrap();
    db.append(tool_call_with_outcome(
        "Backend",
        task_id,
        "cargo_build",
        ToolOutcome::Err("null pointer".into()),
        d,
    ))
    .unwrap();
    task_id.to_string()
}

fn learning_proposal_approved(p: &hivedb_core::LearningProposal) -> EventInput {
    EventInput::new("Harness", "task-1", EventKind::Fact)
        .with_payload(json!({ "approved": serde_json::to_value(p).unwrap() }))
}

#[test]
fn long_running_task_resumes_with_full_causal_context() {
    let db = HiveDB::open_temp().unwrap();
    seed_work_session(&db, "task-1", "session-1", 300);
    seed_work_session(&db, "task-1", "session-2", 300);
    seed_work_session(&db, "task-1", "session-3", 50);

    let ctx = db
        .build_agent_context(AgentContextRequest {
            task_id: "task-1".into(),
            current_phase: "session-3".into(),
            current_objective: "completar integración de pagos".into(),
            max_tokens: 8192,
            strategy: ContextStrategy {
                causal_anchors: true,
                compress_completed_phases: true,
                episodic_similarity: None,
                recent_anomalies: None,
            },
        })
        .unwrap();

    assert!(ctx.spans_sessions(&["session-1", "session-2", "session-3"]));
    assert!(ctx.estimated_tokens() <= 8192);
    assert!(ctx.has_content_from_phase("session-1"));
    assert!(ctx.has_content_from_phase("session-2"));
}

#[test]
fn harness_loop_improves_across_similar_tasks() {
    let db = HiveDB::open_temp().unwrap();

    let t1 = seed_task_with_pattern_failure(&db, "task-1");
    let thread1 = db.causal_thread(t1.as_str()).unwrap();
    let eval1 = HarnessLoop::evaluate(HarnessInput {
        causal_thread: thread1.clone(),
        similar_episodes: vec![],
        ..Default::default()
    });

    for p in &eval1.proposals {
        db.append(learning_proposal_approved(p)).unwrap();
    }

    let t2 = seed_similar_task(&db, "task-2", &t1);
    let thread2 = db.causal_thread(t2.as_str()).unwrap();
    let eval2 = HarnessLoop::evaluate(HarnessInput {
        causal_thread: thread2,
        similar_episodes: vec![thread1],
        ..Default::default()
    });

    assert!(eval2.anomaly_count() < eval1.anomaly_count());
    if !eval2.proposals.is_empty() && !eval1.proposals.is_empty() {
        assert!(eval2.proposals[0].confidence > eval1.proposals[0].confidence);
        assert!(eval2.proposals[0].specificity > eval1.proposals[0].specificity);
    }
}
