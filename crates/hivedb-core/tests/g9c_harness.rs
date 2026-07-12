use hivedb_core::{
    AgentId, CausalThread, EventInput, EventKind, FindingKind, HarnessInput, HarnessLoop, HiveDB,
    LearningProposal, StreamId, ToolOutcome,
};
use serde_json::json;

#[derive(Clone, Copy, Debug)]
enum FailurePattern {
    NullPointer,
}

#[derive(Clone, Copy, Debug)]
enum Resolution {
    AddNullCheck,
}

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

fn intent_logged(
    agent: impl Into<AgentId>,
    stream_id: impl Into<StreamId>,
    intent: impl Into<String>,
) -> EventInput {
    let actor = agent.into();
    EventInput::new(
        actor.clone(),
        stream_id,
        EventKind::IntentLogged {
            actor,
            intent: intent.into(),
            authorized_by: None,
        },
    )
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

fn seed_task_with_error_loop_then_success(db: &HiveDB, task_id: &str) {
    let _intent = db
        .append(intent_logged("PM", task_id, "implementar autenticación"))
        .unwrap();
    for _ in 0..3 {
        let d = db
            .append(decision("Backend", task_id, "compilar módulo", None))
            .unwrap();
        db.append(tool_call_with_outcome(
            "Backend",
            task_id,
            "cargo_build",
            ToolOutcome::Err("E0432".into()),
            d,
        ))
        .unwrap();
    }
}

fn seed_failures_caused_by(db: &HiveDB, task_id: &str, caused_by: u64, n: usize) {
    for _ in 0..n {
        db.append(tool_call_with_outcome(
            "Backend",
            task_id,
            "network_call",
            ToolOutcome::Err("timeout".into()),
            caused_by,
        ))
        .unwrap();
    }
}

fn seed_task_with_pattern_failure(db: &HiveDB, task_id: &str) {
    let _intent = db
        .append(intent_logged("PM", task_id, "implementar autenticación"))
        .unwrap();
    for _ in 0..3 {
        let d = db
            .append(decision("Backend", task_id, "refactorizar ORM", None))
            .unwrap();
        db.append(tool_call_with_outcome(
            "Backend",
            task_id,
            "cargo_build",
            ToolOutcome::Err("E0432".into()),
            d,
        ))
        .unwrap();
    }
}

fn seed_task_with_single_failure(db: &HiveDB, task_id: &str) {
    let _intent = db
        .append(intent_logged("PM", task_id, "implementar autenticación"))
        .unwrap();
    let d = db
        .append(decision("Backend", task_id, "compilar módulo", None))
        .unwrap();
    db.append(tool_call_with_outcome(
        "Backend",
        task_id,
        "cargo_build",
        ToolOutcome::Err("E0432".into()),
        d,
    ))
    .unwrap();
}

fn seed_resolved_episode(
    db: &HiveDB,
    task_id: &str,
    _pattern: FailurePattern,
    _resolution: Resolution,
) -> CausalThread {
    let intent = db
        .append(intent_logged("PM", task_id, "implementar autenticación"))
        .unwrap();
    let bad = db
        .append(decision("Backend", task_id, "null pointer risk", None))
        .unwrap();
    db.append(tool_call_with_outcome(
        "Backend",
        task_id,
        "cargo_build",
        ToolOutcome::Err("null pointer".into()),
        bad,
    ))
    .unwrap();
    // Resolution decision linked to the original intent and the bad decision.
    db.append(decision("Backend", task_id, "Add null check", Some(bad)).with_causation(intent))
        .unwrap();
    db.causal_thread(task_id).unwrap()
}

fn seed_task_with_failure(db: &HiveDB, task_id: &str, _pattern: FailurePattern) -> String {
    let _intent = db
        .append(intent_logged("PM", task_id, "implementar autenticación"))
        .unwrap();
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

fn learning_proposal(p: &LearningProposal) -> EventInput {
    EventInput::new("Harness", "task-1", EventKind::LearningProposal)
        .with_payload(serde_json::to_value(p).unwrap())
}

#[allow(dead_code)]
fn learning_proposal_approved(p: &LearningProposal) -> EventInput {
    EventInput::new("Harness", "task-1", EventKind::Fact)
        .with_payload(json!({ "approved": serde_json::to_value(p).unwrap() }))
}

#[test]
fn harness_evaluates_process_not_just_output() {
    let db = HiveDB::open_temp().unwrap();
    seed_task_with_error_loop_then_success(&db, "task-1");
    let thread = db.causal_thread("task-1").unwrap();

    let eval = HarnessLoop::evaluate(HarnessInput {
        causal_thread: thread,
        similar_episodes: vec![],
        original_intent: "implementar autenticación".into(),
        current_state: Some(json!({ "outcome": "success" })),
        min_confidence: 0.5,
    });

    assert!(eval.process_quality < eval.output_quality);
    assert!(
        eval.findings
            .iter()
            .any(|f| f.kind == FindingKind::InefficientLoop)
    );
}

#[test]
fn harness_root_cause_has_exact_seq_provenance() {
    let db = HiveDB::open_temp().unwrap();

    // Seed 9 unrelated events so the bad decision lands at seq == 10.
    for i in 0..9 {
        db.append(decision("Other", "task-1", format!("noise-{i}"), None))
            .unwrap();
    }

    let bad_decision = db
        .append(decision(
            "Architect",
            "task-1",
            "no manejar errores de red",
            None,
        ))
        .unwrap();
    assert_eq!(bad_decision, 10);

    seed_failures_caused_by(&db, "task-1", bad_decision, 20);

    let thread = db.causal_thread("task-1").unwrap();
    let eval = HarnessLoop::evaluate(HarnessInput {
        causal_thread: thread,
        ..Default::default()
    });

    let root_cause = eval.root_cause.as_ref().unwrap();
    assert_eq!(root_cause.seq, bad_decision);
    assert_eq!(root_cause.agent.0, "Architect");
}

#[test]
fn learning_proposals_include_causal_evidence() {
    let db = HiveDB::open_temp().unwrap();
    seed_task_with_pattern_failure(&db, "task-1");
    let thread = db.causal_thread("task-1").unwrap();

    let eval = HarnessLoop::evaluate(HarnessInput {
        causal_thread: thread,
        ..Default::default()
    });

    assert!(!eval.proposals.is_empty());
    for proposal in &eval.proposals {
        assert!(!proposal.evidence_seqs.is_empty());
        assert!(proposal.trigger_seq.is_some());
        assert!(proposal.confidence > 0.0 && proposal.confidence <= 1.0);
    }
}

#[test]
fn learning_proposals_are_logged_as_events() {
    let db = HiveDB::open_temp().unwrap();
    seed_task_with_pattern_failure(&db, "task-1");
    let thread = db.causal_thread("task-1").unwrap();

    let eval = HarnessLoop::evaluate(HarnessInput {
        causal_thread: thread,
        ..Default::default()
    });

    for p in &eval.proposals {
        let seq = db.append(learning_proposal(p)).unwrap();
        let ev = db.read(seq).unwrap();
        assert_eq!(ev.kind_tag(), "LearningProposal");
        let payload: LearningProposal = ev.deserialize_payload().unwrap();
        assert!(!payload.evidence_seqs.is_empty());
    }
}

#[test]
fn similar_episodes_improve_proposal_specificity() {
    let db = HiveDB::open_temp().unwrap();

    let past = seed_resolved_episode(
        &db,
        "task-past",
        FailurePattern::NullPointer,
        Resolution::AddNullCheck,
    );
    let current_id = seed_task_with_failure(&db, "task-current", FailurePattern::NullPointer);
    let current_thread = db.causal_thread(current_id.as_str()).unwrap();

    let eval_without = HarnessLoop::evaluate(HarnessInput {
        causal_thread: current_thread.clone(),
        similar_episodes: vec![],
        ..Default::default()
    });
    let eval_with = HarnessLoop::evaluate(HarnessInput {
        causal_thread: current_thread,
        similar_episodes: vec![past],
        ..Default::default()
    });

    assert!(!eval_without.proposals.is_empty());
    assert!(!eval_with.proposals.is_empty());
    assert!(eval_with.proposals[0].confidence > eval_without.proposals[0].confidence);
    assert!(eval_with.proposals[0].specificity > eval_without.proposals[0].specificity);
}

#[test]
fn harness_withholds_proposals_below_confidence_threshold() {
    let db = HiveDB::open_temp().unwrap();
    seed_task_with_single_failure(&db, "task-1");
    let thread = db.causal_thread("task-1").unwrap();

    let eval = HarnessLoop::evaluate(HarnessInput {
        causal_thread: thread,
        min_confidence: 0.75,
        ..Default::default()
    });

    assert!(eval.proposals.is_empty());
    assert!(
        eval.findings
            .iter()
            .any(|f| f.kind == FindingKind::InsufficientEvidence)
    );
}
