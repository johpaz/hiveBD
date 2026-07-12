//! Regression guard for the genericity of the G9 harness contract
//! (`docs/AGENT_INTEGRATION.md`).
//!
//! Deliberately outside the `gN_*` gate-numbering convention (see
//! `AGENTS.md`): this is a standing regression guard, not a phase gate. It
//! exercises the same guarantees as `g9a_causal_thread.rs`/
//! `g9b_agent_context.rs`/`g9c_harness.rs`/`g2_tool_ledger.rs`, but through a
//! support-ticket-triage vocabulary (`Analyst`/`Router`/`Auditor` roles,
//! `ticket-*` streams, `classify_ticket`/`lookup_customer`/
//! `escalate_to_human` tools, `triage`/`resolution` phases) instead of
//! hiveCode's "swarm"/"session-N" vocabulary. If a future change to
//! `causal/mod.rs`, `context.rs`, `harness.rs` or `state/tool_ledger.rs`
//! silently starts assuming hiveCode-flavored names or shapes, this file is
//! the canary that should catch it.

use hivedb_core::{
    AgentContextRequest, AgentId, AnomalyKind, ContextStrategy, EventInput, EventKind,
    HarnessInput, HarnessLoop, HiveDB, StreamId, ToolOutcome,
};
use serde_json::json;

const TICKET: &str = "ticket-4821";

fn intent_logged(agent: impl Into<AgentId>, intent: impl Into<String>) -> EventInput {
    let agent = agent.into();
    EventInput::new(
        agent.clone(),
        StreamId::from(TICKET),
        EventKind::IntentLogged {
            actor: agent,
            intent: intent.into(),
            authorized_by: None,
        },
    )
}

fn decision(
    agent: impl Into<AgentId>,
    description: impl Into<String>,
    phase: Option<&str>,
    caused_by: Option<u64>,
) -> EventInput {
    let mut payload = json!({ "description": description.into() });
    if let Some(p) = phase {
        payload["phase"] = json!(p);
    }
    let input = EventInput::new(agent, StreamId::from(TICKET), EventKind::StateTransition)
        .with_payload(payload);
    match caused_by {
        Some(seq) => input.with_causation(seq),
        None => input,
    }
}

fn tool_call(
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
        StreamId::from(TICKET),
        EventKind::ToolCall { tool: tool.into() },
    )
    .with_payload(payload)
    .with_causation(caused_by)
}

/// The full documented contract, end to end: causal chain across agents,
/// error-loop detection, root-cause resolution, and proposal generation —
/// all through a vocabulary that has nothing to do with hiveCode.
#[test]
fn generic_consumer_gets_full_causal_thread_and_harness_value() {
    let db = HiveDB::open_temp().unwrap();

    let intent = db
        .append(intent_logged(
            "Router",
            "resolver timeout de checkout reportado por el cliente",
        ))
        .unwrap();

    let d1 = db
        .append(decision(
            "Analyst",
            "clasificar como bug de pagos",
            Some("triage"),
            Some(intent),
        ))
        .unwrap();
    let t1 = db
        .append(tool_call("Analyst", "classify_ticket", ToolOutcome::Ok, d1))
        .unwrap();

    let d2 = db
        .append(decision(
            "Auditor",
            "escalar a soporte de pagos",
            Some("resolution"),
            Some(t1),
        ))
        .unwrap();
    let t2 = db
        .append(tool_call(
            "Auditor",
            "escalate_to_human",
            ToolOutcome::Ok,
            d2,
        ))
        .unwrap();

    let thread = db.causal_thread(TICKET).unwrap();

    // Causal chain connects across agents (Router -> Analyst -> Auditor).
    assert_eq!(thread.decisions.len(), 2);
    assert_eq!(thread.tool_calls.len(), 2);
    assert_eq!(thread.tool_calls[0].caused_by, Some(d1));
    assert_eq!(thread.tool_calls[1].seq, t2);
    assert_eq!(thread.decisions[1].caused_by, Some(t1));
    assert!(thread.decisions[0].caused.contains(&t1));

    // Harness evaluation over a clean thread: no anomalies, no root cause.
    let clean_eval = HarnessLoop::evaluate(HarnessInput {
        causal_thread: thread,
        similar_episodes: vec![],
        original_intent: "resolver timeout de checkout".into(),
        current_state: Some(json!({ "outcome": "resolved" })),
        min_confidence: 0.5,
    });
    assert!((clean_eval.process_quality - 1.0).abs() < f64::EPSILON);
    assert!(clean_eval.root_cause.is_none());
}

/// `ErrorLoop` detection and root-cause resolution through the alternate
/// vocabulary — mirrors `g9a_causal_thread.rs::causal_thread_detects_error_loops`
/// and `g9c_harness.rs::harness_root_cause_has_exact_seq_provenance`.
#[test]
fn generic_consumer_error_loop_and_root_cause() {
    let db = HiveDB::open_temp().unwrap();

    let root_decision = db
        .append(decision(
            "Analyst",
            "consultar historial del cliente",
            Some("triage"),
            None,
        ))
        .unwrap();

    for _ in 0..3 {
        db.append(tool_call(
            "Analyst",
            "lookup_customer",
            ToolOutcome::Err("crm_timeout".into()),
            root_decision,
        ))
        .unwrap();
    }

    let thread = db.causal_thread(TICKET).unwrap();
    assert!(!thread.anomalies.is_empty());
    let anomaly = &thread.anomalies[0];
    assert_eq!(anomaly.kind, AnomalyKind::ErrorLoop);
    assert_eq!(anomaly.repetitions, 3);
    assert_eq!(anomaly.tool, Some("lookup_customer".into()));

    let eval = HarnessLoop::evaluate(HarnessInput {
        causal_thread: thread,
        similar_episodes: vec![],
        original_intent: "resolver timeout de checkout".into(),
        current_state: None,
        min_confidence: 0.3,
    });

    assert!(eval.process_quality < 1.0);
    let root_cause = eval.root_cause.expect("root cause must resolve");
    assert_eq!(root_cause.seq, root_decision);
    assert_eq!(root_cause.agent, AgentId::from("Analyst"));
    assert!(
        !eval.proposals.is_empty(),
        "a repeated failure must produce at least one learning proposal"
    );
}

/// `ObjectiveDrift` detection: decisions whose `correlation` diverges from
/// the stream's original `IntentLogged`.
#[test]
fn generic_consumer_objective_drift() {
    use uuid::Uuid;

    let db = HiveDB::open_temp().unwrap();
    let intent = db
        .append(intent_logged("Router", "resolver timeout de checkout"))
        .unwrap();

    let unrelated_correlation = Uuid::new_v4();
    for _ in 0..6 {
        let mut input = decision("Auditor", "revisar política de reembolsos", None, None);
        input.correlation = Some(unrelated_correlation);
        db.append(input).unwrap();
    }

    let thread = db.causal_thread(TICKET).unwrap();
    let drift = thread
        .anomalies
        .iter()
        .find(|a| a.kind == AnomalyKind::ObjectiveDrift);
    assert!(drift.is_some());
    assert_eq!(drift.unwrap().original_intent_seq, Some(intent));
}

/// `buildAgentContext` respects the token budget and compresses completed
/// phases through the alternate vocabulary — mirrors
/// `g9b_agent_context.rs::build_context_never_exceeds_token_limit` and
/// `completed_phases_are_compressed_not_dropped`.
#[test]
fn generic_consumer_context_budget_and_phase_compression() {
    let db = HiveDB::open_temp().unwrap();

    let mut last: Option<u64> = None;
    for i in 0..200 {
        let input = decision("Analyst", format!("triage-step-{i}"), Some("triage"), last);
        last = Some(db.append(input).unwrap());
    }
    for i in 0..50 {
        db.append(decision(
            "Auditor",
            format!("resolution-step-{i}"),
            Some("resolution"),
            None,
        ))
        .unwrap();
    }

    let ctx = db
        .build_agent_context(AgentContextRequest {
            task_id: TICKET.into(),
            current_phase: "resolution".into(),
            current_objective: "".into(),
            max_tokens: 2048,
            strategy: ContextStrategy {
                compress_completed_phases: true,
                ..Default::default()
            },
        })
        .unwrap();

    assert!(ctx.estimated_tokens() <= 2048);
    let triage_summary = ctx.phase_summary("triage").unwrap();
    assert!(triage_summary.is_compressed);
    assert!(ctx.has_content_from_phase("resolution"));
}

/// `toolStats()` and `causalThread()` must agree on failures for this
/// vocabulary too — same invariant as
/// `g2_tool_ledger.rs::tool_ledger_and_causal_thread_agree_on_failures`,
/// exercised here with a stream/tool naming scheme that has nothing to do
/// with that test file's.
#[test]
fn generic_consumer_tool_stats_agree_with_causal_thread() {
    let db = HiveDB::open_temp().unwrap();
    let d = db
        .append(decision(
            "Analyst",
            "reintentar lookup",
            Some("triage"),
            None,
        ))
        .unwrap();
    db.append(tool_call(
        "Analyst",
        "lookup_customer",
        ToolOutcome::Timeout,
        d,
    ))
    .unwrap();

    let stats = db.tool_stats("lookup_customer").unwrap().unwrap();
    assert_eq!(stats.errors, 1);

    let thread = db.causal_thread(TICKET).unwrap();
    assert!(
        thread
            .tool_calls
            .iter()
            .all(|t| !matches!(t.outcome, ToolOutcome::Ok))
    );
}
