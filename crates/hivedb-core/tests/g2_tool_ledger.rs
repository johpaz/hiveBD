mod common;

use common::db;
use hivedb_core::{EventInput, EventKind, StreamId, ToolLedger, ToolOutcome};
use serde_json::json;

#[test]
fn tool_ledger_aggregates_invocations() {
    let db = db();
    db.append(tool_call(
        "search",
        json!({"latency_ms": 10, "cost": 0.5, "outcome": "Ok"}),
    ))
    .unwrap();
    db.append(tool_call(
        "search",
        json!({"latency_ms": 20, "cost": 0.7, "outcome": "Ok"}),
    ))
    .unwrap();
    db.append(tool_call(
        "search",
        json!({"latency_ms": 5, "cost": 0.2, "outcome": "Ok"}),
    ))
    .unwrap();

    let stats = db.tool_stats("search").unwrap().unwrap();
    assert_eq!(stats.invocations, 3);
    assert_eq!(stats.total_latency_ms, 35);
    assert!((stats.total_cost - 1.4).abs() < f64::EPSILON);
    assert_eq!(stats.last_outcome.as_deref(), Some("Ok"));
}

#[test]
fn tool_ledger_counts_errors() {
    let db = db();
    db.append(tool_call(
        "email",
        json!({"latency_ms": 8, "cost": 0.1, "outcome": "Ok"}),
    ))
    .unwrap();
    db.append(tool_call(
        "email",
        json!({"latency_ms": 12, "cost": 0.1, "outcome": {"Err": "smtp rejected"}}),
    ))
    .unwrap();
    db.append(tool_call(
        "email",
        json!({"latency_ms": 9, "cost": 0.1, "outcome": "Ok"}),
    ))
    .unwrap();

    let stats = db.tool_stats("email").unwrap().unwrap();
    assert_eq!(stats.invocations, 3);
    assert_eq!(stats.errors, 1);
    assert_eq!(stats.last_outcome.as_deref(), Some("Ok"));
}

#[test]
fn tool_ledger_counts_timeout_as_error() {
    let db = db();
    db.append(tool_call(
        "email",
        json!({"latency_ms": 30000, "cost": 0.1, "outcome": "Timeout"}),
    ))
    .unwrap();

    let stats = db.tool_stats("email").unwrap().unwrap();
    assert_eq!(stats.invocations, 1);
    assert_eq!(stats.errors, 1);
    assert_eq!(stats.last_outcome.as_deref(), Some("Timeout"));
}

#[test]
fn tool_ledger_survives_projection_wipe_and_replay() {
    let db = db();
    db.append(tool_call(
        "parser",
        json!({"latency_ms": 30, "cost": 0.05, "outcome": "Ok"}),
    ))
    .unwrap();

    let before = db.project::<ToolLedger>().unwrap();
    assert_eq!(before.get("parser").unwrap().invocations, 1);

    db.wipe_projections_and_rebuild().unwrap();

    let after = db.project::<ToolLedger>().unwrap();
    assert_eq!(after.get("parser").unwrap().invocations, 1);
    assert_eq!(after.get("parser").unwrap().total_latency_ms, 30);
    assert!((after.get("parser").unwrap().total_cost - 0.05).abs() < f64::EPSILON);
}

/// Regression guard for the outcome-shape bug found in review: `ToolLedger`
/// and `CausalThread` both read `ToolCall.payload.outcome` and must agree on
/// whether a call failed, since they share `parse_tool_outcome`
/// (`state/causal_thread.rs`). A tool call with `outcome: {"Err": ...}` must
/// be counted as an error by `tool_stats()` AND surfaced as a non-`Ok`
/// outcome by `causal_thread()` — previously `ToolLedger` parsed `outcome`
/// as a bare lowercase string while `CausalThread` expected the typed
/// `Ok`/`Timeout`/`{"Err": ...}` shape, so the two projections silently
/// disagreed on every failing call.
#[test]
fn tool_ledger_and_causal_thread_agree_on_failures() {
    let db = db();
    db.append(tool_call(
        "deploy",
        json!({"outcome": {"Err": "rollback triggered"}}),
    ))
    .unwrap();
    db.append(tool_call("deploy", json!({"outcome": "Timeout"})))
        .unwrap();

    let stats = db.tool_stats("deploy").unwrap().unwrap();
    assert_eq!(stats.invocations, 2);
    assert_eq!(stats.errors, 2, "ToolLedger must count both failures");

    let thread = db.causal_thread("task-1").unwrap();
    let failures_in_thread = thread
        .tool_calls
        .iter()
        .filter(|t| !matches!(t.outcome, ToolOutcome::Ok))
        .count();
    assert_eq!(
        failures_in_thread, 2,
        "CausalThread must see the same two calls as failures"
    );
}

fn tool_call(tool: impl Into<String>, payload: serde_json::Value) -> EventInput {
    EventInput::new(
        "agent-1",
        StreamId::from("task-1"),
        EventKind::ToolCall { tool: tool.into() },
    )
    .with_payload(payload)
}
