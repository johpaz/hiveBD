use super::causal_thread::{ToolOutcome, parse_tool_outcome};
use crate::event::{Event, EventKind};
use crate::projection::Projection;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Aggregated statistics for a single tool.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ToolStats {
    /// Number of times the tool was invoked.
    pub invocations: u64,
    /// Number of invocations whose `outcome` parsed as `ToolOutcome::Err` or
    /// `ToolOutcome::Timeout`.
    pub errors: u64,
    /// Sum of `latency_ms` values observed in tool call payloads.
    pub total_latency_ms: u64,
    /// Sum of `cost` values observed in tool call payloads.
    pub total_cost: f64,
    /// Last observed `outcome` string, if any.
    pub last_outcome: Option<String>,
    /// Sequence of the last event that touched this tool, used to pick the
    /// most recent `last_outcome` when merging partial states from shards.
    #[serde(default)]
    pub last_seq: u64,
}

/// State of the `ToolLedger` projection.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ToolLedgerState {
    tools: BTreeMap<String, ToolStats>,
}

impl ToolLedgerState {
    /// Returns the aggregated stats for a tool, if any calls have been recorded.
    pub fn get(&self, tool: &str) -> Option<&ToolStats> {
        self.tools.get(tool)
    }

    /// Iterate over all recorded tools and their stats.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &ToolStats)> + '_ {
        self.tools.iter()
    }
}

/// Marker type for the `ToolLedger` projection.
///
/// Metrics are aggregated from `EventKind::ToolCall` payloads:
/// - `latency_ms` (u64) is added to `total_latency_ms`.
/// - `cost` (number) is added to `total_cost`.
/// - `outcome` is parsed via [`parse_tool_outcome`] (canonical shape:
///   `"Ok"` / `"Timeout"` / `{"Err": "<message>"}`) — `Err` and `Timeout`
///   both increment `errors`; the stringified outcome becomes `last_outcome`.
///   This is the same parser `CausalThread` uses, so the two projections
///   always agree on whether a given tool call was a failure.
pub struct ToolLedger;

impl Projection for ToolLedger {
    type State = ToolLedgerState;

    fn name() -> &'static str {
        "ToolLedger"
    }

    fn merge(whole: &mut Self::State, part: &Self::State) {
        for (tool, part_stats) in &part.tools {
            let whole_stats = whole.tools.entry(tool.clone()).or_default();
            whole_stats.invocations += part_stats.invocations;
            whole_stats.errors += part_stats.errors;
            whole_stats.total_latency_ms += part_stats.total_latency_ms;
            whole_stats.total_cost += part_stats.total_cost;
            if part_stats.last_seq >= whole_stats.last_seq {
                whole_stats.last_seq = part_stats.last_seq;
                if part_stats.last_outcome.is_some() {
                    whole_stats.last_outcome = part_stats.last_outcome.clone();
                }
            }
        }
    }

    fn apply(state: &mut Self::State, event: &Event) {
        if let EventKind::ToolCall { tool } = &event.kind {
            let stats = state.tools.entry(tool.clone()).or_default();
            stats.invocations += 1;
            stats.last_seq = event.seq;

            if let Some(latency) = event.payload.get("latency_ms").and_then(|v| v.as_u64()) {
                stats.total_latency_ms += latency;
            }
            if let Some(cost) = event.payload.get("cost").and_then(|v| v.as_f64()) {
                stats.total_cost += cost;
            }
            if event.payload.get("outcome").is_some() {
                let outcome = parse_tool_outcome(&event.payload);
                stats.last_outcome = Some(stringify_outcome(&outcome));
                if matches!(outcome, ToolOutcome::Err(_) | ToolOutcome::Timeout) {
                    stats.errors += 1;
                }
            }
        }
    }
}

/// Renders a [`ToolOutcome`] as the `last_outcome` string exposed to callers.
fn stringify_outcome(outcome: &ToolOutcome) -> String {
    match outcome {
        ToolOutcome::Ok => "Ok".to_string(),
        ToolOutcome::Timeout => "Timeout".to_string(),
        ToolOutcome::Err(msg) => format!("Err: {msg}"),
    }
}
