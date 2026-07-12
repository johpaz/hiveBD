use crate::event::{AgentId, Event, EventKind, StreamId};
use crate::projection::{Projection, ProjectionScope};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Outcome of a tool invocation, parsed from the event payload.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ToolOutcome {
    Ok,
    Err(String),
    Timeout,
}

/// Raw event data kept by the `CausalThread` projection per stream.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ThreadEvent {
    Decision {
        agent: AgentId,
        description: String,
        phase: Option<String>,
        caused_by: Option<u64>,
        correlation: Option<uuid::Uuid>,
    },
    ToolCall {
        agent: AgentId,
        tool: String,
        outcome: ToolOutcome,
        caused_by: Option<u64>,
        correlation: Option<uuid::Uuid>,
    },
    Intent {
        agent: AgentId,
        intent: String,
        correlation: Option<uuid::Uuid>,
    },
}

/// Events indexed by sequence number inside a single stream.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct StreamThread {
    pub events: BTreeMap<u64, ThreadEvent>,
}

/// Global projection state: one `StreamThread` per `stream_id`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CausalThreadState {
    pub streams: BTreeMap<StreamId, StreamThread>,
}

/// Marker type for the `CausalThread` projection.
///
/// Named `CausalThreadProjection` (not `CausalThread`) to avoid colliding
/// with [`crate::causal::CausalThread`], the ergonomic, consumer-facing data
/// struct returned by `HiveDB::causal_thread()` — that's the type most
/// callers actually want. This marker is currently **not** registered in
/// `default_registry()`; `causal_thread()` rebuilds threads on demand from
/// the raw event log instead of reading this projection's checkpointed
/// state (see `docs/AGENT_INTEGRATION.md` for the resulting scaling note).
pub struct CausalThreadProjection;

impl Projection for CausalThreadProjection {
    type State = CausalThreadState;

    fn name() -> &'static str {
        "CausalThread"
    }

    fn scope() -> ProjectionScope {
        // One sub-thread per agent shard; HiveDB::causal_thread merges them
        // by stream_id so cross-agent causation chains remain intact.
        ProjectionScope::Agent
    }

    fn merge(whole: &mut Self::State, part: &Self::State) {
        for (stream, part_thread) in &part.streams {
            let whole_thread = whole.streams.entry(stream.clone()).or_default();
            for (seq, event) in &part_thread.events {
                whole_thread.events.insert(*seq, event.clone());
            }
        }
    }

    fn apply(state: &mut Self::State, event: &Event) {
        let thread = state.streams.entry(event.stream_id.clone()).or_default();
        apply_to_thread(thread, event);
    }
}

/// Apply a single event to a `StreamThread`. Public so the causal engine can
/// rebuild threads on demand without materializing the projection.
pub fn apply_to_thread(thread: &mut StreamThread, event: &Event) {
    match &event.kind {
        EventKind::StateTransition => {
            if let Some(description) = event.payload.get("description").and_then(|v| v.as_str()) {
                let phase = event
                    .payload
                    .get("phase")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                thread.events.insert(
                    event.seq,
                    ThreadEvent::Decision {
                        agent: event.agent_id.clone(),
                        description: description.to_string(),
                        phase,
                        caused_by: event.causation,
                        correlation: event.correlation,
                    },
                );
            }
        }
        EventKind::ToolCall { tool } => {
            let outcome = parse_tool_outcome(&event.payload);
            thread.events.insert(
                event.seq,
                ThreadEvent::ToolCall {
                    agent: event.agent_id.clone(),
                    tool: tool.clone(),
                    outcome,
                    caused_by: event.causation,
                    correlation: event.correlation,
                },
            );
        }
        EventKind::IntentLogged { actor, intent, .. } => {
            thread.events.insert(
                event.seq,
                ThreadEvent::Intent {
                    agent: actor.clone(),
                    intent: intent.clone(),
                    correlation: event.correlation,
                },
            );
        }
        _ => {}
    }
}

/// Parses a `ToolCall` payload's `outcome` field into the canonical
/// [`ToolOutcome`] shape. This is the single source of truth for the
/// `outcome` contract — every projection that reads `ToolCall` payloads
/// (`CausalThread`, `ToolLedger`) must go through this function so they
/// agree on what counts as a failure. See `docs/AGENT_INTEGRATION.md` for
/// the documented wire shape.
pub(crate) fn parse_tool_outcome(payload: &serde_json::Value) -> ToolOutcome {
    match payload.get("outcome") {
        Some(serde_json::Value::String(s)) if s == "Ok" => ToolOutcome::Ok,
        Some(serde_json::Value::String(s)) if s == "Timeout" => ToolOutcome::Timeout,
        Some(obj) if obj.get("Err").is_some() => {
            let msg = obj
                .get("Err")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_default();
            ToolOutcome::Err(msg)
        }
        _ => ToolOutcome::Ok,
    }
}
