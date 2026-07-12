//! Causal thread analysis for long-running agent tasks.

use crate::event::{AgentId, Event, StreamId};
use crate::state::causal_thread::{CausalThreadState, ThreadEvent, ToolOutcome, apply_to_thread};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A decision node inside a causal thread.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DecisionNode {
    pub seq: u64,
    pub agent: AgentId,
    pub description: String,
    pub phase: Option<String>,
    pub caused_by: Option<u64>,
    pub caused: Vec<u64>,
    pub correlation: Option<uuid::Uuid>,
}

/// A tool-call node inside a causal thread.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolCallNode {
    pub seq: u64,
    pub agent: AgentId,
    pub tool: String,
    pub outcome: ToolOutcome,
    pub caused_by: Option<u64>,
    pub correlation: Option<uuid::Uuid>,
}

/// Kinds of anomaly that can be detected in a causal thread.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AnomalyKind {
    ErrorLoop,
    ObjectiveDrift,
}

/// Detected anomaly with provenance information.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Anomaly {
    pub kind: AnomalyKind,
    pub repetitions: u32,
    pub tool: Option<String>,
    pub original_intent_seq: Option<u64>,
}

/// Complete causal thread for a single `stream_id`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CausalThread {
    pub decisions: Vec<DecisionNode>,
    pub tool_calls: Vec<ToolCallNode>,
    pub anomalies: Vec<Anomaly>,
}

impl CausalThread {
    /// Build a causal thread from a slice of events (on-demand rebuild).
    pub fn from_events(events: &[Event]) -> Self {
        let mut state = CausalThreadState::default();
        for event in events {
            let thread = state.streams.entry(event.stream_id.clone()).or_default();
            apply_to_thread(thread, event);
        }
        // Events are already sorted by seq, so the first stream is the target.
        let stream_id = state
            .streams
            .keys()
            .next()
            .cloned()
            .unwrap_or_else(|| StreamId::from(""));
        Self::from_state(&state, &stream_id)
    }

    /// Build a causal thread from the persisted projection state for the given stream.
    pub fn from_state(state: &CausalThreadState, stream_id: &StreamId) -> Self {
        let empty = crate::state::causal_thread::StreamThread::default();
        let thread = state.streams.get(stream_id).unwrap_or(&empty);

        let mut decisions = Vec::new();
        let mut tool_calls = Vec::new();
        let mut caused_map: HashMap<u64, Vec<u64>> = HashMap::new();

        for (seq, event) in &thread.events {
            match event {
                ThreadEvent::Decision {
                    agent,
                    description,
                    phase,
                    caused_by,
                    correlation,
                } => {
                    if let Some(parent) = caused_by {
                        caused_map.entry(*parent).or_default().push(*seq);
                    }
                    decisions.push(DecisionNode {
                        seq: *seq,
                        agent: agent.clone(),
                        description: description.clone(),
                        phase: phase.clone(),
                        caused_by: *caused_by,
                        caused: Vec::new(),
                        correlation: *correlation,
                    });
                }
                ThreadEvent::ToolCall {
                    agent,
                    tool,
                    outcome,
                    caused_by,
                    correlation,
                } => {
                    if let Some(parent) = caused_by {
                        caused_map.entry(*parent).or_default().push(*seq);
                    }
                    tool_calls.push(ToolCallNode {
                        seq: *seq,
                        agent: agent.clone(),
                        tool: tool.clone(),
                        outcome: outcome.clone(),
                        caused_by: *caused_by,
                        correlation: *correlation,
                    });
                }
                _ => {}
            }
        }

        decisions.sort_by_key(|d| d.seq);
        tool_calls.sort_by_key(|t| t.seq);

        for decision in &mut decisions {
            decision.caused = caused_map.remove(&decision.seq).unwrap_or_default();
            decision.caused.sort();
        }

        let anomalies = detect_anomalies(thread);

        Self {
            decisions,
            tool_calls,
            anomalies,
        }
    }
}

fn detect_anomalies(thread: &crate::state::causal_thread::StreamThread) -> Vec<Anomaly> {
    let mut anomalies = Vec::new();

    detect_error_loops(thread, &mut anomalies);
    detect_objective_drift(thread, &mut anomalies);

    anomalies
}

fn detect_error_loops(
    thread: &crate::state::causal_thread::StreamThread,
    anomalies: &mut Vec<Anomaly>,
) {
    // Count occurrences of (tool, error_message) across the stream.
    let mut counts: HashMap<(String, String), u32> = HashMap::new();
    for event in thread.events.values() {
        if let ThreadEvent::ToolCall {
            tool,
            outcome: ToolOutcome::Err(msg),
            ..
        } = event
        {
            *counts.entry((tool.clone(), msg.clone())).or_insert(0) += 1;
        }
    }

    for ((tool, _msg), count) in counts {
        if count >= 3 {
            anomalies.push(Anomaly {
                kind: AnomalyKind::ErrorLoop,
                repetitions: count,
                tool: Some(tool.clone()),
                original_intent_seq: None,
            });
        }
    }
}

fn detect_objective_drift(
    thread: &crate::state::causal_thread::StreamThread,
    anomalies: &mut Vec<Anomaly>,
) {
    // Find the first intent logged in the stream.
    let first_intent = thread.events.iter().find_map(|(seq, event)| match event {
        ThreadEvent::Intent { .. } => Some(*seq),
        _ => None,
    });

    let Some(intent_seq) = first_intent else {
        return;
    };

    let intent_correlation = thread
        .events
        .get(&intent_seq)
        .and_then(|event| match event {
            ThreadEvent::Intent { correlation, .. } => *correlation,
            _ => None,
        });

    // Count decisions whose correlation differs from the original intent.
    let drift_count = thread
        .events
        .values()
        .filter(|event| matches!(event, ThreadEvent::Decision { .. }))
        .filter(|event| {
            let decision_correlation = match event {
                ThreadEvent::Decision { correlation, .. } => *correlation,
                _ => None,
            };
            decision_correlation != intent_correlation
        })
        .count();

    // Threshold: the TDD example uses 10 unrelated decisions.
    if drift_count >= 5 {
        anomalies.push(Anomaly {
            kind: AnomalyKind::ObjectiveDrift,
            repetitions: drift_count as u32,
            tool: None,
            original_intent_seq: Some(intent_seq),
        });
    }
}
