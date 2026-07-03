use crate::event::{AgentId, Event, EventKind, StreamId};
use crate::projection::Projection;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Projection that tracks currently valid facts per (agent, stream).
///
/// A fact is valid until a `MemoryInvalidate` event targets its sequence number.
/// The original event remains in the immutable log; only the derived state is
/// updated.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CurrentFactsState {
    /// (agent, stream) -> seq -> is_valid
    facts: BTreeMap<(AgentId, StreamId), BTreeMap<u64, bool>>,
}

impl CurrentFactsState {
    /// Returns true if the given seq is a currently valid fact.
    pub fn contains(&self, seq: u64) -> bool {
        self.facts
            .values()
            .any(|stream_facts| stream_facts.get(&seq).copied().unwrap_or(false))
    }

    /// Returns the sequence numbers of all valid facts for a given stream.
    pub fn valid_facts(&self, agent_id: &AgentId, stream_id: &StreamId) -> Vec<u64> {
        self.facts
            .get(&(agent_id.clone(), stream_id.clone()))
            .map(|m| {
                m.iter()
                    .filter(|(_, valid)| **valid)
                    .map(|(seq, _)| *seq)
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// Marker type for the `CurrentFacts` projection.
pub struct CurrentFacts;

impl Projection for CurrentFacts {
    type State = CurrentFactsState;

    fn name() -> &'static str {
        "CurrentFacts"
    }

    fn merge(whole: &mut Self::State, part: &Self::State) {
        for ((agent, stream), part_facts) in &part.facts {
            let whole_facts = whole
                .facts
                .entry((agent.clone(), stream.clone()))
                .or_default();
            for (seq, valid) in part_facts {
                whole_facts.insert(*seq, *valid);
            }
        }
    }

    fn apply(state: &mut Self::State, event: &Event) {
        match &event.kind {
            EventKind::Fact => {
                state
                    .facts
                    .entry((event.agent_id.clone(), event.stream_id.clone()))
                    .or_default()
                    .insert(event.seq, true);
            }
            EventKind::MemoryInvalidate { target_seq } => {
                for stream_facts in state.facts.values_mut() {
                    if let Some(valid) = stream_facts.get_mut(target_seq) {
                        *valid = false;
                    }
                }
            }
            _ => {}
        }
    }
}
