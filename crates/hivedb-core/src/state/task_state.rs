use crate::event::{AgentId, Event, EventKind, StreamId};
use crate::projection::Projection;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::collections::BTreeMap;

/// Projection that tracks the latest `StateTransition` payload per (agent, stream).
#[derive(Clone, Debug, PartialEq, Default)]
pub struct TaskStateState {
    /// (agent, stream) -> latest state payload
    states: BTreeMap<(AgentId, StreamId), Value>,
}

impl TaskStateState {
    /// Returns the latest state for a given stream, if any.
    pub fn get(&self, agent_id: &AgentId, stream_id: &StreamId) -> Option<&Value> {
        self.states.get(&(agent_id.clone(), stream_id.clone()))
    }
}

impl Serialize for TaskStateState {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.states.len()))?;
        for (key, value) in &self.states {
            map.serialize_entry(key, &value.to_string())?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for TaskStateState {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw: BTreeMap<(AgentId, StreamId), String> = BTreeMap::deserialize(deserializer)?;
        let mut states = BTreeMap::new();
        for (key, value_str) in raw {
            let value = serde_json::from_str(&value_str).map_err(serde::de::Error::custom)?;
            states.insert(key, value);
        }
        Ok(TaskStateState { states })
    }
}

/// Marker type for the `TaskState` projection.
pub struct TaskState;

impl Projection for TaskState {
    type State = TaskStateState;

    fn name() -> &'static str {
        "TaskState"
    }

    fn merge(whole: &mut Self::State, part: &Self::State) {
        for (key, value) in &part.states {
            whole.states.insert(key.clone(), value.clone());
        }
    }

    fn apply(state: &mut Self::State, event: &Event) {
        if let EventKind::StateTransition = &event.kind {
            state.states.insert(
                (event.agent_id.clone(), event.stream_id.clone()),
                event.payload.clone(),
            );
        }
    }
}
