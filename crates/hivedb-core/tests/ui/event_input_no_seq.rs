use hivedb_core::{AgentId, EventInput, EventKind, StreamId};

fn main() {
    let _ = EventInput {
        agent_id: AgentId::from("A"),
        stream_id: StreamId::from("s"),
        kind: EventKind::Fact,
        seq: 1,
        timestamp: 0,
        causation: None,
        correlation: None,
        payload: serde_json::Value::Null,
    };
}
