#![allow(dead_code)]

use hivedb_core::{AgentId, EventInput, EventKind, HiveDB, OpenOptions, StreamId, VectorOptions};
use serde_json::json;
use std::time::Duration;

pub fn db() -> HiveDB {
    HiveDB::open_temp().expect("open temp db")
}

pub fn vector_db() -> HiveDB {
    HiveDB::open_temp_with_options(OpenOptions {
        vector: Some(VectorOptions::new(384, "test:384")),
    })
    .expect("open temp vector db")
}

pub fn value() -> serde_json::Value {
    json!({"msg": "hello"})
}

pub fn payload() -> serde_json::Value {
    value()
}

pub fn fact(agent: impl Into<AgentId>, stream: impl Into<StreamId>) -> EventInput {
    EventInput::new(agent, stream, EventKind::Fact).with_payload(payload())
}

pub fn state_transition(
    agent: impl Into<AgentId>,
    stream: impl Into<StreamId>,
    payload: serde_json::Value,
) -> EventInput {
    EventInput::new(agent, stream, EventKind::StateTransition).with_payload(payload)
}

pub fn invalidate(agent: impl Into<AgentId>, target_seq: u64) -> EventInput {
    EventInput::new(
        agent,
        StreamId::from("default"),
        EventKind::MemoryInvalidate { target_seq },
    )
}

pub fn ttl_ms(ms: u64) -> Option<Duration> {
    Some(Duration::from_millis(ms))
}
