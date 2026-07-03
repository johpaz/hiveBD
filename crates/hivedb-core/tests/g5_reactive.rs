mod common;

use common::{db, payload};
use hivedb_core::{AgentId, EventInput, EventKind, EventKindTag, EventPattern, StreamId};
use std::time::Duration;
use tokio::time::timeout;

#[tokio::test]
async fn subscription_is_pushed_on_append() {
    let db = db();
    let mut sub = db.subscribe(EventPattern {
        kind: Some(EventKindTag::ToolCall),
        ..Default::default()
    });

    let db2 = db.clone();
    tokio::spawn(async move {
        db2.append(tool_call("A", "web_search")).unwrap();
    });

    let ev = timeout(Duration::from_millis(500), sub.next())
        .await
        .expect("subscription must be pushed without polling")
        .expect("event must be present");

    assert_eq!(ev.kind_tag(), "ToolCall");
}

#[tokio::test]
async fn subscription_delivers_at_least_once_with_seq() {
    let db = db();
    let mut sub = db.subscribe(EventPattern::all());

    let seq = db.append(fact("A")).unwrap();

    let ev = timeout(Duration::from_millis(500), sub.next())
        .await
        .expect("subscription must deliver")
        .expect("event must be present");

    assert_eq!(ev.seq, seq);
}

fn fact(agent: impl Into<AgentId>) -> EventInput {
    EventInput::new(agent, StreamId::from("task-1"), EventKind::Fact).with_payload(payload())
}

fn tool_call(agent: impl Into<AgentId>, tool: impl Into<String>) -> EventInput {
    EventInput::new(
        agent,
        StreamId::from("task-1"),
        EventKind::ToolCall { tool: tool.into() },
    )
    .with_payload(payload())
}
