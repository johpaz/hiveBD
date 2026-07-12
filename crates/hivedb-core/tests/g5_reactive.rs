mod common;

use common::{db, payload};
use hivedb_core::{
    AgentId, EventInput, EventKind, EventKindTag, EventPattern, Predicate, StreamId,
};
use serde_json::json;
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

#[tokio::test]
async fn subscription_filters_by_payload_equality() {
    let db = db();
    let mut sub = db.subscribe(EventPattern {
        kind: Some(EventKindTag::Fact),
        predicate: Some(Predicate::Eq {
            path: "/temperature".to_string(),
            value: json!(21.5),
        }),
        ..Default::default()
    });

    db.append(
        EventInput::new("A", "task-1", EventKind::Fact)
            .with_payload(json!({"temperature": 22.0, "room": "B"})),
    )
    .unwrap();
    db.append(
        EventInput::new("A", "task-1", EventKind::Fact)
            .with_payload(json!({"temperature": 21.5, "room": "A"})),
    )
    .unwrap();

    let ev = timeout(Duration::from_millis(500), sub.next())
        .await
        .expect("matching event must be delivered")
        .expect("event must be present");

    assert_eq!(ev.payload["temperature"], 21.5);
    assert_eq!(ev.payload["room"], "A");

    assert!(
        timeout(Duration::from_millis(100), sub.next())
            .await
            .is_err(),
        "non-matching event must not be delivered"
    );
}

#[tokio::test]
async fn subscription_filters_by_payload_contains() {
    let db = db();
    let mut sub = db.subscribe(EventPattern {
        kind: Some(EventKindTag::Fact),
        predicate: Some(Predicate::Contains {
            path: "/tags".to_string(),
            value: json!("urgent"),
        }),
        ..Default::default()
    });

    db.append(
        EventInput::new("A", "task-1", EventKind::Fact)
            .with_payload(json!({"tags": ["home"], "room": "B"})),
    )
    .unwrap();
    db.append(
        EventInput::new("A", "task-1", EventKind::Fact)
            .with_payload(json!({"tags": ["urgent", "home"], "room": "A"})),
    )
    .unwrap();

    let ev = timeout(Duration::from_millis(500), sub.next())
        .await
        .expect("matching event must be delivered")
        .expect("event must be present");

    assert_eq!(ev.payload["room"], "A");

    assert!(
        timeout(Duration::from_millis(100), sub.next())
            .await
            .is_err(),
        "non-matching event must not be delivered"
    );
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
