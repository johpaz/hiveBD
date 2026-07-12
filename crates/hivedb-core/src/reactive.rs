use crate::event::{AgentId, Event, EventKindTag, StreamId};
use dashmap::DashMap;
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

/// A predicate over the payload of an event.
#[derive(Clone, Debug, PartialEq)]
pub enum Predicate {
    /// Always matches.
    Always,
    /// Equality of the value at a JSON pointer path.
    Eq { path: String, value: Value },
    /// Membership/containment: array contains value, or string contains substring.
    Contains { path: String, value: Value },
}

impl Predicate {
    /// Evaluate the predicate against an event payload.
    ///
    /// `path` is interpreted as a JSON pointer (`/field/nested`). If it does not
    /// start with `/`, a leading slash is inserted automatically.
    pub fn matches(&self, payload: &Value) -> bool {
        match self {
            Predicate::Always => true,
            Predicate::Eq { path, value } => {
                let pointer = normalize_pointer(path);
                payload.pointer(&pointer) == Some(value)
            }
            Predicate::Contains { path, value } => {
                let pointer = normalize_pointer(path);
                match payload.pointer(&pointer) {
                    Some(Value::Array(arr)) => arr.contains(value),
                    Some(Value::String(s)) => match value {
                        Value::String(sub) => s.contains(sub),
                        other => s.contains(&other.to_string()),
                    },
                    Some(other) => other == value,
                    None => false,
                }
            }
        }
    }
}

fn normalize_pointer(path: &str) -> String {
    if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    }
}

/// Pattern used to filter events delivered to a subscription.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct EventPattern {
    pub agent_id: Option<AgentId>,
    pub kind: Option<EventKindTag>,
    pub stream_id: Option<StreamId>,
    pub predicate: Option<Predicate>,
}

impl EventPattern {
    /// Matches any event.
    pub fn all() -> Self {
        Self::default()
    }
}

/// A subscription to a stream of events.
///
/// Drop the subscription to cancel it.
pub struct Subscription {
    id: u64,
    receiver: UnboundedReceiver<Event>,
    engine_handle: std::sync::Weak<DashMap<u64, (EventPattern, UnboundedSender<Event>)>>,
}

impl Subscription {
    /// Wait for the next matching event.
    pub async fn next(&mut self) -> Option<Event> {
        self.receiver.recv().await
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        if let Some(engine) = self.engine_handle.upgrade() {
            engine.remove(&self.id);
        }
    }
}

/// Reactive engine that dispatches events to matching subscribers.
#[derive(Debug)]
pub(crate) struct ReactiveEngine {
    subscribers: Arc<DashMap<u64, (EventPattern, UnboundedSender<Event>)>>,
    next_id: AtomicU64,
}

use std::sync::Arc;

impl ReactiveEngine {
    pub(crate) fn new() -> Self {
        Self {
            subscribers: Arc::new(DashMap::new()),
            next_id: AtomicU64::new(1),
        }
    }

    pub(crate) fn subscribe(&self, pattern: EventPattern) -> Subscription {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = unbounded_channel();
        self.subscribers.insert(id, (pattern, tx));
        Subscription {
            id,
            receiver: rx,
            engine_handle: Arc::downgrade(&self.subscribers),
        }
    }

    pub(crate) fn dispatch(&self, event: &Event) {
        for entry in self.subscribers.iter() {
            let (pattern, tx) = entry.value();
            if matches(pattern, event) {
                // Unbounded channel never blocks; if the receiver is dropped the
                // send fails silently, which is fine for at-least-once semantics.
                let _ = tx.send(event.clone());
            }
        }
    }
}

fn matches(pattern: &EventPattern, event: &Event) -> bool {
    if let Some(agent_id) = &pattern.agent_id
        && agent_id != &event.agent_id
    {
        return false;
    }
    if let Some(kind) = &pattern.kind
        && kind != &EventKindTag::from(&event.kind)
    {
        return false;
    }
    if let Some(stream_id) = &pattern.stream_id
        && stream_id != &event.stream_id
    {
        return false;
    }
    if let Some(predicate) = &pattern.predicate
        && !predicate.matches(&event.payload)
    {
        return false;
    }
    true
}
