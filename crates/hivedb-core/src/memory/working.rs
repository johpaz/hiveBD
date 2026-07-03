use crate::event::AgentId;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use serde_json::Value;
use std::time::{Duration, Instant};

/// An entry in working memory with an optional expiration time.
#[derive(Clone, Debug)]
struct WorkingEntry {
    value: Value,
    expires_at: Option<Instant>,
}

/// In-memory key/value blackboard with per-entry TTL.
///
/// Working memory is not persisted to the event log; it is ephemeral by design.
/// Expiration is lazy: entries are pruned on read or enumeration.
#[derive(Debug, Default)]
pub struct WorkingMemory {
    store: DashMap<(AgentId, String), WorkingEntry>,
}

impl WorkingMemory {
    pub fn new() -> Self {
        Self {
            store: DashMap::new(),
        }
    }

    /// Store a value with an optional TTL.
    pub fn set(&self, agent_id: AgentId, key: String, value: Value, ttl: Option<Duration>) {
        let expires_at = ttl.map(|d| Instant::now() + d);
        self.store
            .insert((agent_id, key), WorkingEntry { value, expires_at });
    }

    /// Retrieve a value, removing it first if it has expired.
    pub fn get(&self, agent_id: &AgentId, key: &str) -> Option<Value> {
        let k = (agent_id.clone(), key.to_string());
        match self.store.entry(k) {
            Entry::Occupied(entry) => {
                if is_expired(entry.get()) {
                    entry.remove();
                    None
                } else {
                    Some(entry.get().value.clone())
                }
            }
            Entry::Vacant(_) => None,
        }
    }

    /// Return all non-expired keys for an agent.
    pub fn keys(&self, agent_id: &AgentId) -> Vec<String> {
        self.store
            .iter()
            .filter(|entry| &entry.key().0 == agent_id)
            .filter(|entry| !is_expired(entry.value()))
            .map(|entry| entry.key().1.clone())
            .collect()
    }
}

fn is_expired(entry: &WorkingEntry) -> bool {
    entry
        .expires_at
        .map(|expires_at| Instant::now() > expires_at)
        .unwrap_or(false)
}
