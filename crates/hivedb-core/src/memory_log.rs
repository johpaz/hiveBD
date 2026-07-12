use crate::clock::Clock;
use crate::error::{HiveError, HiveResult};
use crate::event::{AgentId, Event, EventInput, StreamId};
use crate::projection::Projection;
use std::collections::HashMap;
use std::sync::Arc;

#[cfg(loom)]
mod sync {
    pub use loom::sync::Mutex;
    pub use loom::sync::atomic::{AtomicU64, Ordering};
}

#[cfg(not(loom))]
mod sync {
    pub use std::sync::Mutex;
    pub use std::sync::atomic::{AtomicU64, Ordering};
}

/// In-memory event log used by the loom model checker.
///
/// It preserves the same public contract as the sharded `EventLog` but does
/// not touch disk or `redb`, so loom can exhaustively explore thread
/// interleavings.
pub(crate) struct MemoryEventLog {
    shards: sync::Mutex<HashMap<AgentId, Vec<Event>>>,
    global: sync::Mutex<Vec<Event>>,
    next_seq: sync::AtomicU64,
    clock: Arc<dyn Clock>,
}

impl MemoryEventLog {
    pub(crate) fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            shards: sync::Mutex::new(HashMap::new()),
            global: sync::Mutex::new(Vec::new()),
            next_seq: sync::AtomicU64::new(1),
            clock,
        }
    }

    pub(crate) fn append(&self, input: EventInput) -> HiveResult<Event> {
        let seq = self.next_seq.fetch_add(1, sync::Ordering::SeqCst);
        let event = Event {
            seq,
            agent_id: input.agent_id.clone(),
            stream_id: input.stream_id,
            kind: input.kind,
            timestamp: self.clock.now_ms(),
            causation: input.causation,
            correlation: input.correlation,
            payload: input.payload,
        };

        let mut shards = self.shards.lock().unwrap();
        shards
            .entry(input.agent_id)
            .or_default()
            .push(event.clone());

        // Global shard receives the same events the sharded log would store
        // there (consent-related events). For the loom test we simply mirror
        // everything; projections are not materialized anyway.
        self.global.lock().unwrap().push(event.clone());

        Ok(event)
    }

    pub(crate) fn read(&self, seq: u64) -> HiveResult<Event> {
        let shards = self.shards.lock().unwrap();
        for events in shards.values() {
            if let Some(event) = events.iter().find(|e| e.seq == seq) {
                return Ok(event.clone());
            }
        }
        let global = self.global.lock().unwrap();
        if let Some(event) = global.iter().find(|e| e.seq == seq) {
            return Ok(event.clone());
        }
        Err(HiveError::NotFound(format!("event seq={seq}")))
    }

    pub(crate) fn len(&self) -> HiveResult<u64> {
        let next = self.next_seq.load(sync::Ordering::SeqCst);
        Ok(if next == 1 { 0 } else { next - 1 })
    }

    pub(crate) fn read_stream(
        &self,
        agent_id: &AgentId,
        stream_id: &StreamId,
    ) -> HiveResult<Vec<Event>> {
        let shards = self.shards.lock().unwrap();
        let mut out: Vec<Event> = shards
            .get(agent_id)
            .map(|events| {
                events
                    .iter()
                    .filter(|e| &e.stream_id == stream_id)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        out.sort_by_key(|e| e.seq);
        Ok(out)
    }

    pub(crate) fn read_stream_all_agents(&self, stream_id: &StreamId) -> HiveResult<Vec<Event>> {
        let shards = self.shards.lock().unwrap();
        let mut out: Vec<Event> = shards
            .values()
            .flat_map(|events| events.iter().filter(|e| &e.stream_id == stream_id).cloned())
            .collect();
        out.sort_by_key(|e| e.seq);
        Ok(out)
    }

    pub(crate) fn project<P: Projection>(&self) -> HiveResult<P::State> {
        // In-memory backend does not materialize projections.
        Ok(P::State::default())
    }

    pub(crate) fn wipe_projections_and_rebuild(&self) -> HiveResult<()> {
        Ok(())
    }
}
