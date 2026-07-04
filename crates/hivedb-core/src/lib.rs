//! HiveDB core — event-log append-only engine with deterministic projections.

pub mod clock;
pub mod db;
pub mod error;
pub mod event;
pub mod log;
pub mod memory;
#[cfg(any(test, loom))]
pub(crate) mod memory_log;
pub mod projection;
pub mod reactive;
pub mod shard;
pub mod state;

pub use clock::{Clock, MockClock, SystemClock};
pub use db::{Decision, HiveDB, OpenOptions};
pub use error::{HiveError, HiveResult};
pub use event::{AgentId, Event, EventInput, EventKind, EventKindTag, Scope, StreamId};
pub use projection::Projection;
pub use reactive::{EventPattern, Predicate, Subscription};
pub use state::{
    consent_graph::{ConsentGraph, ConsentGraphState},
    current_facts::{CurrentFacts, CurrentFactsState},
    task_state::{TaskState, TaskStateState},
};

// Re-export hybrid-search types from the index layer so consumers only need
// one import.
pub use hivedb_index::{FieldBoosts, Fusion, Hit, HybridQuery, IndexDoc, ScalarFilter};
