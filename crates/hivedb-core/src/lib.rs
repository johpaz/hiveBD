//! HiveDB core — event-log append-only engine with deterministic projections.

pub mod causal;
pub mod clock;
pub mod collections;
pub mod context;
pub mod db;
pub mod error;
pub mod event;
pub mod harness;
pub mod log;
pub mod memory;
#[cfg(any(test, loom))]
pub(crate) mod memory_log;
pub mod projection;
pub mod reactive;
pub mod shard;
pub mod state;

pub use clock::{Clock, MockClock, SystemClock};
pub use collections::{ColOp, Collections, DocEntry, PutOptions, ScanOptions};
pub use db::{Decision, HiveDB, OpenOptions};
pub use error::{HiveError, HiveResult};
pub use event::{AgentId, Event, EventInput, EventKind, EventKindTag, Scope, StreamId};
pub use projection::Projection;
pub use reactive::{EventPattern, Predicate, Subscription};
pub use state::{
    causal_thread::{CausalThreadProjection, CausalThreadState, ThreadEvent, ToolOutcome},
    consent_graph::{ConsentGraph, ConsentGraphState},
    current_facts::{CurrentFacts, CurrentFactsState},
    task_state::{TaskState, TaskStateState},
    tool_ledger::{ToolLedger, ToolLedgerState, ToolStats},
};

pub use causal::{Anomaly, AnomalyKind, CausalThread, DecisionNode, ToolCallNode};
pub use context::{
    AgentContext, AgentContextRequest, AnomalyConfig, ContextItem, ContextStrategy, Episode,
    EpisodicConfig, PhaseSummary,
};
pub use harness::{
    Finding, FindingKind, HarnessEvaluation, HarnessInput, HarnessLoop, LearningProposal, RootCause,
};

// Re-export hybrid-search types from the index layer so consumers only need
// one import.
pub use hivedb_index::{FieldBoosts, Fusion, Hit, HybridQuery, IndexDoc, ScalarFilter};
