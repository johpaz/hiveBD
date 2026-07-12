use crate::clock::{Clock, SystemClock, into_clock};
use crate::collections::{ColOp, Collections, DocEntry, PutOptions, ScanOptions};
use crate::error::HiveResult;
use crate::event::{AgentId, Event, EventInput, EventKind, StreamId};
use crate::log::EventLog;
use crate::memory::WorkingMemory;
#[cfg(any(test, loom))]
use crate::memory_log::MemoryEventLog;
use crate::projection::{Projection, ProjectionRegistry};
use crate::reactive::{EventPattern, ReactiveEngine, Subscription};
use crate::state::{
    consent_graph::ConsentGraph,
    current_facts::CurrentFacts,
    task_state::TaskState,
    tool_ledger::{ToolLedger, ToolStats},
};
use hivedb_index::{Hit, HybridQuery, IndexDoc, ScalarFilter, SemanticIndex};
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

const DEFAULT_VECTOR_DIMENSION: usize = 384;

/// File under the base directory recording immutable per-database settings.
const META_FILE: &str = "meta.json";

/// Options for opening a database.
#[derive(Clone, Copy, Debug)]
pub struct OpenOptions {
    /// Dimension of vectors accepted by the semantic index. Fixed at first
    /// open; reopening with a different value is an error.
    pub vector_dimension: usize,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            vector_dimension: DEFAULT_VECTOR_DIMENSION,
        }
    }
}

/// Load the persisted vector dimension, or persist `requested` on first open.
/// Returns an error if the database was created with a different dimension.
fn resolve_vector_dimension(base: &Path, requested: usize) -> HiveResult<usize> {
    let meta_path = base.join(META_FILE);
    if meta_path.exists() {
        let raw = std::fs::read_to_string(&meta_path)?;
        let meta: Value = serde_json::from_str(&raw).map_err(|e| {
            crate::error::HiveError::InvalidInput(format!("corrupt {META_FILE}: {e}"))
        })?;
        let stored = meta
            .get("vector_dimension")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| {
                crate::error::HiveError::InvalidInput(format!(
                    "corrupt {META_FILE}: missing vector_dimension"
                ))
            })? as usize;
        if stored != requested {
            return Err(crate::error::HiveError::InvalidInput(format!(
                "database was created with vector_dimension={stored}, \
                 cannot reopen with vector_dimension={requested}"
            )));
        }
        Ok(stored)
    } else {
        let meta = serde_json::json!({ "vector_dimension": requested });
        std::fs::write(&meta_path, meta.to_string())?;
        Ok(requested)
    }
}

/// Public handle to a HiveDB database.
///
/// All writes happen through [`HiveDB::append`]; the log is immutable once an
/// event receives a `seq`.
#[derive(Clone)]
pub struct HiveDB {
    log: LogHandle,
    working: Arc<WorkingMemory>,
    semantic: Option<Arc<SemanticIndex>>,
    collections: Option<Arc<Collections>>,
    reactive: Arc<ReactiveEngine>,
    clock: Arc<dyn Clock>,
    base_path: PathBuf,
    /// Keeps ephemeral databases (`open_temp` / `":memory:"`) alive: the
    /// directory is removed from disk when the last clone drops. `None` for
    /// persistent databases.
    _temp_dir: Option<Arc<tempfile::TempDir>>,
}

/// Internal handle that hides whether the log is backed by sharded `redb`
/// files or by an in-memory structure used for concurrency model checking.
#[derive(Clone)]
enum LogHandle {
    Redb(Arc<EventLog>),
    #[cfg(any(test, loom))]
    Memory(Arc<MemoryEventLog>),
}

impl LogHandle {
    fn append(&self, input: EventInput) -> HiveResult<Event> {
        match self {
            LogHandle::Redb(log) => log.append(input),
            #[cfg(any(test, loom))]
            LogHandle::Memory(log) => log.append(input),
        }
    }

    fn read(&self, seq: u64) -> HiveResult<Event> {
        match self {
            LogHandle::Redb(log) => log.read(seq),
            #[cfg(any(test, loom))]
            LogHandle::Memory(log) => log.read(seq),
        }
    }

    fn len(&self) -> HiveResult<u64> {
        match self {
            LogHandle::Redb(log) => log.len(),
            #[cfg(any(test, loom))]
            LogHandle::Memory(log) => log.len(),
        }
    }

    fn last_seq(&self) -> HiveResult<u64> {
        match self {
            LogHandle::Redb(log) => log.last_seq(),
            #[cfg(any(test, loom))]
            LogHandle::Memory(log) => log.len(),
        }
    }

    fn read_stream(&self, agent_id: &AgentId, stream_id: &StreamId) -> HiveResult<Vec<Event>> {
        match self {
            LogHandle::Redb(log) => log.read_stream(agent_id, stream_id),
            #[cfg(any(test, loom))]
            LogHandle::Memory(log) => log.read_stream(agent_id, stream_id),
        }
    }

    fn read_stream_all_agents(&self, stream_id: &StreamId) -> HiveResult<Vec<Event>> {
        match self {
            LogHandle::Redb(log) => log.read_stream_all_agents(stream_id),
            #[cfg(any(test, loom))]
            LogHandle::Memory(log) => log.read_stream_all_agents(stream_id),
        }
    }

    fn project<P: Projection>(&self) -> HiveResult<P::State> {
        match self {
            LogHandle::Redb(log) => log.project::<P>(),
            #[cfg(any(test, loom))]
            LogHandle::Memory(log) => log.project::<P>(),
        }
    }

    fn projection_checkpoint<P: Projection>(&self) -> HiveResult<u64> {
        match self {
            LogHandle::Redb(log) => log.projection_checkpoint::<P>(),
            #[cfg(any(test, loom))]
            LogHandle::Memory(_) => Ok(0),
        }
    }

    fn wipe_projections_and_rebuild(&self) -> HiveResult<()> {
        match self {
            LogHandle::Redb(log) => log.wipe_projections_and_rebuild(),
            #[cfg(any(test, loom))]
            LogHandle::Memory(log) => log.wipe_projections_and_rebuild(),
        }
    }

    fn flush_next_seq(&self) -> HiveResult<()> {
        match self {
            LogHandle::Redb(log) => log.flush_next_seq(),
            #[cfg(any(test, loom))]
            LogHandle::Memory(_) => Ok(()),
        }
    }
}

impl HiveDB {
    /// Open a database at the given path, creating it if necessary.
    pub fn open<P: AsRef<Path>>(path: P) -> HiveResult<Self> {
        Self::open_with_options(path, OpenOptions::default())
    }

    /// Open a database with explicit options.
    pub fn open_with_options<P: AsRef<Path>>(path: P, options: OpenOptions) -> HiveResult<Self> {
        Self::open_with_clock_and_options(path, into_clock(SystemClock), options)
    }

    /// Open a database with an explicit clock source.
    #[doc(hidden)]
    pub fn open_with_clock<P: AsRef<Path>>(path: P, clock: Arc<dyn Clock>) -> HiveResult<Self> {
        Self::open_with_clock_and_options(path, clock, OpenOptions::default())
    }

    /// Open a database with an explicit clock source and options.
    #[doc(hidden)]
    pub fn open_with_clock_and_options<P: AsRef<Path>>(
        path: P,
        clock: Arc<dyn Clock>,
        options: OpenOptions,
    ) -> HiveResult<Self> {
        let base = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&base)?;

        let dimension = resolve_vector_dimension(&base, options.vector_dimension)?;

        let registry = default_registry();
        let log = LogHandle::Redb(Arc::new(EventLog::open(&base, registry, clock.clone())?));
        let working = Arc::new(WorkingMemory::new());
        let semantic = Some(Arc::new(SemanticIndex::open(&base, dimension)?));
        let collections = Some(Arc::new(Collections::open(&base)?));
        let reactive = Arc::new(ReactiveEngine::new());

        Ok(Self {
            log,
            working,
            semantic,
            collections,
            reactive,
            clock,
            base_path: base,
            _temp_dir: None,
        })
    }

    /// Open a temporary database. The data directory is removed from disk
    /// when the last clone of the handle drops.
    pub fn open_temp() -> HiveResult<Self> {
        Self::open_temp_with_clock(into_clock(SystemClock))
    }

    /// Open an in-memory database for concurrency model checking.
    ///
    /// The semantic index still uses a temporary directory because the index
    /// layer is not loom-aware.
    #[cfg(any(test, loom))]
    #[doc(hidden)]
    pub fn open_in_memory() -> HiveResult<Self> {
        Self::open_in_memory_with_clock(into_clock(SystemClock))
    }

    /// Open an in-memory database with an explicit clock source.
    #[cfg(any(test, loom))]
    #[doc(hidden)]
    pub fn open_in_memory_with_clock(clock: Arc<dyn Clock>) -> HiveResult<Self> {
        let dir = tempfile::tempdir()?;
        let base = dir.path().to_path_buf();

        let log = LogHandle::Memory(Arc::new(MemoryEventLog::new(clock.clone())));
        let working = Arc::new(WorkingMemory::new());
        let semantic = None;
        let collections = None;
        let reactive = Arc::new(ReactiveEngine::new());

        Ok(Self {
            log,
            working,
            semantic,
            collections,
            reactive,
            clock,
            base_path: base,
            _temp_dir: Some(Arc::new(dir)),
        })
    }

    /// Open a temporary database with an explicit clock source.
    #[doc(hidden)]
    pub fn open_temp_with_clock(clock: Arc<dyn Clock>) -> HiveResult<Self> {
        Self::open_temp_with_clock_and_options(clock, OpenOptions::default())
    }

    /// Open a temporary database with an explicit clock source and options.
    /// The backing directory is removed when the last clone drops.
    #[doc(hidden)]
    pub fn open_temp_with_clock_and_options(
        clock: Arc<dyn Clock>,
        options: OpenOptions,
    ) -> HiveResult<Self> {
        let dir = tempfile::tempdir()?;
        let mut db = Self::open_with_clock_and_options(dir.path(), clock, options)?;
        db._temp_dir = Some(Arc::new(dir));
        Ok(db)
    }

    /// Open a temporary database with explicit options.
    pub fn open_temp_with_options(options: OpenOptions) -> HiveResult<Self> {
        Self::open_temp_with_clock_and_options(into_clock(SystemClock), options)
    }

    /// Advance the clock to `timestamp_ms`.
    ///
    /// For test clocks (e.g. [`MockClock`]) this moves time forward. For the
    /// system clock it is a no-op.
    #[doc(hidden)]
    pub fn advance_clock_to(&self, timestamp_ms: u64) {
        self.clock.advance_clock_to(timestamp_ms);
    }

    /// Append a new event to the log.
    ///
    /// Returns the engine-assigned global sequence number.
    pub fn append(&self, input: EventInput) -> HiveResult<u64> {
        let event = self.log.append(input)?;
        self.reactive.dispatch(&event);
        Ok(event.seq)
    }

    /// Read a single event by sequence number.
    pub fn read(&self, seq: u64) -> HiveResult<Event> {
        self.log.read(seq)
    }

    /// Query the current state of a projection.
    pub fn project<P: Projection>(&self) -> HiveResult<P::State> {
        self.log.project::<P>()
    }

    /// Returns the last sequence number applied to a projection.
    pub fn projection_checkpoint<P: Projection>(&self) -> HiveResult<u64> {
        self.log.projection_checkpoint::<P>()
    }

    /// Returns aggregated statistics for a tool, if any `ToolCall` events have
    /// been recorded for it.
    pub fn tool_stats(&self, tool: &str) -> HiveResult<Option<ToolStats>> {
        let state = self.log.project::<ToolLedger>()?;
        Ok(state.get(tool).cloned())
    }

    /// Store a value in working memory with an optional TTL.
    pub fn working_set(
        &self,
        agent_id: impl Into<AgentId>,
        key: impl Into<String>,
        value: Value,
        ttl: Option<Duration>,
    ) {
        self.working.set(agent_id.into(), key.into(), value, ttl);
    }

    /// Retrieve a value from working memory, returning `None` if expired.
    pub fn working_get(&self, agent_id: impl Into<AgentId>, key: &str) -> Option<Value> {
        self.working.get(&agent_id.into(), key)
    }

    /// Return all non-expired keys for an agent.
    pub fn working_keys(&self, agent_id: impl Into<AgentId>) -> Vec<String> {
        self.working.keys(&agent_id.into())
    }

    fn semantic(&self) -> HiveResult<&SemanticIndex> {
        self.semantic.as_deref().ok_or_else(|| {
            crate::error::HiveError::InvalidInput(
                "semantic index not available in in-memory mode".into(),
            )
        })
    }

    /// Insert or replace a document in the semantic index.
    pub fn upsert_doc(&self, doc: &IndexDoc) -> HiveResult<()> {
        self.semantic()?.upsert(doc).map_err(Into::into)
    }

    /// Insert or replace a batch of documents under a single text-index
    /// commit.
    pub fn upsert_batch(&self, docs: &[IndexDoc]) -> HiveResult<()> {
        self.semantic()?.upsert_batch(docs).map_err(Into::into)
    }

    /// Delete a document from the semantic index. Missing ids are a no-op.
    pub fn delete_doc(&self, id: &str) -> HiveResult<()> {
        self.semantic()?.delete(id).map_err(Into::into)
    }

    /// Delete every indexed document carrying the given scalar filter.
    pub fn delete_by_filter(&self, filter: &ScalarFilter) -> HiveResult<()> {
        self.semantic()?
            .delete_by_filter(filter)
            .map_err(Into::into)
    }

    /// Remove every document from the semantic index.
    pub fn clear_index(&self) -> HiveResult<()> {
        self.semantic()?.clear().map_err(Into::into)
    }

    fn collections(&self) -> HiveResult<&Collections> {
        self.collections.as_deref().ok_or_else(|| {
            crate::error::HiveError::InvalidInput(
                "collections not available in in-memory mode".into(),
            )
        })
    }

    /// Insert or replace a JSON document in a collection. Returns the new
    /// version (starts at 1).
    pub fn col_put(
        &self,
        collection: &str,
        id: &str,
        doc: &Value,
        options: PutOptions,
    ) -> HiveResult<u64> {
        self.collections()?.put(collection, id, doc, options)
    }

    /// Read a document by id.
    pub fn col_get(&self, collection: &str, id: &str) -> HiveResult<Option<DocEntry>> {
        self.collections()?.get(collection, id)
    }

    /// Delete a document. Returns `true` if it existed.
    pub fn col_delete(&self, collection: &str, id: &str) -> HiveResult<bool> {
        self.collections()?.delete(collection, id)
    }

    /// Scan a collection in id order.
    pub fn col_scan(&self, collection: &str, options: &ScanOptions) -> HiveResult<Vec<DocEntry>> {
        self.collections()?.scan(collection, options)
    }

    /// Number of documents in a collection.
    pub fn col_count(&self, collection: &str) -> HiveResult<u64> {
        self.collections()?.count(collection)
    }

    /// Create an equality index on a top-level field (optionally unique).
    pub fn col_create_index(&self, collection: &str, field: &str, unique: bool) -> HiveResult<()> {
        self.collections()?.create_index(collection, field, unique)
    }

    /// Look up documents whose indexed field equals `value`.
    pub fn col_find_by(
        &self,
        collection: &str,
        field: &str,
        value: &Value,
        options: &ScanOptions,
    ) -> HiveResult<Vec<DocEntry>> {
        self.collections()?
            .find_by(collection, field, value, options)
    }

    /// Apply several puts/deletes atomically across collections.
    pub fn col_batch(&self, ops: &[ColOp]) -> HiveResult<()> {
        self.collections()?.batch(ops)
    }

    /// Index a document for hybrid search.
    ///
    /// Deprecated shim over [`HiveDB::upsert_doc`]: `text` maps to the `body`
    /// field.
    pub fn index_doc(
        &self,
        id: impl Into<String>,
        text: impl Into<String>,
        vector: Vec<f32>,
    ) -> HiveResult<()> {
        self.index_doc_with(id, text, vector, &[])
    }

    /// Index a document with scalar filters for hybrid search.
    ///
    /// Deprecated shim over [`HiveDB::upsert_doc`]: `text` maps to the `body`
    /// field.
    pub fn index_doc_with(
        &self,
        id: impl Into<String>,
        text: impl Into<String>,
        vector: Vec<f32>,
        filters: &[ScalarFilter],
    ) -> HiveResult<()> {
        let doc = IndexDoc::new(id)
            .with_body(text)
            .with_vector(vector)
            .with_filters(filters.to_vec());
        self.upsert_doc(&doc)
    }

    /// Execute a hybrid search query.
    pub fn query_hybrid(&self, query: HybridQuery) -> HiveResult<Vec<Hit>> {
        self.semantic()?.query_hybrid(query).map_err(Into::into)
    }

    /// Subscribe to a pattern of events.
    pub fn subscribe(&self, pattern: EventPattern) -> Subscription {
        self.reactive.subscribe(pattern)
    }

    /// Wipe all materialized projection state and rebuild it from the log.
    #[doc(hidden)]
    pub fn wipe_projections_and_rebuild(&self) -> HiveResult<()> {
        self.log.wipe_projections_and_rebuild()
    }

    /// Number of events currently stored in the log.
    pub fn log_len(&self) -> HiveResult<u64> {
        self.log.len()
    }

    /// Returns the highest assigned sequence number, or 0 if the log is empty.
    pub fn last_seq(&self) -> HiveResult<u64> {
        self.log.last_seq()
    }

    /// Read all events for a given agent/stream in ascending order.
    pub fn read_stream(&self, agent_id: &AgentId, stream_id: &StreamId) -> HiveResult<Vec<Event>> {
        self.log.read_stream(agent_id, stream_id)
    }

    /// Returns the base path of the database.
    pub fn path(&self) -> &Path {
        &self.base_path
    }

    /// Returns true if `agent` is authorized to perform `action` on `resource`
    /// according to the current consent graph.
    ///
    /// This method appends an `IntentLogged` event to the log for audit
    /// purposes. The returned `Decision` contains the sequence number of that
    /// event and, if allowed, the grant that authorized it.
    pub fn can(
        &self,
        agent: impl Into<AgentId>,
        action: impl Into<String>,
        resource: impl Into<String>,
    ) -> HiveResult<Decision> {
        let agent = agent.into();
        let action = action.into();
        let resource = resource.into();

        let state = self.log.project::<ConsentGraph>()?;
        let now = self.clock.now_ms();
        let authorized_by = state.find_active_grant(&agent, &action, &resource, now);

        let intent_seq = self.append(EventInput::new(
            agent.clone(),
            StreamId::from("consent"),
            EventKind::IntentLogged {
                actor: agent,
                intent: format!("{}:{}", action, resource),
                authorized_by,
            },
        ))?;

        Ok(Decision {
            allowed: authorized_by.is_some(),
            intent_log_seq: Some(intent_seq),
        })
    }

    /// Build the causal thread for a given stream.
    pub fn causal_thread(
        &self,
        stream_id: impl Into<StreamId>,
    ) -> HiveResult<crate::causal::CausalThread> {
        let stream_id = stream_id.into();
        let events = self.log.read_stream_all_agents(&stream_id)?;
        Ok(crate::causal::CausalThread::from_events(&events))
    }
}

impl Drop for HiveDB {
    fn drop(&mut self) {
        // Best-effort flush of the next sequence number so graceful shutdowns
        // avoid a full shard scan on the next open.
        let _ = self.log.flush_next_seq();
    }
}

/// Result of an authorization check.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Decision {
    allowed: bool,
    intent_log_seq: Option<u64>,
}

impl Decision {
    /// True if the action is authorized.
    pub fn allowed(&self) -> bool {
        self.allowed
    }

    /// Sequence number of the `IntentLogged` event recording this decision.
    pub fn intent_log_seq(&self) -> Option<u64> {
        self.intent_log_seq
    }
}

fn default_registry() -> ProjectionRegistry {
    let mut registry = ProjectionRegistry::empty();
    registry.register::<CurrentFacts>();
    registry.register::<TaskState>();
    registry.register::<ConsentGraph>();
    registry.register::<ToolLedger>();
    registry
}

impl From<hivedb_index::IndexError> for crate::error::HiveError {
    fn from(e: hivedb_index::IndexError) -> Self {
        crate::error::HiveError::InvalidInput(e.to_string())
    }
}
