use crate::clock::Clock;
use crate::error::{HiveError, HiveResult};
use crate::event::{AgentId, Event, EventInput};
use crate::projection::{
    EventLogInternal, Projection, ProjectionRegistry, ProjectionScope, ProjectionStore,
};
use crate::shard::AgentShard;
use dashmap::DashMap;
use redb::{ReadableTable, TableDefinition};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const SHARDS_DIR: &str = "shards";
const GLOBAL_SHARD: &str = "_global.redb";

const EVENTS_TABLE: TableDefinition<u64, Vec<u8>> = TableDefinition::new("events");
const PROJECTION_CHECKPOINTS: TableDefinition<&str, u64> =
    TableDefinition::new("projection_checkpoints");
const PROJECTION_STATE: TableDefinition<&str, Vec<u8>> = TableDefinition::new("projection_state");

/// Internal sharded event-log implementation on top of `redb`.
pub(crate) struct EventLog {
    base: PathBuf,
    shards: DashMap<AgentId, Arc<AgentShard>>,
    global: AgentShard,
    registry: ProjectionRegistry,
    clock: Arc<dyn Clock>,
    next_seq: AtomicU64,
    seq_to_agent: DashMap<u64, AgentId>,
}

impl EventLog {
    pub(crate) fn open<P: AsRef<Path>>(
        base: P,
        registry: ProjectionRegistry,
        clock: Arc<dyn Clock>,
    ) -> HiveResult<Self> {
        let base = base.as_ref().to_path_buf();
        let shards_dir = base.join(SHARDS_DIR);
        std::fs::create_dir_all(&shards_dir)?;

        let global_path = shards_dir.join(GLOBAL_SHARD);
        let global = AgentShard::open(global_path)?;

        let log = Self {
            base,
            shards: DashMap::new(),
            global,
            registry,
            clock,
            next_seq: AtomicU64::new(1),
            seq_to_agent: DashMap::new(),
        };

        log.load_existing_shards()?;
        log.recover_projections()?;
        Ok(log)
    }

    fn shards_dir(&self) -> PathBuf {
        self.base.join(SHARDS_DIR)
    }

    fn shard_path(&self, agent_id: &AgentId) -> PathBuf {
        // Sanitize the agent id for use as a file name. For the test data we
        // use simple identifiers; replace path separators and dots to be safe.
        let mut name = agent_id.0.clone();
        name = name.replace(['/', '\\', '.'], "_");
        name = name.replace("..", "_");
        self.shards_dir().join(format!("{}.redb", name))
    }

    fn get_or_create_shard(&self, agent_id: &AgentId) -> HiveResult<Arc<AgentShard>> {
        match self.shards.get(agent_id) {
            Some(entry) => Ok(Arc::clone(entry.value())),
            None => {
                let shard = Arc::new(AgentShard::open(self.shard_path(agent_id))?);
                self.shards.insert(agent_id.clone(), Arc::clone(&shard));
                Ok(shard)
            }
        }
    }

    fn load_existing_shards(&self) -> HiveResult<()> {
        let mut max_seq = 0u64;
        let entries = std::fs::read_dir(self.shards_dir())?;
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            let name = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
            let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");
            if ext != "redb" || name == "_global" {
                continue;
            }

            let agent_id = AgentId(name.to_string());
            let shard = Arc::new(AgentShard::open(&path)?);
            for event in shard.iter_events()? {
                self.seq_to_agent.insert(event.seq, agent_id.clone());
                if event.seq > max_seq {
                    max_seq = event.seq;
                }
            }
            self.shards.insert(agent_id, Arc::clone(&shard));
        }
        self.next_seq.store(max_seq + 1, Ordering::SeqCst);
        Ok(())
    }

    /// Append a new event and atomically update projections within the agent
    /// shard (and the global shard if the event affects global projections).
    pub(crate) fn append(&self, input: EventInput) -> HiveResult<Event> {
        let shard = self.get_or_create_shard(&input.agent_id)?;

        // Assign the seq while holding every write lock the event will touch
        // (agent shard first, then global — a fixed order that prevents
        // deadlocks). This guarantees projections are applied in seq order
        // within each shard.
        let _agent_guard = shard.lock_writes();
        let is_global = affects_global_projections(&input.kind);
        let _global_guard = is_global.then(|| self.global.lock_writes());

        let timestamp = self.clock.now_ms();
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);

        let event = Event {
            seq,
            agent_id: input.agent_id.clone(),
            stream_id: input.stream_id,
            kind: input.kind,
            timestamp,
            causation: input.causation,
            correlation: input.correlation,
            payload: input.payload,
        };

        shard.append_event(&event, &mut self.registry.agent_handlers())?;

        if is_global {
            self.global
                .append_event(&event, &mut self.registry.global_handlers())?;
        }

        self.seq_to_agent.insert(seq, input.agent_id);
        Ok(event)
    }

    /// Read a single event by sequence number.
    pub(crate) fn read(&self, seq: u64) -> HiveResult<Event> {
        match self.seq_to_agent.get(&seq) {
            Some(entry) => {
                let agent_id = entry.value();
                match self.shards.get(agent_id) {
                    Some(shard) => match shard.read_event(seq)? {
                        Some(event) => Ok(event),
                        None => Err(HiveError::NotFound(format!("event seq={seq}"))),
                    },
                    None => Err(HiveError::NotFound(format!("shard for agent {agent_id}"))),
                }
            }
            None => Err(HiveError::NotFound(format!("event seq={seq}"))),
        }
    }

    /// Returns the highest assigned sequence number, or 0 if the log is empty.
    pub(crate) fn last_seq(&self) -> HiveResult<u64> {
        let next = self.next_seq.load(Ordering::SeqCst);
        if next == 1 { Ok(0) } else { Ok(next - 1) }
    }

    /// Returns the number of events in the log.
    pub(crate) fn len(&self) -> HiveResult<u64> {
        self.last_seq()
    }

    /// Read all events for a given agent/stream in ascending order.
    pub(crate) fn read_stream(
        &self,
        agent_id: &AgentId,
        stream_id: &crate::event::StreamId,
    ) -> HiveResult<Vec<Event>> {
        match self.shards.get(agent_id) {
            Some(shard) => {
                let mut out = Vec::new();
                for event in shard.iter_events()? {
                    if &event.stream_id == stream_id {
                        out.push(event);
                    }
                }
                Ok(out)
            }
            None => Ok(Vec::new()),
        }
    }

    /// Query the current state of a projection.
    pub(crate) fn project<P: Projection>(&self) -> HiveResult<P::State> {
        if P::scope() == ProjectionScope::Global {
            self.global.project_local::<P>()
        } else {
            let mut whole = P::State::default();
            for entry in self.shards.iter() {
                let shard = entry.value();
                let part = shard.project_local::<P>()?;
                P::merge(&mut whole, &part);
            }
            Ok(whole)
        }
    }

    /// Returns the last sequence number applied to a projection.
    pub(crate) fn projection_checkpoint<P: Projection>(&self) -> HiveResult<u64> {
        if P::scope() == ProjectionScope::Global {
            self.global.projection_checkpoint::<P>()
        } else {
            let mut min: Option<u64> = None;
            for entry in self.shards.iter() {
                let shard = entry.value();
                let checkpoint = shard.projection_checkpoint::<P>()?;
                min = Some(min.map_or(checkpoint, |m| m.min(checkpoint)));
            }
            Ok(min.unwrap_or(0))
        }
    }

    /// Rebuild any projection that is behind the durable log. Called once at open.
    fn recover_projections(&self) -> HiveResult<()> {
        // Recover agent-scoped projections in each shard.
        for entry in self.shards.iter() {
            let shard = entry.value();
            Self::recover_shard_projections(shard, &self.registry)?;
        }

        // Recover global projections.
        let last_seq = self.last_seq()?;
        if last_seq > 0 {
            let from_seq = Self::min_global_checkpoint(&self.global, &self.registry)?;
            if from_seq < last_seq {
                self.global_rebuild(from_seq + 1, last_seq)?;
            }
        }

        Ok(())
    }

    fn recover_shard_projections(
        shard: &AgentShard,
        registry: &ProjectionRegistry,
    ) -> HiveResult<()> {
        let last_seq = shard_last_seq(shard)?;
        if last_seq == 0 {
            return Ok(());
        }
        let from_seq = Self::min_agent_checkpoint(shard, registry)?;
        if from_seq >= last_seq {
            return Ok(());
        }

        // Build an in-memory reader for the events in this shard.
        let events = shard.iter_events()?;
        let reader = InMemoryEventReader { events };

        let db = &shard.db;
        let txn = db.begin_write()?;
        let checkpoints_table = txn.open_table(PROJECTION_CHECKPOINTS)?;
        let state_table = txn.open_table(PROJECTION_STATE)?;
        let mut store = GlobalProjectionStore {
            state: state_table,
            checkpoints: checkpoints_table,
            current_seq: last_seq,
        };
        for handler in registry.agent_handlers() {
            handler.rebuild(from_seq + 1, last_seq, &reader, &mut store)?;
        }
        drop(store);
        txn.commit()?;
        Ok(())
    }

    fn min_agent_checkpoint(shard: &AgentShard, registry: &ProjectionRegistry) -> HiveResult<u64> {
        // We need to read the checkpoint table directly because AgentShard only
        // exposes per-projection checkpoints. Open a read transaction.
        let txn = shard.db.begin_read()?;
        let table = txn.open_table(PROJECTION_CHECKPOINTS)?;
        let mut min_checkpoint: Option<u64> = None;
        for handler in registry.agent_handlers() {
            let checkpoint = table.get(handler.name())?.map(|g| g.value()).unwrap_or(0);
            min_checkpoint = Some(min_checkpoint.map_or(checkpoint, |m| m.min(checkpoint)));
        }
        Ok(min_checkpoint.unwrap_or(0))
    }

    fn min_global_checkpoint(
        global: &AgentShard,
        registry: &ProjectionRegistry,
    ) -> HiveResult<u64> {
        let txn = global.db.begin_read()?;
        let table = txn.open_table(PROJECTION_CHECKPOINTS)?;
        let mut min_checkpoint: Option<u64> = None;
        for handler in registry.global_handlers() {
            let checkpoint = table.get(handler.name())?.map(|g| g.value()).unwrap_or(0);
            min_checkpoint = Some(min_checkpoint.map_or(checkpoint, |m| m.min(checkpoint)));
        }
        Ok(min_checkpoint.unwrap_or(0))
    }

    fn global_rebuild(&self, from_seq: u64, to_seq: u64) -> HiveResult<()> {
        let reader = ShardedEventReader { log: self };
        let db = &self.global.db;
        let txn = db.begin_write()?;
        let checkpoints_table = txn.open_table(PROJECTION_CHECKPOINTS)?;
        let state_table = txn.open_table(PROJECTION_STATE)?;
        let mut store = GlobalProjectionStore {
            state: state_table,
            checkpoints: checkpoints_table,
            current_seq: to_seq,
        };
        for handler in self.registry.global_handlers() {
            handler.rebuild(from_seq, to_seq, &reader, &mut store)?;
        }
        drop(store);
        txn.commit()?;
        Ok(())
    }

    /// Wipe all materialized projection state and rebuild it from the log.
    pub(crate) fn wipe_projections_and_rebuild(&self) -> HiveResult<()> {
        for entry in self.shards.iter() {
            let shard = entry.value();
            shard.wipe_and_rebuild_local(&self.registry)?;
        }

        let last_seq = self.last_seq()?;
        if last_seq > 0 {
            self.global_wipe_and_rebuild(1, last_seq)?;
        }
        Ok(())
    }

    fn global_wipe_and_rebuild(&self, from_seq: u64, to_seq: u64) -> HiveResult<()> {
        // Clear global projection tables.
        {
            let txn = self.global.db.begin_write()?;
            let checkpoint_keys: Vec<_> = {
                let table = txn.open_table(PROJECTION_CHECKPOINTS)?;
                table
                    .iter()?
                    .map(|item| item.unwrap().0.value().to_string())
                    .collect()
            };
            {
                let mut table = txn.open_table(PROJECTION_CHECKPOINTS)?;
                for key in checkpoint_keys {
                    table.remove(key.as_str())?;
                }
            }
            let state_keys: Vec<_> = {
                let table = txn.open_table(PROJECTION_STATE)?;
                table
                    .iter()?
                    .map(|item| item.unwrap().0.value().to_string())
                    .collect()
            };
            {
                let mut table = txn.open_table(PROJECTION_STATE)?;
                for key in state_keys {
                    table.remove(key.as_str())?;
                }
            }
            txn.commit()?;
        }
        self.global_rebuild(from_seq, to_seq)
    }
}

fn affects_global_projections(kind: &crate::event::EventKind) -> bool {
    matches!(
        kind,
        crate::event::EventKind::ConsentGranted { .. }
            | crate::event::EventKind::ConsentRevoked { .. }
            | crate::event::EventKind::IntentLogged { .. }
    )
}

fn shard_last_seq(shard: &AgentShard) -> HiveResult<u64> {
    let txn = shard.db.begin_read()?;
    let table = txn.open_table(EVENTS_TABLE)?;
    match table.last()? {
        Some((key, _)) => Ok(key.value()),
        None => Ok(0),
    }
}

struct GlobalProjectionStore<'txn> {
    state: redb::Table<'txn, &'static str, Vec<u8>>,
    checkpoints: redb::Table<'txn, &'static str, u64>,
    current_seq: u64,
}

impl<'txn> ProjectionStore for GlobalProjectionStore<'txn> {
    fn load_state(&self, name: &str) -> HiveResult<Option<Vec<u8>>> {
        match self.state.get(name)? {
            Some(access) => Ok(Some(access.value().to_vec())),
            None => Ok(None),
        }
    }

    fn save_state(&mut self, name: &str, bytes: &[u8], checkpoint: u64) -> HiveResult<()> {
        self.state.insert(name, bytes.to_vec())?;
        self.checkpoints.insert(name, checkpoint)?;
        Ok(())
    }

    fn current_seq(&self) -> u64 {
        self.current_seq
    }
}

struct ShardedEventReader<'a> {
    log: &'a EventLog,
}

impl EventLogInternal for ShardedEventReader<'_> {
    fn read_event(&self, seq: u64) -> HiveResult<Option<Event>> {
        self.log.read(seq).map(Some).or_else(|e| match e {
            HiveError::NotFound(_) => Ok(None),
            other => Err(other),
        })
    }
}

struct InMemoryEventReader {
    events: Vec<Event>,
}

impl EventLogInternal for InMemoryEventReader {
    fn read_event(&self, seq: u64) -> HiveResult<Option<Event>> {
        Ok(self.events.iter().find(|e| e.seq == seq).cloned())
    }
}
