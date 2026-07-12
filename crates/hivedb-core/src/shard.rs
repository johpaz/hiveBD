use crate::error::HiveResult;
use crate::event::Event;
use crate::projection::{
    DynProjection, EventLogInternal, Projection, ProjectionRegistry, ProjectionStore,
};
use redb::{Database, ReadableTable, TableDefinition};
use std::path::Path;
use std::sync::Mutex;

const EVENTS_TABLE: TableDefinition<u64, Vec<u8>> = TableDefinition::new("events");
const PROJECTION_CHECKPOINTS: TableDefinition<&str, u64> =
    TableDefinition::new("projection_checkpoints");
const PROJECTION_STATE: TableDefinition<&str, Vec<u8>> = TableDefinition::new("projection_state");
const META_TABLE: TableDefinition<&str, u64> = TableDefinition::new("meta");

/// A single-agent shard: one `redb` database plus a mutex that serializes
/// writes for that agent.
pub(crate) struct AgentShard {
    pub(crate) db: Database,
    /// Serializes all writes to this shard so same-agent events keep causal
    /// order and monotonic seq.
    write_lock: Mutex<()>,
}

impl AgentShard {
    pub(crate) fn open<P: AsRef<Path>>(path: P) -> HiveResult<Self> {
        let db = Database::create(path)?;
        {
            let txn = db.begin_write()?;
            txn.open_table(EVENTS_TABLE)?;
            txn.open_table(PROJECTION_CHECKPOINTS)?;
            txn.open_table(PROJECTION_STATE)?;
            txn.open_table(META_TABLE)?;
            txn.commit()?;
        }
        Ok(Self {
            db,
            write_lock: Mutex::new(()),
        })
    }

    /// Acquire this shard's write lock. Callers of [`AgentShard::append_event`]
    /// must hold this guard so that seq assignment and projection application
    /// happen in the same critical section.
    pub(crate) fn lock_writes(&self) -> std::sync::MutexGuard<'_, ()> {
        self.write_lock.lock().unwrap()
    }

    /// Serialize and store the event, then apply the given projections
    /// atomically inside the same `redb` transaction.
    ///
    /// The caller must hold the guard returned by [`AgentShard::lock_writes`].
    pub(crate) fn append_event(
        &self,
        event: &Event,
        handlers: &mut dyn Iterator<Item = &Box<dyn DynProjection>>,
    ) -> HiveResult<()> {
        let txn = self.db.begin_write()?;

        // 1. Persist the raw event.
        {
            let mut events_table = txn.open_table(EVENTS_TABLE)?;
            let bytes = bincode::serialize(event)?;
            events_table.insert(event.seq, bytes)?;
        }

        // 2. Apply projections.
        {
            let checkpoints_table = txn.open_table(PROJECTION_CHECKPOINTS)?;
            let state_table = txn.open_table(PROJECTION_STATE)?;
            let mut store = ShardProjectionStore {
                state: state_table,
                checkpoints: checkpoints_table,
                current_seq: event.seq,
            };
            for handler in handlers {
                handler.apply_event(event, &mut store)?;
            }
        }

        txn.commit()?;
        Ok(())
    }

    pub(crate) fn read_event(&self, seq: u64) -> HiveResult<Option<Event>> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(EVENTS_TABLE)?;
        match table.get(seq)? {
            Some(access) => {
                let bytes = access.value();
                Ok(Some(bincode::deserialize(&bytes)?))
            }
            None => Ok(None),
        }
    }

    /// Iterate all events in this shard in ascending seq order.
    pub(crate) fn iter_events(&self) -> HiveResult<Vec<Event>> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(EVENTS_TABLE)?;
        let mut out = Vec::new();
        for item in table.iter()? {
            let (_key, access) = item?;
            out.push(bincode::deserialize(&access.value())?);
        }
        Ok(out)
    }

    /// Read the persisted state of a concrete projection from this shard.
    pub(crate) fn project_local<P: Projection>(&self) -> HiveResult<P::State> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(PROJECTION_STATE)?;
        match table.get(P::name())? {
            Some(access) => {
                let bytes = access.value();
                Ok(bincode::deserialize(&bytes)?)
            }
            None => Ok(P::State::default()),
        }
    }

    /// Returns the checkpoint of a concrete projection in this shard.
    pub(crate) fn projection_checkpoint<P: Projection>(&self) -> HiveResult<u64> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(PROJECTION_CHECKPOINTS)?;
        Ok(table.get(P::name())?.map(|g| g.value()).unwrap_or(0))
    }

    /// Wipe local projection state and rebuild it from this shard's events.
    pub(crate) fn wipe_and_rebuild_local(&self, registry: &ProjectionRegistry) -> HiveResult<()> {
        let _guard = self.write_lock.lock().unwrap();
        let txn = self.db.begin_write()?;

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

        let events_table = txn.open_table(EVENTS_TABLE)?;
        let last_seq = match events_table.last()? {
            Some((key, _)) => key.value(),
            None => 0,
        };

        if last_seq > 0 {
            let checkpoints_table = txn.open_table(PROJECTION_CHECKPOINTS)?;
            let state_table = txn.open_table(PROJECTION_STATE)?;
            let reader = ShardEventReader {
                events: &events_table,
            };
            let mut store = ShardProjectionStore {
                state: state_table,
                checkpoints: checkpoints_table,
                current_seq: 0,
            };
            for handler in registry.agent_handlers() {
                handler.rebuild(1, last_seq, &reader, &mut store)?;
            }
            drop(store);
        }

        drop(events_table);
        txn.commit()?;
        Ok(())
    }
}

struct ShardProjectionStore<'txn> {
    state: redb::Table<'txn, &'static str, Vec<u8>>,
    checkpoints: redb::Table<'txn, &'static str, u64>,
    current_seq: u64,
}

impl<'txn> ProjectionStore for ShardProjectionStore<'txn> {
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

struct ShardEventReader<'txn> {
    events: &'txn redb::Table<'txn, u64, Vec<u8>>,
}

impl EventLogInternal for ShardEventReader<'_> {
    fn read_event(&self, seq: u64) -> HiveResult<Option<Event>> {
        match self.events.get(seq)? {
            Some(access) => {
                let bytes = access.value();
                Ok(Some(bincode::deserialize(&bytes)?))
            }
            None => Ok(None),
        }
    }
}
