use crate::error::HiveResult;
use crate::event::Event;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::fmt::Debug;

/// Scope of a projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjectionScope {
    /// Projection state is scoped to a single agent shard.
    Agent,
    /// Projection state spans all agents and lives in the global shard.
    Global,
}

/// Storage abstraction used by projections.
///
/// Implementations may be backed by a single `redb` shard, the global shard, or
/// an in-memory structure for tests.
pub(crate) trait ProjectionStore {
    /// Load the persisted bytes for a projection, if any.
    fn load_state(&self, name: &str) -> HiveResult<Option<Vec<u8>>>;

    /// Persist projection state and advance its checkpoint.
    fn save_state(&mut self, name: &str, bytes: &[u8], checkpoint: u64) -> HiveResult<()>;

    /// Sequence number of the event currently being applied.
    fn current_seq(&self) -> u64;
}

/// Minimal read-only interface the projection layer needs from the log.
pub(crate) trait EventLogInternal {
    fn read_event(&self, seq: u64) -> HiveResult<Option<Event>>;
}

/// A deterministic fold over the event log.
///
/// Implementations must be pure functions: given the same sequence of events,
/// they must always produce the same state.
pub trait Projection: Send + Sync + 'static {
    /// The accumulated state type. Must be serializable so it can be persisted
    /// alongside the log.
    type State: Serialize + DeserializeOwned + PartialEq + Debug + Default + Clone;

    /// Unique name used as registry key and persistence prefix.
    fn name() -> &'static str
    where
        Self: Sized;

    /// Scope of the projection.
    fn scope() -> ProjectionScope
    where
        Self: Sized,
    {
        ProjectionScope::Agent
    }

    /// Apply one event to the state.
    fn apply(state: &mut Self::State, event: &Event);

    /// Merge a partial shard state into a whole.
    ///
    /// Default implementation simply overwrites. Projections whose state is a
    /// collection (e.g. maps) must override this to combine partial results
    /// from each agent shard.
    fn merge(whole: &mut Self::State, part: &Self::State) {
        *whole = part.clone();
    }
}

/// Extension trait that lets users query a projection through the public API.
pub trait ProjectionExt {
    type State;
    /// Returns the default/empty state for this projection.
    fn initial_state() -> Self::State;
}

impl<P: Projection> ProjectionExt for P {
    type State = P::State;
    fn initial_state() -> Self::State {
        P::State::default()
    }
}

/// Internal object-safe projection handler used by the event log to update
/// projections inside a `ProjectionStore`.
pub(crate) trait DynProjection: Send + Sync {
    fn name(&self) -> &'static str;

    fn scope(&self) -> ProjectionScope;

    fn apply_event(&self, event: &Event, store: &mut dyn ProjectionStore) -> HiveResult<()>;

    fn rebuild(
        &self,
        from_seq: u64,
        to_seq: u64,
        log: &dyn EventLogInternal,
        store: &mut dyn ProjectionStore,
    ) -> HiveResult<()>;
}

/// Wraps a concrete `Projection` into an object-safe handler.
pub(crate) struct ProjectionHandler<P: Projection> {
    _phantom: std::marker::PhantomData<P>,
}

impl<P: Projection> ProjectionHandler<P> {
    pub(crate) fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }

    fn load_state(&self, store: &dyn ProjectionStore) -> HiveResult<P::State> {
        match store.load_state(P::name())? {
            Some(bytes) => Ok(bincode::deserialize(&bytes)?),
            None => Ok(P::State::default()),
        }
    }
}

impl<P: Projection> DynProjection for ProjectionHandler<P> {
    fn name(&self) -> &'static str {
        P::name()
    }

    fn scope(&self) -> ProjectionScope {
        P::scope()
    }

    fn apply_event(&self, event: &Event, store: &mut dyn ProjectionStore) -> HiveResult<()> {
        let mut state = self.load_state(store)?;
        P::apply(&mut state, event);
        let bytes = bincode::serialize(&state)?;
        store.save_state(P::name(), &bytes, store.current_seq())
    }

    fn rebuild(
        &self,
        from_seq: u64,
        to_seq: u64,
        log: &dyn EventLogInternal,
        store: &mut dyn ProjectionStore,
    ) -> HiveResult<()> {
        // If this is a full rebuild (from 0), start from the default state.
        // Otherwise continue from the persisted state.
        let mut state = if from_seq == 0 {
            P::State::default()
        } else {
            self.load_state(store)?
        };

        for seq in from_seq..=to_seq {
            if let Some(event) = log.read_event(seq)? {
                P::apply(&mut state, &event);
            }
        }

        // Persist the rebuilt state and advance the checkpoint.
        let bytes = bincode::serialize(&state)?;
        store.save_state(P::name(), &bytes, to_seq)
    }
}

/// Registry of all projections known to a `HiveDB` instance.
pub(crate) struct ProjectionRegistry {
    projections: HashMap<&'static str, Box<dyn DynProjection>>,
}

impl ProjectionRegistry {
    pub(crate) fn empty() -> Self {
        Self {
            projections: HashMap::new(),
        }
    }

    pub(crate) fn register<P: Projection>(&mut self) {
        let handler: Box<dyn DynProjection> = Box::new(ProjectionHandler::<P>::new());
        self.projections.insert(P::name(), handler);
    }

    /// Returns handlers for agent-scoped projections.
    pub(crate) fn agent_handlers(&self) -> impl Iterator<Item = &Box<dyn DynProjection>> {
        self.projections
            .values()
            .filter(|h| h.scope() == ProjectionScope::Agent)
    }

    /// Returns handlers for global-scoped projections.
    pub(crate) fn global_handlers(&self) -> impl Iterator<Item = &Box<dyn DynProjection>> {
        self.projections
            .values()
            .filter(|h| h.scope() == ProjectionScope::Global)
    }
}
