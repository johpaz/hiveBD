use crate::clock::Clock;
use crate::error::{HiveError, HiveResult};
use crate::event::{AgentId, Event, EventInput};
use crate::projection::{
    EventLogInternal, Projection, ProjectionRegistry, ProjectionScope, ProjectionStore,
};
use crate::shard::AgentShard;
use dashmap::{DashMap, DashSet};
use redb::{ReadableTable, TableDefinition};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const SHARDS_DIR: &str = "shards";
const GLOBAL_SHARD: &str = "_global.redb";

/// Cuántos shards se mantienen abiertos a la vez. Cada shard es una
/// `redb::Database`, o sea un descriptor y una región mmap: una base compartida
/// por muchos enjambres acumula miles y se come el `ulimit -n` del proceso.
const DEFAULT_MAX_OPEN_SHARDS: usize = 256;

/// Clave en la tabla `meta` del shard global: marca si la última sesión cerró
/// ordenadamente. Si no, `next_seq` puede estar atrasado —los eventos no
/// globales no lo persisten, justo para no serializar todas las escrituras
/// contra el shard global— y hay que redescubrirlo escaneando.
const CLEAN_SHUTDOWN_KEY: &str = "clean_shutdown";

const EVENTS_TABLE: TableDefinition<u64, Vec<u8>> = TableDefinition::new("events");
const PROJECTION_CHECKPOINTS: TableDefinition<&str, u64> =
    TableDefinition::new("projection_checkpoints");
const PROJECTION_STATE: TableDefinition<&str, Vec<u8>> = TableDefinition::new("projection_state");
const META_TABLE: TableDefinition<&str, u64> = TableDefinition::new("meta");

/// Internal sharded event-log implementation on top of `redb`.
pub(crate) struct EventLog {
    base: PathBuf,
    /// Caché acotada de shards ABIERTOS, no el censo de los que existen.
    shards: DashMap<AgentId, Arc<AgentShard>>,
    /// Orden de apertura, para desalojar el más antiguo cuando se llena.
    open_order: Mutex<VecDeque<AgentId>>,
    max_open_shards: usize,
    /// Censo de agentes con shard en disco. Se llena al abrir leyendo sólo los
    /// NOMBRES de fichero del directorio —sin abrir ninguna base ni leer un
    /// solo evento— y crece cuando nace un agente nuevo.
    known_agents: DashSet<AgentId>,
    /// Shards cuyas proyecciones ya se recuperaron en esta sesión.
    recovered: DashSet<AgentId>,
    global: AgentShard,
    registry: ProjectionRegistry,
    clock: Arc<dyn Clock>,
    next_seq: AtomicU64,
    /// Índice seq → agente para `read(seq)`. Se puebla sobre la marcha (al
    /// escribir, y al escanear en una recuperación sucia): ya no se reconstruye
    /// leyendo el log entero en cada apertura.
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
            open_order: Mutex::new(VecDeque::new()),
            max_open_shards: DEFAULT_MAX_OPEN_SHARDS,
            known_agents: DashSet::new(),
            recovered: DashSet::new(),
            global,
            registry,
            clock,
            next_seq: AtomicU64::new(1),
            seq_to_agent: DashMap::new(),
        };

        // Abrir la base ya no cuesta O(eventos totales): se listan los nombres
        // de los shards y se lee el contador persistido. Los shards se abren y
        // se recuperan uno a uno, la primera vez que alguien los toca.
        log.census_shards()?;
        log.restore_next_seq()?;
        log.mark_dirty()?;
        log.recover_global_projections()?;
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

    /// Abre el shard de un agente, creándolo si hace falta.
    ///
    /// La entrada del `DashMap` se toma antes de tocar el disco: sin eso, dos
    /// hilos podrían intentar abrir el mismo fichero a la vez y el segundo
    /// fallaría, porque redb toma un lock exclusivo por base.
    fn get_or_create_shard(&self, agent_id: &AgentId) -> HiveResult<Arc<AgentShard>> {
        use dashmap::mapref::entry::Entry;

        let (shard, recien_abierto) = match self.shards.entry(agent_id.clone()) {
            Entry::Occupied(entry) => (Arc::clone(entry.get()), false),
            Entry::Vacant(entry) => {
                let shard = Arc::new(AgentShard::open(self.shard_path(agent_id))?);
                entry.insert(Arc::clone(&shard));
                (shard, true)
            }
        };

        if recien_abierto {
            self.known_agents.insert(agent_id.clone());
            // Recuperar las proyecciones del shard aquí, y no al abrir la base,
            // es lo que convierte el arranque en O(1) en vez de O(shards).
            if self.recovered.insert(agent_id.clone()) {
                Self::recover_shard_projections(&shard, &self.registry)?;
            }
            self.remember_open(agent_id);
        }

        Ok(shard)
    }

    /// Registra el shard como abierto y desaloja los más antiguos si hace falta.
    ///
    /// Sólo se desaloja un shard que nadie más esté usando (`strong_count == 1`,
    /// comprobado bajo el lock del mapa): reabrir un fichero cuyo handle sigue
    /// vivo fallaría por el lock exclusivo de redb. Un shard "en uso" se salta,
    /// no se fuerza — la caché puede rebasar el tope un rato, que es preferible
    /// a romper una escritura en curso.
    fn remember_open(&self, agent_id: &AgentId) {
        let mut order = self.open_order.lock().unwrap();
        order.push_back(agent_id.clone());

        // Los intentos se acotan al tamaño de la cola: si todos los shards
        // están en uso no hay nada que desalojar, y rebasar el tope un rato es
        // preferible a girar en vano o a cerrar algo que alguien está usando.
        let mut intentos = order.len();
        while order.len() > self.max_open_shards && intentos > 0 {
            intentos -= 1;
            let Some(candidato) = order.pop_front() else {
                break;
            };

            // El recién abierto nunca es candidato: desalojarlo aquí obligaría
            // a reabrirlo acto seguido.
            if &candidato == agent_id {
                order.push_back(candidato);
                continue;
            }

            let desalojado = self
                .shards
                .remove_if(&candidato, |_, shard| Arc::strong_count(shard) == 1)
                .is_some();

            // Si no se pudo desalojar porque sigue en uso, vuelve a la cola
            // para reintentarlo luego. Si ya no está en el mapa, se olvida:
            // reencolarlo sería seguir la pista de algo que ya no existe.
            if !desalojado && self.shards.contains_key(&candidato) {
                order.push_back(candidato);
            }
        }
    }

    /// Todos los agentes con shard, estén abiertos o no.
    fn all_agents(&self) -> Vec<AgentId> {
        self.known_agents.iter().map(|e| e.key().clone()).collect()
    }

    /// Shard de un agente para LEER. Devuelve `None` si ese agente no tiene
    /// shard: abrirlo lo crearía —`AgentShard::open` hace `Database::create`—
    /// y una consulta no debe dejar ficheros detrás.
    fn shard_for_read(&self, agent_id: &AgentId) -> HiveResult<Option<Arc<AgentShard>>> {
        if !self.known_agents.contains(agent_id) {
            return Ok(None);
        }
        self.get_or_create_shard(agent_id).map(Some)
    }

    /// Censo de shards: sólo nombres de fichero. No abre ninguna base ni lee un
    /// solo evento — es lo que hace que abrir cueste lo mismo con 10 shards que
    /// con 10 000.
    fn census_shards(&self) -> HiveResult<()> {
        for entry in std::fs::read_dir(self.shards_dir())? {
            let entry = entry?;
            let path = entry.path();
            let name = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
            let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");
            if ext != "redb" || name == "_global" {
                continue;
            }
            self.known_agents.insert(AgentId(name.to_string()));
        }
        Ok(())
    }

    /// Restaura `next_seq`.
    ///
    /// Tras un cierre ordenado basta con la meta del shard global. Si la sesión
    /// anterior se cayó, el contador puede estar atrasado —los eventos no
    /// globales no lo persisten, para no serializar todas las escrituras contra
    /// el shard global— y hay que redescubrirlo escaneando.
    ///
    /// Una base creada por una versión anterior no tiene la marca de cierre
    /// limpio, así que paga el escaneo una vez y ya queda al día.
    fn restore_next_seq(&self) -> HiveResult<()> {
        // El contador sólo es de fiar si EXISTE y además la sesión anterior
        // cerró ordenadamente. Que falte es motivo de escaneo por sí solo: es
        // el caso de una base escrita por una versión que aún no lo persistía.
        let next = match (self.read_stored_next_seq()?, self.read_clean_shutdown()?) {
            (Some(stored), true) => stored,
            (stored, _) => stored.unwrap_or(1).max(self.rescan_max_seq()? + 1),
        };

        self.next_seq.store(next, Ordering::SeqCst);
        self.write_stored_next_seq(next)?;
        Ok(())
    }

    /// Camino de recuperación: recorre todos los shards para redescubrir el seq
    /// más alto y rellenar el índice seq → agente. Caro y excepcional a
    /// propósito — sólo tras un cierre sucio.
    fn rescan_max_seq(&self) -> HiveResult<u64> {
        let mut max_seq = 0u64;
        for agent_id in self.all_agents() {
            let shard = self.get_or_create_shard(&agent_id)?;
            for event in shard.iter_events()? {
                self.seq_to_agent.insert(event.seq, agent_id.clone());
                max_seq = max_seq.max(event.seq);
            }
        }
        Ok(max_seq)
    }

    fn read_clean_shutdown(&self) -> HiveResult<bool> {
        let txn = self.global.db.begin_read()?;
        let table = txn.open_table(META_TABLE)?;
        Ok(table
            .get(CLEAN_SHUTDOWN_KEY)?
            .map(|v| v.value() == 1)
            .unwrap_or(false))
    }

    /// Marca la base como abierta: si el proceso muere ahora, la próxima
    /// apertura sabrá que tiene que escanear.
    fn mark_dirty(&self) -> HiveResult<()> {
        let txn = self.global.db.begin_write()?;
        {
            let mut table = txn.open_table(META_TABLE)?;
            table.insert(CLEAN_SHUTDOWN_KEY, 0u64)?;
        }
        txn.commit()?;
        Ok(())
    }

    fn read_stored_next_seq(&self) -> HiveResult<Option<u64>> {
        let txn = self.global.db.begin_read()?;
        let table = txn.open_table(META_TABLE)?;
        Ok(table.get("next_seq")?.map(|v| v.value()))
    }

    /// Persiste el contador y marca el cierre como ordenado, en una sola
    /// transacción: media verdad —contador nuevo con la marca vieja, o al
    /// revés— dejaría la siguiente apertura escaneando de más o, peor,
    /// confiando en un contador atrasado.
    pub(crate) fn flush_next_seq(&self) -> HiveResult<()> {
        let next = self.next_seq.load(Ordering::SeqCst);
        let txn = self.global.db.begin_write()?;
        {
            let mut table = txn.open_table(META_TABLE)?;
            table.insert("next_seq", next)?;
            table.insert(CLEAN_SHUTDOWN_KEY, 1u64)?;
        }
        txn.commit()?;
        Ok(())
    }

    fn write_stored_next_seq(&self, value: u64) -> HiveResult<()> {
        let txn = self.global.db.begin_write()?;
        {
            let mut table = txn.open_table(META_TABLE)?;
            table.insert("next_seq", value)?;
        }
        txn.commit()?;
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
            // Persist the next free sequence number together with the global
            // write so reopening does not have to rediscover it by scanning
            // every shard. Non-global events skip this write to preserve the
            // per-agent write isolation that sharding provides.
            self.write_stored_next_seq(seq + 1)?;
        }

        self.seq_to_agent.insert(seq, input.agent_id);
        Ok(event)
    }

    /// Read a single event by sequence number.
    pub(crate) fn read(&self, seq: u64) -> HiveResult<Event> {
        let agent_id = match self.seq_to_agent.get(&seq) {
            Some(entry) => entry.value().clone(),
            None => return Err(HiveError::NotFound(format!("event seq={seq}"))),
        };
        match self.shard_for_read(&agent_id)? {
            Some(shard) => match shard.read_event(seq)? {
                Some(event) => Ok(event),
                None => Err(HiveError::NotFound(format!("event seq={seq}"))),
            },
            None => Err(HiveError::NotFound(format!("shard for agent {agent_id}"))),
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
        match self.shard_for_read(agent_id)? {
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

    /// Lee un stream restringido a un conjunto de agentes, ordenado por seq.
    ///
    /// Esta es la variante que debe usar cualquier consumidor multi-inquilino:
    /// recorre `agents.len()` shards en vez de todos los de la base, así que ni
    /// devuelve eventos de otros enjambres ni paga su coste.
    pub(crate) fn read_stream_for_agents(
        &self,
        agents: &[AgentId],
        stream_id: &crate::event::StreamId,
    ) -> HiveResult<Vec<Event>> {
        let mut out = Vec::new();
        for agent_id in agents {
            let Some(shard) = self.shard_for_read(agent_id)? else {
                continue;
            };
            for event in shard.iter_events()? {
                if &event.stream_id == stream_id {
                    out.push(event);
                }
            }
        }
        out.sort_by_key(|e| e.seq);
        Ok(out)
    }

    /// Read all events for a stream across every agent shard, sorted by seq.
    ///
    /// Recorre el log ENTERO de la base. En una base de un solo dueño es lo
    /// correcto; si la base aloja varios inquilinos hay que usar
    /// [`EventLog::read_stream_for_agents`], que además evita mezclar eventos
    /// de enjambres distintos.
    pub(crate) fn read_stream_all_agents(
        &self,
        stream_id: &crate::event::StreamId,
    ) -> HiveResult<Vec<Event>> {
        let agents = self.all_agents();
        self.read_stream_for_agents(&agents, stream_id)
    }

    /// Estado de una proyección, restringido a un conjunto de agentes.
    ///
    /// Para proyecciones con scope `Agent` —el scope por defecto— el estado
    /// vive repartido por shard y hay que mezclarlo. Mezclarlo TODO en una base
    /// compartida significa sumar las estadísticas de otros inquilinos, así que
    /// el consumidor multi-inquilino tiene que decir de quién pregunta.
    pub(crate) fn project_for_agents<P: Projection>(
        &self,
        agents: &[AgentId],
    ) -> HiveResult<P::State> {
        if P::scope() == ProjectionScope::Global {
            return self.global.project_local::<P>();
        }
        let mut whole = P::State::default();
        for agent_id in agents {
            let Some(shard) = self.shard_for_read(agent_id)? else {
                continue;
            };
            let part = shard.project_local::<P>()?;
            P::merge(&mut whole, &part);
        }
        Ok(whole)
    }

    /// Query the current state of a projection.
    ///
    /// Mezcla los shards de TODA la base. Ver
    /// [`EventLog::project_for_agents`] para el caso multi-inquilino.
    pub(crate) fn project<P: Projection>(&self) -> HiveResult<P::State> {
        if P::scope() == ProjectionScope::Global {
            return self.global.project_local::<P>();
        }
        let agents = self.all_agents();
        self.project_for_agents::<P>(&agents)
    }

    /// Returns the last sequence number applied to a projection.
    pub(crate) fn projection_checkpoint<P: Projection>(&self) -> HiveResult<u64> {
        if P::scope() == ProjectionScope::Global {
            self.global.projection_checkpoint::<P>()
        } else {
            let mut min: Option<u64> = None;
            for agent_id in self.all_agents() {
                let Some(shard) = self.shard_for_read(&agent_id)? else {
                    continue;
                };
                let checkpoint = shard.projection_checkpoint::<P>()?;
                min = Some(min.map_or(checkpoint, |m| m.min(checkpoint)));
            }
            Ok(min.unwrap_or(0))
        }
    }

    /// Reconstruye las proyecciones GLOBALES que estén por detrás del log.
    ///
    /// Las de scope `Agent` ya no se recuperan aquí: eso obligaba a abrir todos
    /// los shards al arrancar. Cada shard recupera las suyas la primera vez que
    /// se abre, dentro de `get_or_create_shard`.
    fn recover_global_projections(&self) -> HiveResult<()> {
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
        // Una reconstrucción total sí tiene que tocar todos los shards, no sólo
        // los que estén abiertos: es una operación explícita y rara, no el
        // camino caliente.
        for agent_id in self.all_agents() {
            let shard = self.get_or_create_shard(&agent_id)?;
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

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use crate::clock::SystemClock;
    use crate::event::{EventKind, StreamId};

    /// Un `EventLog` con un tope de shards abiertos deliberadamente diminuto,
    /// para que la evicción entre en juego con pocos agentes.
    fn log_con_tope(dir: &std::path::Path, tope: usize) -> EventLog {
        let mut log = EventLog::open(dir, ProjectionRegistry::empty(), Arc::new(SystemClock))
            .expect("abrir log");
        log.max_open_shards = tope;
        log
    }

    fn hecho(agente: &str) -> EventInput {
        EventInput::new(agente, StreamId::from("s"), EventKind::Fact)
    }

    /// El motivo de existir de la caché: una base compartida por muchos
    /// enjambres acumula miles de shards, y cada uno es un descriptor y una
    /// región mmap. Sin tope, abrir la base agota el `ulimit -n` del proceso.
    #[test]
    fn la_cache_de_shards_no_crece_sin_limite() {
        let dir = tempfile::tempdir().unwrap();
        let log = log_con_tope(dir.path(), 4);

        for i in 0..20 {
            log.append(hecho(&format!("agente-{i}"))).unwrap();
        }

        assert!(
            log.shards.len() <= 4,
            "quedaron {} shards abiertos con un tope de 4",
            log.shards.len()
        );
        // Desalojar es olvidar un handle, no perder datos: el censo sigue
        // conociendo a los 20 agentes.
        assert_eq!(log.known_agents.len(), 20);
    }

    /// Desalojar un shard no puede costar datos: al volver a tocarlo se reabre
    /// y sus eventos siguen ahí.
    #[test]
    fn un_shard_desalojado_se_reabre_con_sus_datos() {
        let dir = tempfile::tempdir().unwrap();
        let log = log_con_tope(dir.path(), 2);

        let seq = log.append(hecho("agente-0")).unwrap().seq;
        for i in 1..10 {
            log.append(hecho(&format!("agente-{i}"))).unwrap();
        }

        let recuperados = log
            .read_stream_for_agents(&[AgentId::from("agente-0")], &StreamId::from("s"))
            .unwrap();
        assert_eq!(recuperados.len(), 1);
        assert_eq!(recuperados[0].seq, seq);
    }

    /// Un shard con una escritura viva no se puede cerrar: reabrirlo fallaría
    /// por el lock exclusivo de redb. Ante la duda, la caché rebasa el tope.
    #[test]
    fn no_se_desaloja_un_shard_en_uso() {
        let dir = tempfile::tempdir().unwrap();
        let log = log_con_tope(dir.path(), 1);

        log.append(hecho("ocupado")).unwrap();
        let retenido = log.get_or_create_shard(&AgentId::from("ocupado")).unwrap();

        for i in 0..5 {
            log.append(hecho(&format!("otro-{i}"))).unwrap();
        }

        assert!(
            log.shards.contains_key(&AgentId::from("ocupado")),
            "un shard con un Arc vivo fuera del mapa no debe desalojarse"
        );
        drop(retenido);
    }
}
