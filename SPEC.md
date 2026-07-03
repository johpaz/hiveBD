# HiveDB — Especificación del Motor

> Motor de base de datos embebido, local-first, agent-native para el ecosistema Hive.
> **Lenguaje:** Rust 1.95 (edición 2024) · **Runtime consumidor:** Bun (Rust) vía napi-rs
> **Licencia objetivo:** Apache-2.0 · **Estado:** spec v0.1 (pre-implementación)

---

## 0. Principios de diseño (no negociables)

1. **Soberanía digital.** Cero dependencia de servicios externos. Sin daemon, sin red, sin cloud. Un binario, un directorio de datos.
2. **Event-log como fuente de verdad.** El estado es una *proyección* derivada de un log append-only inmutable. Nunca se muta el pasado.
3. **Embebido in-process.** Corre dentro del proceso Bun como librería nativa, igual que `bun:sqlite` envuelve SQLite. Sin RPC.
4. **Agent-native en el motor, no en la app.** Los primitivos del agente (tool-ledger, consent graph, triggers reactivos, tiers de memoria) viven en el motor, no se simulan arriba.
5. **`unsafe` minimizado y aislado.** Solo en fronteras mmap y FFI, encapsulado en módulos auditables. Rust idiomático en todo lo demás.
6. **Determinismo y replay.** Todo el estado debe poder reconstruirse reproduciendo el log desde cero. Auditoría e intent-logging nativos.

---

## 1. Arquitectura de capas

```
┌─────────────────────────────────────────────────────────┐
│  Capa TS (@johpaz/hive-db)  — API ergonómica para Bun   │
│  open(), append(), query(), subscribe(), project()      │
└───────────────────────────┬─────────────────────────────┘
                            │ napi-rs (C ABI)
┌───────────────────────────┴─────────────────────────────┐
│  Núcleo Rust (hivedb-core)                              │
│                                                         │
│  ┌────────────┐  ┌─────────────┐  ┌──────────────────┐ │
│  │ Event Log  │  │ Projections │  │ Reactive Engine  │ │
│  │ (append)   │→ │ (state)     │← │ (triggers/subs)  │ │
│  └─────┬──────┘  └──────┬──────┘  └────────┬─────────┘ │
│        │                │                  │            │
│  ┌─────┴────────────────┴──────────────────┴─────────┐ │
│  │              Memory Tiers                          │ │
│  │  Working (RAM/TTL) · Episodic (log) · Semantic     │ │
│  └────────────────────────────────────────────────────┘ │
│        │              │                  │              │
│  ┌─────┴──────┐ ┌─────┴──────┐  ┌────────┴─────────┐   │
│  │ redb (KV)  │ │ tantivy    │  │ hnsw_rs (vector) │   │
│  │ log+state  │ │ (BM25/FTS) │  │ (ANN/semantic)   │   │
│  └────────────┘ └────────────┘  └──────────────────┘   │
│  ┌──────────────────────────────────────────────────┐  │
│  │ Consent Graph (delegación / intent audit)        │  │
│  └──────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
```

### 1.1 Crates de almacenamiento elegidos

| Subsistema | Crate | Por qué |
|---|---|---|
| Log append-only + proyecciones (KV) | `redb` | Puro Rust, transaccional MVCC, mmap, sin C. ACID embebido. |
| Búsqueda full-text BM25 | `tantivy` | Lucene-equivalente en Rust. Da el FTS/BM25 nativo que en SQLite era FTS5. |
| Índice vectorial ANN | `hnsw_rs` | HNSW puro Rust. Sin servidor, sin Python. |
| Fusión de rankings | propio (RRF) | Reciprocal Rank Fusion sobre resultados BM25 + ANN. |
| Serialización de eventos | `rkyv` o `bincode` | Zero-copy / compacto. `rkyv` si se quiere mmap directo del payload. |

> **Nota:** `redb` es la columna vertebral. El log vive en una tabla append-only con claves monotónicas; las proyecciones en tablas separadas dentro de la misma transacción, garantizando que estado y log nunca divergen.

---

## 2. Modelo de datos

### 2.1 El Evento (primitivo base)

Todo lo que ocurre es un evento. Inmutable. Append-only.

```rust
struct Event {
    seq:        u64,          // secuencia global monotónica (asignada por el motor)
    agent_id:   AgentId,      // partición lógica de escritura
    stream_id:  StreamId,     // sub-stream dentro del agente (ej. una tarea)
    kind:       EventKind,    // tipo discriminado (ver 2.2)
    timestamp:  u64,          // epoch ms, asignado por el motor (no por el cliente)
    causation:  Option<u64>,  // seq del evento que causó este (cadena causal)
    correlation:Option<Uuid>, // agrupa eventos de una misma intención
    payload:    Bytes,        // cuerpo serializado, schema-flex (ver 2.3)
}
```

**Reglas duras:**
- `seq` es asignado por el motor, nunca por el cliente. Monotónico global.
- `timestamp` lo pone el motor. El cliente no controla el tiempo (evita replay corrupto).
- Un evento, una vez con `seq` asignado, es inmutable. No hay `UPDATE` ni `DELETE` sobre el log.
- La corrección de un hecho se modela como un *nuevo* evento que invalida al anterior (estilo `valid_at`/`invalid_at`), no como mutación.

### 2.2 Tipos de evento agent-native (en el motor)

```rust
enum EventKind {
    // Genéricos
    Fact,                 // un hecho/dato que el agente conoce
    StateTransition,      // cambio de estado de una tarea/agente

    // Tool ledger (primitivo nativo)
    ToolCall {            // invocación de herramienta
        tool: String,
        input_hash: Hash,
        latency_ms: u32,
        cost: Option<Cost>,
        outcome: ToolOutcome, // Ok / Err / Timeout
    },

    // Memoria
    MemoryWrite { tier: Tier, key: String },
    MemoryInvalidate { target_seq: u64 }, // invalida un hecho previo

    // Consentimiento / delegación (intent audit)
    ConsentGranted  { from: AgentId, to: AgentId, scope: Scope, expires: Option<u64> },
    ConsentRevoked  { grant_seq: u64 },
    IntentLogged    { actor: AgentId, intent: String, authorized_by: Option<u64> },
}
```

### 2.3 Payload schema-flex tipado en los bordes

- El **cuerpo** del payload es schema-flexible (documento/JSON o `rkyv`), porque el razonamiento del agente muta.
- Los **bordes** (`agent_id`, `stream_id`, `kind`, `timestamp`, `correlation`) son tipados e indexados.
- Filosofía: *Mongo en el centro, SQLite en los bordes.*

---

## 3. Proyecciones (el estado derivado)

Una proyección es una función pura `fold` sobre el log que produce estado consultable.

```rust
trait Projection {
    type State;
    fn apply(state: &mut Self::State, event: &Event);
    fn name() -> &'static str;
}
```

**Garantías:**
- Las proyecciones se actualizan en la **misma transacción `redb`** que el append del evento → estado y log nunca divergen.
- Cada proyección guarda un `checkpoint_seq`: el último `seq` aplicado. Permite reconstrucción incremental y replay parcial.
- Una proyección puede reconstruirse desde cero (`seq=0`) de forma determinista. **Test crítico** (ver TDD §4.1).

**Proyecciones base que el motor provee:**

| Proyección | Estado que materializa |
|---|---|
| `CurrentFacts` | hechos vigentes (no invalidados) por agente/stream |
| `TaskState` | estado actual de cada `stream_id` |
| `ToolLedger` | agregados de uso de herramientas (latencia, costo, error-rate) |
| `ConsentGraph` | grafo de delegaciones activas (ver §6) |

---

## 4. Tiers de memoria

Tres tiers físicamente distintos, cada uno optimizado para su patrón de acceso.

### 4.1 Working memory (RAM + TTL)
- KV en memoria, el *blackboard*. Escritura altísima frecuencia, lectura por múltiples workers.
- TTL por entrada. No toca disco salvo snapshot opcional.
- Equivalente al `agent_context` de la versión SQLite, pero como estructura del motor.
- Implementación: `DashMap` concurrente con expiración perezosa.

### 4.2 Episodic memory (log en disco)
- ES el event-log. Append-only, inmutable, en `redb`.
- Time-travel: "¿qué sabía el agente en el `seq` N?" → replay hasta N.

### 4.3 Semantic memory (búsqueda híbrida nativa)
- Vectores (`hnsw_rs`) + texto (`tantivy`) **en una sola query**, con fusión RRF en el motor.
- El cliente hace UNA llamada `query_hybrid(text, vector, k)` y recibe resultados fusionados y rankeados.
- Esto es lo que en SQLite obligaba a tener FTS5 por un lado y vectores por otro, unidos a mano en TS.

```rust
struct HybridQuery {
    text:   Option<String>,   // → tantivy BM25
    vector: Option<Vec<f32>>, // → hnsw_rs ANN
    filters: Vec<ScalarFilter>, // empujados al índice
    k: usize,
    fusion: Fusion,           // RRF (default) | WeightedSum
}
```

---

## 5. Concurrencia: muchos lectores, escritura particionada por agente

El cuello de SQLite (single writer global) se elimina particionando el log por `agent_id`.

- **Escritura:** cada `agent_id` escribe a su propio segmento de log sin contención con otros agentes. La asignación de `seq` global se serializa con un contador atómico ligero, NO con un lock de escritura global sobre todo el store.
- **Lectura:** ilimitada y concurrente sobre proyecciones (MVCC de `redb`).
- **Workers Bun:** cada worker abre un handle de lectura; las escrituras van por el canal del agente propietario.
- **Garantía:** dos agentes distintos nunca se bloquean mutuamente al escribir. Dos escrituras al *mismo* agente se serializan (orden causal preservado).

> Esto es multi-writer real sin el peso de MVCC completo de Postgres, porque los agentes raramente compiten por el mismo stream.

---

## 6. Consent Graph (delegación + intent audit)

Grafo embebido que responde: *¿quién autorizó qué, a qué agente, y sigue vigente?*

- Nodos: `AgentId`. Aristas: grants (`ConsentGranted`) con `scope` y `expires`.
- Derivado de eventos `ConsentGranted`/`ConsentRevoked` vía la proyección `ConsentGraph`.
- API: `can(agent, action, resource) -> Decision` resuelta por traversal del grafo vigente.
- Cada decisión de autorización emite un `IntentLogged` con `authorized_by` apuntando al `seq` del grant usado → cadena de auditoría completa y reproducible.

Esto materializa directamente la línea de *delegated consent / intent audit logs* del trabajo de gobernanza.

---

## 7. Reactive Engine (triggers / suscripciones)

El blackboard pattern como mecanismo del motor, no como polling del cliente.

```rust
// Un agente se suscribe a un patrón de eventos; el motor lo despierta.
fn subscribe(pattern: EventPattern) -> Subscription; // stream async de eventos

struct EventPattern {
    agent_id: Option<AgentId>,
    kind:     Option<EventKindTag>,
    stream_id:Option<StreamId>,
    predicate:Option<Predicate>, // filtro sobre payload
}
```

- Al hacer `append`, el motor evalúa suscripciones activas y empuja a los matches.
- Entrega *at-least-once* con `seq` para deduplicación del lado del consumidor.
- Sustituye el polling sobre `agent_context` por un push reactivo.

---

## 8. Durabilidad y crash-safety

- `redb` da ACID transaccional. El append del evento + actualización de proyecciones + checkpoints ocurren en UNA transacción.
- Política de fsync configurable: `Always` (máxima seguridad), `Periodic(ms)`, `OnCheckpoint`.
- Recuperación tras crash: al abrir, el motor verifica el último `seq` durable y reconstruye proyecciones que estén por detrás de su checkpoint.
- **Invariante:** tras cualquier crash, el estado materializado == replay del log durable. (Test §4.4.)

---

## 9. API pública (capa TS sobre napi-rs)

```typescript
import { HiveDB } from "@johpaz/hive-db";

const db = await HiveDB.open("./hive-data");

// Append (única vía de escritura)
const seq = await db.append({
  agentId: "ProductManager",
  streamId: task.id,
  kind: "Fact",
  payload: { ... },
});

// Proyección / estado actual
const state = await db.project("TaskState", task.id);

// Búsqueda híbrida nativa (texto + vector + filtros, fusión RRF)
const hits = await db.queryHybrid({
  text: "error de compilación en el módulo de pagos",
  vector: embedding,
  filters: [{ field: "agentId", eq: "BackendEngineer" }],
  k: 10,
});

// Suscripción reactiva (push, no polling)
for await (const ev of db.subscribe({ kind: "ToolCall" })) {
  // despertado por el motor
}

// Consentimiento
const decision = await db.can("FrontendEngineer", "deploy", "prod");
```

---

## 10. Layout en disco (soberanía: todo local, un directorio)

```
./hive-data/
├── log.redb           # event-log append-only + proyecciones (redb)
├── fts/               # índice tantivy (BM25)
├── vec/               # índice hnsw_rs (ANN)
├── snapshots/         # snapshots opcionales de working memory
└── MANIFEST           # versión de esquema, checkpoints, metadata
```

Backup = copiar el directorio. Sync futuro (Hive Cloud) = protocolo CRDT-friendly sobre el log, eventual-consistency sin reescribir el modelo.

---

## 11. Estructura del repositorio (monorepo)

```
hivedb/
├── crates/
│   ├── hivedb-core/        # núcleo Rust: log, proyecciones, tiers, reactive
│   │   ├── src/
│   │   │   ├── event.rs
│   │   │   ├── log.rs
│   │   │   ├── projection.rs
│   │   │   ├── memory/
│   │   │   │   ├── working.rs
│   │   │   │   ├── episodic.rs
│   │   │   │   └── semantic.rs
│   │   │   ├── consent.rs
│   │   │   ├── reactive.rs
│   │   │   └── lib.rs
│   │   └── tests/          # tests de integración (ver TDD)
│   ├── hivedb-index/       # wrappers tantivy + hnsw_rs + fusión RRF
│   └── hivedb-napi/        # binding napi-rs → expone C ABI a Bun
├── packages/
│   └── hive-db/            # capa TS @johpaz/hive-db (consume el .node)
│       ├── src/index.ts
│       └── test/           # tests de la API TS desde Bun
├── Cargo.toml              # workspace
└── package.json
```

---

## 12. Roadmap de implementación (orden TDD-first)

| Fase | Entregable | Gate (tests verdes) |
|---|---|---|
| 0 | Esqueleto workspace + CI | compila, `cargo test` corre vacío |
| 1 | Event + Log append-only sobre redb | §4.1, §4.2 |
| 2 | Proyecciones + replay determinista | §4.3, §4.4 |
| 3 | Working memory (TTL) | §4.5 |
| 4 | Semantic (tantivy + hnsw + RRF) | §4.6, §4.7 |
| 5 | Reactive engine | §4.8 |
| 6 | Consent graph | §4.9 |
| 7 | Concurrencia particionada | §4.10 |
| 8 | napi-rs binding + capa TS | §4.11 |
| 9 | Crash-safety / recovery | §4.4 (fuzz) |

Cada fase: **rojo → verde → refactor.** No se pasa de fase sin el gate verde.
