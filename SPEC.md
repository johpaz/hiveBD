# HiveDB — Especificación del Motor

> Motor de base de datos embebido, local-first, agent-native — pensado para ser el motor de
> memoria/persistencia de cualquier runtime de agentes; hoy usado por el ecosistema Hive.
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

    // Harness de larga duración (ver §9)
    LearningProposal, // proposal del HarnessLoop persistida para auditoría; el harness nunca la relee
}
```

> **Shape canónico de `ToolCall.payload.outcome`** (implementación actual): el
> `outcome` vive en el payload JSON del evento, no como campo tipado aparte, y
> debe ser uno de `"Ok"` / `"Timeout"` / `{"Err": "<mensaje>"}` — parseado por
> `parse_tool_outcome` (`state/causal_thread.rs`), la única fuente de verdad
> que usan tanto `ToolLedger` como `CausalThread`. Cualquier otro shape
> (por ejemplo strings sueltos como `"ok"`/`"error"`) cae en el default
> `Ok`, así que un fallo emitido con el shape equivocado se pierde en
> silencio. Ver `docs/AGENT_INTEGRATION.md` para el contrato completo de
> eventos.

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
| `ToolLedger` | agregados de uso de herramientas (latencia, costo, error-rate); scope `Agent` con merge cross-shard |
| `ConsentGraph` | grafo de delegaciones activas (ver §6); scope `Global` |

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
    text:   Option<String>,   // → tantivy BM25 (parsing tolerante, analyzer español)
    vector: Option<Vec<f32>>, // → hnsw_rs ANN
    filters: Vec<ScalarFilter>, // aplicados a texto y vector
    k: usize,
    fusion: Fusion,           // RRF { k } — solo aplica en modo híbrido
    boosts: Option<FieldBoosts>, // pesos por campo: name/body/tags
}
```

### 4.4 Colecciones de documentos (CRUD mutable sobre `redb`)

- Cuarto tier, distinto del event-log: mientras la memoria episódica es append-only e inmutable, las colecciones son un almacén de documentos JSON **mutable** (put/get/delete/scan) para el estado que no tiene semántica de evento — la contraparte de las tablas relacionales de SQLite.
- Documento = `{ id: string, version: u64, doc: JSON }`. `version` habilita **control de concurrencia optimista**: `put(id, doc, { expectedVersion })` falla con `version conflict` si la versión no coincide (o si `expectedVersion: 0` y el documento ya existe).
- Colecciones son namespaces por nombre de string; dos colecciones nunca comparten ids (`col_docs` está keyed por `(collection, id)`).
- **Índices secundarios de igualdad**: `createIndex(field, { unique? })` indexa un campo escalar (string/number/bool) del documento. Backfilla docs existentes al crearse; si el backfill encuentra un duplicado bajo `unique`, la creación del índice falla sin dejar rastro. Campos no-escalares (arrays, objetos) o ausentes se omiten silenciosamente del índice. `findBy(field, value)` sin índice creado sobre ese campo es un error explícito (no hay full-scan implícito).
- **Scan**: `scan({ prefix?, start?, offset?, limit?, reverse? })` recorre por orden lexicográfico de `id`.
- **Batch atómico**: `batch(ops)` aplica una lista de `Put`/`Delete` (potencialmente sobre distintas colecciones) en una sola transacción `redb` — si un op falla (p. ej. version conflict), nada se commitea.
- Tres tablas `redb` por debajo: `col_docs`, `col_index_entries`, `col_index_defs` (las definiciones de índice persisten y se re-verifican al reabrir la base).
- Coexiste en la misma base que el event-log y el índice semántico — un solo `HiveDB.open(path)` sirve los tres.

```rust
// Núcleo Rust (hivedb-core)
db.col_put("agents", "a1", &json!({"name": "Atlas"}), PutOptions::default())?; // -> version
db.col_get("agents", "a1")?;        // -> Option<DocEntry>
db.col_delete("agents", "a1")?;     // -> bool
db.col_scan("agents", &ScanOptions::default())?;
db.col_create_index("agents", "name", /* unique */ false)?;
db.col_find_by("agents", "name", &Value::from("Atlas"), &ScanOptions::default())?;
db.col_batch(&[ColOp::Put { .. }, ColOp::Delete { .. }])?;
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
- API: `can(agent, action, resource) -> Decision` resuelta por traversal del grafo vigente. El traversal es transitivo: `PM → Lead → Backend` autoriza a `Backend` si todos los eslabones son vigentes. Se detectan ciclos para evitar loops infinitos.
- Cada decisión de autorización emite un `IntentLogged` con `authorized_by` apuntando al `seq` del **grant directo** que cierra la cadena (el más cercano al solicitante) → cadena de auditoría completa y reproducible.

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

enum Predicate {
    Always,
    Eq { path: String, value: Value },       // igualdad en ruta JSON pointer
    Contains { path: String, value: Value }, // array contiene valor, o string contiene subcadena
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

## 9. Harness de larga duración (memoria causal + evaluación de proceso)

Primitivo del motor, no de la app (principio de diseño #4, §0): la memoria causal, la
construcción de contexto y la evaluación de proceso para tareas de agentes de larga duración
viven en `hivedb-core`, no se reimplementan por cada runtime que consume el motor. Este
harness es **agnóstico del consumidor** — no asume ningún concepto de aplicación específica
(sesión de chat, cola de jobs, checkpoint/lease de proceso); solo interpreta el event-log
genérico según el contrato de eventos descrito abajo. Ver `docs/AGENT_INTEGRATION.md` para el
contrato completo (vocabulario MUST/SHOULD/MAY) que cualquier runtime debe cumplir para
obtener valor de estos primitivos.

Tres piezas, todas puras (sin I/O más allá de leer el log/índice):

- **`CausalThread`** (`causal/mod.rs`): reconstruye, para un `stream_id`, el grafo de
  decisiones (`StateTransition`) y llamadas a herramientas (`ToolCall`) siguiendo los enlaces
  `causation`. Detecta dos anomalías: `ErrorLoop` (misma herramienta con el mismo error ≥ 3
  veces) y `ObjectiveDrift` (decisiones cuya `correlation` difiere del `IntentLogged` inicial
  del stream). Se reconstruye bajo demanda desde el log completo — ver la nota de
  escalabilidad en `docs/IMPLEMENTATION.md` §9.
- **`build_agent_context`** (`context.rs`): ventana de contexto acotada en tokens para un
  prompt de LLM, con estrategias configurables (`causal_anchors`, `compress_completed_phases`,
  `episodic_similarity` sobre el índice híbrido, `recent_anomalies`). Nunca excede el
  presupuesto de tokens solicitado; `content_hash()` permite chequeos de idempotencia.
- **`HarnessLoop::evaluate`** (`harness.rs`): evaluador puro que recibe un `CausalThread` (más
  episodios similares opcionales) y produce `process_quality`, `output_quality`, `root_cause`
  (decisión más temprana en la cadena de fallos), `findings` y `proposals`
  (`LearningProposal` con `evidence_seqs`/`confidence`/`specificity`). No tiene side effects —
  el llamador decide si persiste las proposals como eventos `LearningProposal` para auditoría.

**Contrato de payload que estas piezas asumen** (ver también §2.3): `StateTransition.payload`
lleva `description` (string) y opcionalmente `phase`; `ToolCall.payload` lleva `outcome` en el
shape canónico `"Ok"` / `"Timeout"` / `{"Err": "<mensaje>"}` (documentado en §2.2) y
opcionalmente `latency_ms`/`cost`; `IntentLogged.correlation` es el ancla contra la que se mide
`ObjectiveDrift`. Un consumidor que no siga este contrato simplemente no obtiene esas señales —
no hay fallos silenciosos más allá del ya corregido bug de shape de `outcome` (§2.2).

---

## 10. API pública (capa TS sobre napi-rs)

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

// Proyección / estado actual (hoy: projectTaskState; project genérico es roadmap)
const state = await db.projectTaskState(agentId, task.id);

// Búsqueda híbrida nativa (texto + vector + filtros, fusión RRF)
const hits = await db.queryHybrid({
  text: "error de compilación en el módulo de pagos",
  vector: embedding,
  filters: [{ field: "agentId", value: "BackendEngineer" }],
  k: 10,
});

// Suscripción reactiva con filtro por payload (push, no polling)
for await (const ev of db.events({
  kind: "Fact",
  predicate: { kind: "Eq", path: "/severity", value: "critical" },
})) {
  // despertado por el motor
}

// Consentimiento
const decision = await db.can("FrontendEngineer", "deploy", "prod");

// Métricas de herramientas
const stats = await db.toolStats("web_search");

// Último seq asignado
const last = await db.lastSeq();

// Colecciones de documentos (CRUD mutable, versionado optimista)
const agents = db.collection<{ name: string; role: string }>("agents");
const version = await agents.put("a1", { name: "Atlas", role: "worker" });
const entry = await agents.get("a1"); // -> { id, version, doc } | undefined
await agents.createIndex("role");
const workers = await agents.findBy("role", "worker");
await db.batch([
  { op: "put", collection: "agents", id: "a2", doc: { name: "Nova", role: "coordinator" } },
  { op: "delete", collection: "agents", id: "a1" },
]);
```

---

## 11. Layout en disco (soberanía: todo local, un directorio)

```
./hive-data/
├── shards/
│   ├── <agent_id>.redb  # event-log append-only + proyecciones por agente
│   └── _global.redb     # proyecciones globales + tabla meta (next_seq)
├── collections.redb     # colecciones de documentos: docs, índices secundarios, defs
├── fts/                 # índice tantivy (BM25)
├── vec/                 # índice hnsw_rs (ANN)
├── meta.json            # dimensión vectorial y settings inmutables
└── snapshots/           # snapshots opcionales de working memory
```

Backup = copiar el directorio. Sync futuro (Hive Cloud) = protocolo CRDT-friendly sobre el log, eventual-consistency sin reescribir el modelo.

---

## 12. Estructura del repositorio (monorepo)

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

## 13. Roadmap de implementación (orden TDD-first)

| Fase | Entregable | Gate (tests verdes) |
|---|---|---|
| 0 | Esqueleto workspace + CI | compila, `cargo test` corre vacío |
| 1 | Event + Log append-only sobre redb; persistencia de `next_seq` | §4.1, §4.2 |
| 2 | Proyecciones + replay determinista; `ToolLedger` | §4.3, §4.4 |
| 3 | Working memory (TTL) + binding TS | §4.5 |
| 4 | Semantic (tantivy + hnsw + RRF) | §4.6, §4.7 |
| 5 | Reactive engine con predicados de payload | §4.8 |
| 6 | Consent graph transitivo | §4.9 |
| 7 | Concurrencia particionada; `loom` fuera de builds de producción | §4.10 |
| 8 | napi-rs binding + capa TS (working memory, suscripciones con predicados, `toolStats`, `lastSeq`) | §4.11 |
| 9 (G9) | Harness de larga duración: `CausalThread`, `buildAgentContext`, `HarnessLoop` (§9) | TDD §5.1-§5.4 |
| 10 (G10) | Distribución multiplataforma con `@napi-rs/cli` (6 targets) | CI de release |
| 11 (G11) | Colecciones de documentos: CRUD versionado, índices secundarios, batches atómicos | §4.4 (colecciones) |

Cada fase: **rojo → verde → refactor.** No se pasa de fase sin el gate verde.

> Nota de numeración: esta tabla usa "Fase N" desde el diseño original; los docs más nuevos
> (`README.md`, `TDD.md`, `AGENTS.md`) llaman a las mismas fases "Gate GN" — son la misma
> secuencia, solo con dos nombres por evolución del proyecto. `TDD.md` también numera los tests
> de colecciones como `g9_collections.rs` por una colisión histórica; el gate funcional de
> colecciones es G11, no G9 (ver nota en `README.md`).
