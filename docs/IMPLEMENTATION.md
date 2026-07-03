# HiveDB — Manual de implementación

> Guía para desarrolladores que mantienen o extienden el motor HiveDB.

---

## 1. Estructura del workspace

```
hiveBD/
├── Cargo.toml                 # workspace Rust
├── package.json               # workspace Bun
├── README.md
├── SPEC.md                    # especificación de diseño
├── TDD.md                     # contratos de test por fase
├── crates/
│   ├── hivedb-core/           # motor: log, proyecciones, consent, reactivo
│   ├── hivedb-index/          # índice semántico híbrido
│   └── hivedb-napi/           # binding napi-rs (cdylib)
└── packages/
    └── hive-db/               # envoltorio TypeScript para Bun
```

| Crate / Paquete | Responsabilidad | Tecnologías clave |
|---|---|---|
| `hivedb-core` | Event-log sharded, proyecciones deterministas, working memory, motor reactivo, consent graph | `redb`, `dashmap`, `tokio`, `serde_json` |
| `hivedb-index` | BM25 full-text (`tantivy`), ANN vectorial (`hnsw_rs`), fusión RRF | `tantivy`, `hnsw_rs` |
| `hivedb-napi` | Expone `HiveDB` al runtime JS vía napi-rs | `napi`, `napi-derive`, `tokio` |
| `@johpaz/hive-db` | API ergonómica TypeScript, async iterators, tipos | Bun |

---

## 2. Filosofía de implementación

1. **El log es la fuente de verdad.** Todo estado es una proyección derivada.
2. **Append-only.** Nunca se muta un evento existente.
3. **Cliente no controla `seq` ni `timestamp`.** El motor los asigna para garantizar orden y auditoría.
4. **Particionamiento por `agent_id`.** Cada agente tiene su propio shard `redb`; escrituras de distintos agentes no se bloquean.
5. **Determinismo puro.** Una proyección debe producir el mismo estado reproduciendo el log.

---

## 3. El event log (`hivedb-core/src/log.rs`, `shard.rs`)

### Shards

- Un directorio base contiene:
  - `shards/<agent_id>.redb` — un shard por agente.
  - `shards/_global.redb` — shard global para proyecciones y registro de consentimiento.
- `next_seq` y `seq_to_agent` viven en memoria (protegidos por primitivas `loom`-safe en tests).

### Flujo de `append`

1. `HiveDB::append(input)` → `LogHandle::append`.
2. Se asigna `seq = next_seq + 1`.
3. Se resuelve el shard del `agent_id` (o se crea).
4. En una transacción `redb`:
   - Guarda el evento.
   - Actualiza `seq_to_agent`.
   - Aplica handlers de proyección (agente y global).
5. Se dispara el evento por el `ReactiveEngine`.

### Tests de log

Los tests de G1 viven en `crates/hivedb-core/tests/g1_log.rs` y verifican monotonía global, inmutabilidad y ausencia de API de mutación.

---

## 4. Proyecciones (`hivedb-core/src/projection.rs`)

Una proyección es un *fold* determinista sobre el log.

```rust
pub trait Projection: Send + Sync + 'static {
    type State: Serialize + DeserializeOwned + PartialEq + Debug + Default + Clone;
    fn name() -> &'static str;
    fn scope() -> ProjectionScope { ProjectionScope::Agent }
    fn apply(state: &mut Self::State, event: &Event);
    fn merge(whole: &mut Self::State, part: &Self::State) { *whole = part.clone(); }
}
```

### Cómo añadir una proyección

1. Define el tipo de estado en `hivedb-core/src/state/`.
2. Implementa `Projection`.
3. Regístrala en `default_registry()` en `db.rs`:

```rust
fn default_registry() -> ProjectionRegistry {
    let mut registry = ProjectionRegistry::empty();
    registry.register::<CurrentFacts>();
    registry.register::<TaskState>();
    registry.register::<ConsentGraph>();
    registry.register::<MiProyeccion>(); // <-- nuevo
    registry
}
```

4. Añade test de G2/G3 que verifique replay determinista y checkpoints.

### Scope

- `Agent`: se mantiene una copia del estado por shard de agente. Útil para hechos vigentes, tareas, etc.
- `Global`: un único estado en `_global.redb`. Útil para consentimiento y métricas cross-agent.

### Merge

Para proyecciones de agente cuyo estado sea un mapa, sobrescribe `merge` para combinar resultados parciales de cada shard en lugar de reemplazarlos.

---

## 5. Eventos (`hivedb-core/src/event.rs`)

### Añadir un nuevo `EventKind`

1. Añade la variante en `EventKind`.
2. Añade el tag correspondiente en `EventKindTag`.
3. Implementa la conversión `From<&EventKind> for EventKindTag`.
4. Si el evento requiere payload estructurado, define la serialización en el JSON del `payload`.
5. Actualiza `js_to_event_input` en `crates/hivedb-napi/src/lib.rs` para traducir desde JS si es necesario.
6. Actualiza el tipo `EventInput.kind` en `packages/hive-db/src/index.ts`.

### Regla de oro

No expongas `seq` ni `timestamp` en `EventInput`. Hay un test `compile_fail` (`tests/ui/event_input_no_seq.rs`) que lo garantiza.

---

## 6. Motor reactivo (`hivedb-core/src/reactive.rs`)

- `ReactiveEngine` mantiene un `DashMap<u64, (EventPattern, UnboundedSender<Event>)>`.
- `subscribe(pattern)` devuelve una `Subscription` con receptor `tokio::mpsc::unbounded`.
- `dispatch(event)` itera suscriptores y envía clones si el evento coincide con el patrón.
- Semántica **at-least-once**: si el receptor se cerró, el `send` falla silenciosamente.

### En el binding napi

`JsHiveDB::subscribe` lanza una tarea `tokio` que lee del `Subscription` y llama a `ThreadsafeFunction` en modo no bloqueante. La callback JS recibe `(err, event)`.

---

## 7. Índice semántico (`hivedb-index`)

### Componentes

| Módulo | Responsabilidad |
|---|---|
| `text.rs` | `TextIndex` sobre `tantivy`: indexado y BM25. |
| `hnsw.rs` | `VectorIndex` sobre `hnsw_rs`: ANN de vectores `f32`. |
| `rrf.rs` | Fusión Reciprocal Rank. |
| `index.rs` | `SemanticIndex` orquesta text + vector + RRF. |

### Dimensión vectorial

La constante `VECTOR_DIMENSION` en `hivedb-core/src/db.rs` fija la dimensión en **384**. Todos los vectores indexados o consultados deben tener ese tamaño.

### Persistencia de vectores

`VectorIndex` persiste cada `(id, vector)` en un archivo append-only `vec/vectors.bin` (bincode) y reconstruye el grafo HNSW al abrir. Un registro final truncado (crash a mitad de escritura) se descarta recortando el archivo al último registro completo.

### Filtros escalares

Actualmente solo `ScalarFilter::Eq { field, value }`, sobre **cualquier campo**. Cada filtro se indexa como token `campo\u{1F}valor` en un campo `STRING` multivalor de `tantivy` y se aplica como `TermQuery` obligatorio antes del ranking.

### Añadir una nueva estrategia de fusión

1. Añade variante en `Fusion`.
2. Implementa la lógica en `SemanticIndex::query_hybrid`.
3. Expón el parámetro en TS si aplica.

---

## 8. Grafo de consentimiento (`hivedb-core/src/state/consent_graph.rs`)

- Proyección **global**.
- Estado: `BTreeMap<u64, Grant>` donde la clave es el `seq` del evento `ConsentGranted`.
- `find_active_grant(agent, action, resource, now)` busca un grant vigente (no revocado, no expirado, scope matching).
- `ConsentGraph::apply` reacciona a `ConsentGranted` (inserta) y `ConsentRevoked` (elimina por `grant_seq`).

### Auditoría

`HiveDB::can` siempre hace `append(IntentLogged { actor, intent, authorized_by })` y devuelve `Decision { allowed, intent_log_seq }`.

---

## 9. Binding napi-rs (`hivedb-napi`)

### Reglas importantes

- `u64` **no** es un tipo nativo JS en napi-rs 2.x. Expón `i64` y castea internamente.
- `ThreadsafeFunction` se escribe con `f` minúscula: `ThreadsafeFunction`.
- Los structs `#[napi(object)]` deben tener campos que implementen `FromNapiValue`/`ToNapiValue`.
- `Float32Array` se pasa directamente para vectores.
- La callback de `ThreadsafeFunction` recibe `(err, value)` en JS.

### Cómo exponer un nuevo método

1. Añade método en `#[napi] impl JsHiveDB`.
2. Convierte tipos JS ↔ Rust en funciones helper (`js_to_*`, `*_to_js`).
3. Recompila con `cargo build -p hivedb-napi --release`.
4. Actualiza `packages/hive-db/src/index.ts` con tipos y wrapper.
5. Añade test en `packages/hive-db/test/`.

### Construcción del `.node`

```bash
cd packages/hive-db
bun run build:napi   # cargo build --release + cp libhivedb_napi.so -> hivedb-napi.node
```

El artefacto es un shared object renombrado a `.node` para que Bun/Node lo carguen.

---

## 10. Tests

### Rust

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

### Bun

```bash
cd packages/hive-db
bun run build:napi
bun test
```

### Convenciones

- Cada gate G1-G8 tiene su archivo de test (`g1_log.rs`, `g2_projections.rs`, …).
- Los tests de propiedad usan `proptest` donde aplica.
- Los tests de concurrencia usan `loom` en `g7_concurrency.rs`.
- Los tests de compilación fallida usan `trybuild`.
- Los tests TS llevan ID `§4.11`, `§4.11b`, etc. del TDD.

---

## 11. Convenciones de código

- Formato con `rustfmt`.
- Clippy sin warnings (`-D warnings`).
- Errores propios: `HiveError` / `IndexError` con `thiserror`.
- `unsafe` solo en fronteras FFI/mmap; no en lógica de negocio.
- Nombres de proyecciones en PascalCase; nombres de test `snake_case` descriptivos.

---

## 12. Roadmap técnico próximo

1. Convertir `loom` en `dev-dependency` o feature; hoy es dependencia normal por facilidad de compilación.
2. Exponer working memory en el binding TS.
3. Añadir persistencia de checkpoints de proyección más granular.
4. Mejorar manejo de errores de callback en `ThreadsafeFunction`.
5. Empaquetar `.node` multiplataforma con `@napi-rs/cli` en CI.
