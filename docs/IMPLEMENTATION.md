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

Configurable por base vía `OpenOptions { vector_dimension }` (default 384 si no se especifica). `db.rs::resolve_vector_dimension()` la persiste en `meta.json` al crear la base y valida que coincida en cada `open` posterior — reabrir con una dimensión distinta falla con un error explícito en vez de corromper el índice.

### Persistencia de vectores

`VectorIndex` persiste cada `(id, vector)` en un archivo append-only `vec/vectors.bin` (bincode) y reconstruye el grafo HNSW al abrir. Un registro final truncado (crash a mitad de escritura) se descarta recortando el archivo al último registro completo.

### Filtros escalares

Actualmente solo `ScalarFilter::Eq { field, value }`, sobre **cualquier campo**. Cada filtro se indexa como token `campo\u{1F}valor` en un campo `STRING` multivalor de `tantivy` y se aplica como `TermQuery` obligatorio antes del ranking.

### Añadir una nueva estrategia de fusión

1. Añade variante en `Fusion`.
2. Implementa la lógica en `SemanticIndex::query_hybrid`.
3. Expón el parámetro en TS si aplica.

---

## 8. Colecciones de documentos (`hivedb-core/src/collections.rs`)

CRUD mutable sobre `redb`, independiente del event-log — la contraparte de tablas relacionales de SQLite. Añadido en el gate G10 (el nombre de archivo de tests quedó como `g9_collections.rs`/`g9_collections.test.ts` por una colisión de numeración con el gate G9 de distribución napi; es cosmético).

### Tablas `redb`

| Tabla | Clave | Contenido |
|---|---|---|
| `col_docs` | `(collection, id)` | `StoredDoc { version, json }` |
| `col_index_entries` | `(collection, field, value_token, id)` | entrada de índice secundario (marcador, sin valor) |
| `col_index_defs` | `(collection, field)` | `IndexDef { unique }` — persiste entre reaperturas |

### Versionado optimista

Cada `put` incrementa `version`. `PutOptions.expected_version`: `None` = sin chequeo, `Some(0)` = crear-solo, `Some(n)` = debe coincidir con la versión actual o falla con `version conflict`.

### Índices secundarios

`col_create_index(collection, field, unique)` recorre (`scan`) todos los docs existentes y los indexa (backfill). Si `unique` y hay un duplicado, la creación falla y no deja índice a medias — se revisa el conjunto completo antes de escribir cualquier entrada. Solo valores JSON escalares (string/number/bool) se indexan; arrays, objetos y campos ausentes se omiten sin error. El mantenimiento del índice (altas/bajas/cambios de valor) ocurre dentro de la misma transacción que el `put`/`delete` del documento vía los helpers compartidos `put_in_txn()` / `delete_in_txn()`.

### Batches atómicos

`col_batch(&[ColOp::Put{..} | ColOp::Delete{..}])` abre una única transacción `redb` (`begin_write()`) y aplica cada op con los mismos helpers `put_in_txn`/`delete_in_txn` que usan los métodos de una sola operación — garantiza que un fallo a mitad de la lista (p. ej. version conflict) aborta toda la transacción sin dejar cambios parciales.

### Añadir un nuevo método de colección

1. Implementa la función en `collections.rs` operando sobre `&Database` o dentro de una `WriteTransaction` si necesita atomicidad con otras ops.
2. Expón el facade en `db.rs` (`col_*`).
3. Añade el método napi en `hivedb-napi/src/lib.rs` (tipos `Js*` si el payload cruza la frontera FFI).
4. Añade el método en la clase `Collection<T>` de `packages/hive-db/src/index.ts`.

---

## 9. Grafo de consentimiento (`hivedb-core/src/state/consent_graph.rs`)

- Proyección **global**.
- Estado: `BTreeMap<u64, Grant>` donde la clave es el `seq` del evento `ConsentGranted`.
- `find_active_grant(agent, action, resource, now)` busca un grant vigente (no revocado, no expirado, scope matching).
- `ConsentGraph::apply` reacciona a `ConsentGranted` (inserta) y `ConsentRevoked` (elimina por `grant_seq`).

### Auditoría

`HiveDB::can` siempre hace `append(IntentLogged { actor, intent, authorized_by })` y devuelve `Decision { allowed, intent_log_seq }`.

---

## 10. Binding napi-rs (`hivedb-napi`)

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

## 11. Tests

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

## 12. Convenciones de código

- Formato con `rustfmt`.
- Clippy sin warnings (`-D warnings`).
- Errores propios: `HiveError` / `IndexError` con `thiserror`.
- `unsafe` solo en fronteras FFI/mmap; no en lógica de negocio.
- Nombres de proyecciones en PascalCase; nombres de test `snake_case` descriptivos.

---

## 13. Roadmap técnico próximo

1. Convertir `loom` en `dev-dependency` o feature; hoy es dependencia normal por facilidad de compilación.
2. Exponer working memory en el binding TS.
3. Añadir persistencia de checkpoints de proyección más granular.
4. Mejorar manejo de errores de callback en `ThreadsafeFunction`.

---

## 14. Distribución con `@napi-rs/cli`

El binding nativo se construye, empaqueta y publica usando `@napi-rs/cli` 3.x. Ver `docs/DISTRIBUTION.md` para la guía completa.

### Crates y versiones

- `napi` 3.10.1 + `napi-derive` 3.5.9 + `napi-build` 2.x en `crates/hivedb-napi/Cargo.toml`.
- `@napi-rs/cli` 3.7.2 en `packages/hive-db/package.json` (`devDependencies`).
- El crate `hivedb-napi` requiere el feature `tokio_rt` de `napi` (adicional a `napi8` y `async`) porque `subscribe` usa `tokio::spawn`])

### Configuración `napi` en `package.json`

```jsonc
"napi": {
  "binaryName": "hivedb-napi",
  "targets": [
    "x86_64-unknown-linux-gnu",
    "x86_64-unknown-linux-musl",
    "aarch64-unknown-linux-gnu",
    "x86_64-apple-darwin",
    "aarch64-apple-darwin",
    "x86_64-pc-windows-msvc"
  ]
}
```

### Scripts del paquete

| Script | Acción |
|---|---|
| `build:native` | `napi build --platform --release ...` + renombra `index.js` → `native.cjs` |
| `build` | `build:native` + `tsc` |
| `artifacts` | `napi artifacts` (coloca binarios en `npm/<triple>/`) |
| `create-npm-dirs` | `napi create-npm-dirs` (genera subpaquetes `npm/`) |
| `prepublishOnly` | `napi prepublish -t npm` + `tsc` |
| `preversion` | `napi build --platform` + `git add .` |
| `version` | `napi version` (sincroniza versión en subpaquetes) |

### CI (`.github/workflows/ci.yml`)

- **Job `lint-and-test`** (ubuntu): `cargo fmt`, `cargo clippy`, `cargo test`.
- **Job `build`** (matrix de 6 targets): compila cada binario con `napi build --platform --release --target <triple>`. Musl usa `-x` (cargo-zigbuild + Zig). El runner de macOS x64 se hace cross-compile desde `macos-latest` (ARM) — no se usa `macos-13` (Intel) porque GitHub lo está retirando.
- **Job `test-bun`** (ubuntu): descarga bindings linux-x64-gnu, compila TS, ejecuta `bun test`.
- **Job `publish`** (solo en tag `v*`): `create-npm-dirs` → `download-artifact` → `napi artifacts` → `npm publish` por subpaquete → `tsc` → `npm publish` principal.

### `.cargo/config.toml` (musl)

```toml
[target.x86_64-unknown-linux-musl]
rustflags = ["-C", "target-feature=-crt-static"]

[target.aarch64-unknown-linux-musl]
rustflags = ["-C", "target-feature=-crt-static"]
```

### Archivos generados (no commitear)

- `packages/hive-db/native.cjs` — loader multiplataforma generado por `napi build --platform`.
- `packages/hive-db/hivedb-napi.*.node` — binarios nativos por triple.
- `packages/hive-db/npm/` — subpaquetes por plataforma (regenerados por `napi create-npm-dirs`).
- `packages/hive-db/dist/` — salida de `tsc`.

Todos están en `.gitignore`.
