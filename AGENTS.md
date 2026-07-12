# AGENTS.md — HiveDB

Guía para agentes de código que trabajen en HiveDB.

## Contexto rápido

HiveDB es un motor de base de datos embebido en Rust con tres crates:

- `crates/hivedb-core` — event-log, proyecciones, working memory, reactive engine, consent graph.
- `crates/hivedb-index` — índice semántico híbrido (BM25 + ANN + RRF).
- `crates/hivedb-napi` — binding napi-rs 3.x que expone `HiveDB` a Bun/Node.

El log es **append-only e inmutable**. El motor asigna `seq` y `timestamp`. Las proyecciones se actualizan atómicamente dentro de la misma transacción `redb` (por shard desde G7).

## Construcción y verificación

Antes de entregar cualquier cambio, deben pasar:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Para tests de concurrencia con `loom`:

```bash
RUSTFLAGS="--cfg loom" cargo test --test g7_concurrency no_data_race_on_seq_assignment
```

## Estructura del código

```
crates/hivedb-core/src/
├── clock.rs          # Clock, SystemClock, MockClock
├── db.rs             # HiveDB: API pública y LogHandle
├── error.rs          # HiveError / HiveResult
├── event.rs          # Event, EventInput, EventKind, AgentId, StreamId, Scope
├── lib.rs            # re-exports
├── log.rs            # EventLog sharded sobre redb
├── memory/           # WorkingMemory (RAM + TTL)
├── memory_log.rs     # backend in-memory para loom
├── projection.rs     # trait Projection, ProjectionStore, registry
├── reactive.rs       # motor reactivo con tokio mpsc
├── shard.rs          # AgentShard (un redb por agente)
└── state/            # proyecciones: consent_graph, current_facts, task_state, tool_ledger

crates/hivedb-core/tests/
├── common/           # helpers compartidos
├── compile_fail.rs   # tests de API (trybuild)
├── g1_log.rs
├── g2_projections.rs
├── g2_tool_ledger.rs
├── g3_working.rs
├── g4_semantic.rs
├── g5_reactive.rs
├── g6_consent.rs
└── g7_concurrency.rs
```

## Convenciones

- **Idioma:** código y documentación pública en español (igual que SPEC/TDD), excepto nombres de API que siguen a SPEC.
- **Mínimo cambio:** no refactorizar más allá de lo necesario para el gate actual.
- **Tests:** cada gate añade su archivo `gN_*.rs` y se registra en `Cargo.toml`.
- **Reloj:** todo tiempo pasa por `Clock`. Nunca uses `SystemTime::now()` directamente en lógica de negocio.
- **Proyecciones:**
  - Locales (`CurrentFacts`, `TaskState`, `ToolLedger`): viven en cada shard; `project()` mergea estados parciales.
  - Globales (`ConsentGraph`): viven en `_global.redb`; implementar `Projection::scope() -> ProjectionScope::Global`.
- **Concurrencia:** desde G7, cada `agent_id` escribe en su propio shard `redb`; el `seq` global es `AtomicU64`.
- **Harness de larga duración (G9):** `CausalThread`, `build_agent_context` y `HarnessLoop` viven en `hivedb-core`. Es agnóstico del consumidor — ver `docs/AGENT_INTEGRATION.md` para el contrato de eventos que cualquier runtime de agentes debe cumplir, `SPEC.md` §9 para la arquitectura, TDD §5.1-§5.4 para el contrato de tests y `docs/IMPLEMENTATION.md` §9 para el detalle de implementación.
- **Distribución (G10):** el binario nativo se construye y publica con `@napi-rs/cli` 3.x. Ver `docs/DISTRIBUTION.md` y `docs/IMPLEMENTATION.md` §15. Los scripts `napi` viven en `packages/hive-db/package.json`.
- **napi-rs:** el crate `hivedb-napi` usa napi 3.10.1 con features `napi8`, `async`, `tokio_rt`. El runtime de tokio se captura en `open()` y se reutiliza en `subscribe()` (método sync que lanza `self.runtime.spawn`).

## Decisiones arquitectónicas vigentes

- Sharding por `agent_id` (un archivo `redb` por agente) para evitar el single-writer global de `redb`.
- `ConsentGraph` se mantiene en un shard global separado `_global.redb`.
- El contador `seq` persiste en la tabla `meta` del shard global (`_global.redb`) bajo la clave `"next_seq"`, pero **solo en eventos globales** (`ConsentGranted`, `ConsentRevoked`, `IntentLogged`) y en el `Drop` de `HiveDB`. Los eventos normales de agente no tocan `_global.redb`, preservando el sharding por agente. Las reaperturas usan el valor persistido y caen al escaneo de shards si la clave no existe o está atrasada.
- `HiveDB::open_in_memory()` no materializa proyecciones ni índice semántico; es solo para `loom`.

## Workarounds conocidos

- `zstd-sys = 2.0.9` está fijado en `Cargo.lock` porque `zstd-safe 6.0.6` (traído por `tantivy 0.21`) falla con `zstd-sys 2.0.16`. No actualizar `zstd-sys` sin verificar `cargo test --workspace`.
- `Cargo.lock` **está commiteado** (no en `.gitignore`) precisamente para preservar el fijado de `zstd-sys` en CI. No removerlo del repo.

## Puntos de extensión comunes

- Nuevo `EventKind`: añadir variante en `event.rs`, `EventKindTag`, y manejar en proyecciones/reactive/napi según corresponda. `LearningProposal` es el ejemplo añadido para G9.
- Nueva proyección: implementar `Projection`, registrar en `default_registry()`, decidir `ProjectionScope`.
- Nuevo gate de concurrencia: usar `DashMap`/mutex por shard; para `loom` usar `memory_log.rs`.
- Nuevo análisis de harness: añadir variante en `causal::AnomalyKind`, detector en `causal/mod.rs`, y generador de proposal en `harness.rs`.

## Contacto / dudas

- SPEC.md para requisitos.
- TDD.md para contratos de test.
- `.kimi/plans/` para planes de implementación aprobados.
