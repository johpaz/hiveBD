# AGENTS.md — HiveDB

Guía para agentes de código que trabajen en HiveDB.

## Contexto rápido

HiveDB es un motor de base de datos embebido en Rust con dos crates:

- `crates/hivedb-core` — event-log, proyecciones, working memory, reactive engine, consent graph.
- `crates/hivedb-index` — índice semántico híbrido (BM25 + ANN + RRF).

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
└── state/            # proyecciones: consent_graph, current_facts, task_state

crates/hivedb-core/tests/
├── common/           # helpers compartidos
├── compile_fail.rs   # tests de API (trybuild)
├── g1_log.rs
├── g2_projections.rs
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
  - Locales (`CurrentFacts`, `TaskState`): viven en cada shard; `project()` mergea estados parciales.
  - Globales (`ConsentGraph`): viven en `_global.redb`; implementar `Projection::scope() -> ProjectionScope::Global`.
- **Concurrencia:** desde G7, cada `agent_id` escribe en su propio shard `redb`; el `seq` global es `AtomicU64`.

## Decisiones arquitectónicas vigentes

- Sharding por `agent_id` (un archivo `redb` por agente) para evitar el single-writer global de `redb`.
- `ConsentGraph` se mantiene en un shard global separado `_global.redb`.
- El contador `seq` no se persiste; se reconstruye al abrir escaneando shards (suficiente hasta G9).
- `HiveDB::open_in_memory()` no materializa proyecciones ni índice semántico; es solo para `loom`.

## Workarounds conocidos

- `zstd-sys = 2.0.9` está fijado en `Cargo.lock` porque `zstd-safe 6.0.6` (traído por `tantivy 0.21`) falla con `zstd-sys 2.0.16`. No actualizar `zstd-sys` sin verificar `cargo test --workspace`.

## Puntos de extensión comunes

- Nuevo `EventKind`: añadir variante en `event.rs`, `EventKindTag`, y manejar en proyecciones/reactive según corresponda.
- Nueva proyección: implementar `Projection`, registrar en `default_registry()`, decidir `ProjectionScope`.
- Nuevo gate de concurrencia: usar `DashMap`/mutex por shard; para `loom` usar `memory_log.rs`.

## Contacto / dudas

- SPEC.md para requisitos.
- TDD.md para contratos de test.
- `.kimi/plans/` para planes de implementación aprobados.
