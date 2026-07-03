# hivedb-core

Núcleo Rust de HiveDB. Implementa el event-log append-only y las proyecciones deterministas sobre `redb`.

## Gates implementados

- **G1 — Event Log**: `seq` monotónico global asignado por el motor, log inmutable, corrección vía evento de invalidación.
- **G2 — Proyecciones**: estado derivado en la misma transacción `redb` que el append; replay desde cero reconstruye el estado idéntico.
- **G3 — Working Memory**: almacenamiento en memoria `DashMap` con TTL por entrada; no persiste al log.
- **G4 — Semantic Memory**: búsqueda híbrida BM25 (`tantivy`) + ANN (`hnsw_rs`) con fusión RRF; filtros escalares empujados al índice.
- **G5 — Reactive Engine**: suscripciones push con `tokio::sync::mpsc`; matching por agente, tipo de evento y stream.

## Decisiones técnicas

- **Serialización de eventos**: `bincode` + serialización manual de `serde_json::Value` como string JSON. Suficiente para G1/G2; se puede migrar a `rkyv` si se necesita zero-copy mmap.
- **Almacenamiento**: `redb` con tablas para eventos, contador de secuencia, checkpoints y estado de proyecciones.
- **Proyecciones**: trait `Projection` con estado serializable; `CurrentFacts` y `TaskState` como proyecciones base.
- **Recovery**: al abrir se detecta el checkpoint más antiguo y se reconstruyen las proyecciones desde allí.

## Comandos

```bash
cargo test --workspace
cargo test --test g1_log
cargo test --test g2_projections
cargo test --test g3_working
cargo test --test g4_semantic
cargo test --test g5_reactive
```
