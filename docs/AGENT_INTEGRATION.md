# Integración de un runtime de agentes con el harness de HiveDB (G9)

## 1. Alcance

Este documento describe el vocabulario mínimo de eventos que un runtime de agentes debe
emitir al event log de HiveDB para obtener valor de `causalThread()`, `buildAgentContext()`,
`evaluateHarness()` y `toolStats()` — el harness de larga duración descrito en `SPEC.md` §9 e
implementado en `crates/hivedb-core/src/{causal/mod.rs,context.rs,harness.rs}`.

**hiveCode** (el swarm de agentes del ecosistema Hive) es el primer consumidor de referencia de
este contrato, no uno obligatorio. El código del harness no asume nada específico de hiveCode
— no hay ningún concepto de sesión de chat, cola de jobs, checkpoint/lease de proceso, ni
ningún otro vocabulario de aplicación en `causal/mod.rs`/`context.rs`/`harness.rs`. Cualquier
runtime que emita eventos con el shape descrito abajo obtiene las mismas garantías. La sección
4 muestra un ejemplo deliberadamente ajeno a hiveCode (triage de tickets de soporte) para que
quede claro que no hace falta "hablar" el vocabulario de un swarm de agentes de código.

## 2. Qué es / qué no es G9

G9 es una capa de **análisis causal retrospectivo y evaluación de proceso** sobre el event
log. Reconstruye qué pasó (`causalThread`), arma una ventana de contexto adaptativa para un
LLM a partir de eso (`buildAgentContext`), y evalúa la calidad del proceso — no solo del
resultado — con evidencia causal (`evaluateHarness`).

G9 **no es**:
- Una cola de jobs durable ni un motor de concurrencia/prioridad.
- Un mecanismo de checkpoint/lease/crash-recovery para procesos en curso.
- Un loop de verificación de metas multi-turno.
- Un grafo de dependencias/orquestación de tareas.

Esas cuatro cosas las sigue resolviendo cada aplicación en su propia capa — hoy, por ejemplo,
la app `hive` (el consumidor TypeScript más grande de `@johpaz/hive-db`) las implementa
íntegramente en su propio código de aplicación (`agent/run-store.ts`, `gateway/job-store.ts` +
`durable-queue.ts`, `agent/goal-runner.ts`, `scheduler/task-driver.ts`) sin usar ninguna
primitiva de G9 para eso, y así debe seguir: G9 no reemplaza esa capa, la complementa con
memoria causal y evaluación retrospectiva.

## 3. Vocabulario mínimo de eventos (MUST / SHOULD / MAY)

Derivado de leer directamente `causal/mod.rs`, `state/causal_thread.rs`, `context.rs` y
`harness.rs` — no es aspiracional, cada fila corresponde a una línea de código real.

| Campo | Nivel | Efecto si falta |
|---|---|---|
| `StateTransition.payload.description` (string) | **MUST** | sin esto el evento no se interpreta como una "decisión"; no aparece en `causalThread().decisions` ni en el contexto. |
| `StateTransition.payload.phase` (string) | SHOULD | sin `phase`, la decisión cae en la fase actual (`current_phase` del request) en vez de agruparse/comprimirse por su propia fase en `buildAgentContext`. |
| `Event.causation` (seq del evento que lo causó) | SHOULD | sin `causation`, el evento queda sin padre en el grafo — no participa de anclas causales (`causalAnchors`) ni de la resolución de `rootCause`. |
| `Event.correlation` (UUID) | SHOULD | sin `correlation`, `ObjectiveDrift` no puede comparar la decisión contra la intención original — nunca se detecta esa anomalía para ese evento. |
| `ToolCall { tool }` + `payload.outcome` | **MUST** ser `"Ok"` \| `"Timeout"` \| `{"Err": "<mensaje>"}` | cualquier otro shape (incluyendo strings sueltos como `"ok"`/`"error"`) cae en el default `Ok` — un fallo real se pierde en silencio, tanto para `toolStats().errors` como para la detección de `ErrorLoop`/`rootCause`. Ver `SPEC.md` §2.2. |
| `ToolCall.payload.latency_ms` / `.cost` | SHOULD | sin esto, `toolStats()` sigue contando invocaciones/errores pero `totalLatencyMs`/`totalCost` quedan en 0. |
| `IntentLogged { actor, intent, authorized_by }` | **MUST** si se quiere `ObjectiveDrift` o anclaje del objetivo actual en `buildAgentContext` | sin un `IntentLogged` inicial en el stream, `detect_objective_drift` no tiene contra qué comparar y nunca dispara; `buildAgentContext` no puede ubicar la decisión "actual" por texto de objetivo si no hay decisiones que la mencionen. |
| Documentos de episodio indexados con `upsertDoc`/`queryHybrid`, campo escalar `kind = "episode"` | **MUST** si se usa `episodicSimilarity` en `ContextStrategy` | hoy este filtro está *hardcodeado* en `context.rs` (`collect_similar_episodes`) y no está documentado en ningún otro lado — sin ese campo, la estrategia de similaridad episódica devuelve siempre una lista vacía, sin error. |
| `LearningProposal` (evento, sin payload propio requerido) | MAY | es solo un canal de auditoría — si el consumidor decide persistir las proposals que devuelve `evaluateHarness()`, este es el `EventKind` a usar. El harness nunca relee estos eventos; no hay side effects automáticos. |

Todo lo demás (`Fact`, `MemoryWrite`, `ConsentGranted`, etc.) es ortogonal a G9 — el harness los
ignora.

## 4. Ejemplo mínimo (vocabulario deliberadamente no-hiveCode)

Runtime de ejemplo: un sistema de triage de tickets de soporte, sin relación con "swarms" de
código ni sesiones de agentes de programación — para dejar explícito que el contrato no asume
ese dominio.

```ts
import { HiveDB } from "@johpaz/hive-db";

const db = await HiveDB.open("./triage-data");

// 1. Intención inicial del stream (ancla para ObjectiveDrift).
const intentSeq = await db.append({
  agentId: "Router",
  streamId: "ticket-4821",
  kind: "IntentLogged",
  payload: JSON.stringify({
    actor: "Router",
    intent: "resolver timeout de checkout reportado por el cliente",
  }),
});

// 2. Una decisión, causada por la intención.
const decisionSeq = await db.append({
  agentId: "Analyst",
  streamId: "ticket-4821",
  kind: "StateTransition",
  payload: JSON.stringify({ description: "clasificar como bug de pagos", phase: "triage" }),
  causation: intentSeq,
});

// 3. Una llamada a herramienta, con el shape canónico de `outcome`.
await db.append({
  agentId: "Analyst",
  streamId: "ticket-4821",
  kind: "ToolCall",
  payload: JSON.stringify({
    tool: "classify_ticket",
    latency_ms: 340,
    cost: 0.002,
    outcome: "Ok",
  }),
  causation: decisionSeq,
});

// 4. Reconstruir el hilo causal y evaluarlo.
const thread = await db.causalThread("ticket-4821");
const evaluation = await db.evaluateHarness({
  causalThread: thread,
  similarEpisodes: [],
  originalIntent: "resolver timeout de checkout",
  currentState: { outcome: "resolved" },
  minConfidence: 0.5,
});

console.log(evaluation.processQuality, evaluation.outputQuality);
```

Este mismo escenario (con roles `Analyst`/`Router`/`Auditor`, streams `ticket-*` y tools
`classify_ticket`/`lookup_customer`/`escalate_to_human`) está cubierto como regression guard en
`crates/hivedb-core/tests/contract_generic_consumer.rs` y
`packages/hive-db/test/generic_consumer.test.ts` — sirven como referencia ejecutable de este
contrato y protegen contra que futuras features del harness vuelvan a asumir implícitamente el
vocabulario de hiveCode.

## 5. Versionado del contrato

Este contrato está versionado junto con el crate (`@johpaz/hive-db`). Mientras el paquete esté
en `0.x` (pre-1.0), un cambio breaking al shape de eventos requerido o a la forma del JSON de
salida de `causalThread`/`buildAgentContext`/`evaluateHarness` bumpea la versión **minor**, no
patch. Una vez que el paquete llegue a `1.0.0`, este documento pasa a regirse por semver
estricto: cambios breaking al contrato requieren un major bump.

## 6. Ver también

- `SPEC.md` §9 — descripción a nivel de arquitectura de las tres piezas del harness.
- `docs/IMPLEMENTATION.md` §9 — detalle de implementación, contrato de wire JSON (camelCase +
  tagging interno) y el límite de escalabilidad conocido de `causalThread()`.
- `TDD.md` §5.1-§5.4 — contrato de tests (con ejemplos en el vocabulario de hiveCode, el primer
  consumidor de referencia).
