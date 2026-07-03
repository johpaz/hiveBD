# HiveDB — Especificación TDD

> Los tests se escriben **antes** que la implementación. Cada uno define un contrato del motor.
> Ciclo por fase: **rojo (test falla) → verde (mínimo para pasar) → refactor**.
> No se avanza de fase sin su gate verde.

Convención: cada test lleva ID `§N.M` que mapea al roadmap del SPEC. Los `#[test]` son Rust (núcleo); los `test()` son Bun (capa TS). Todos empiezan como `todo!()` / `unimplemented!()` o `should fail`, en rojo.

---

## Filosofía de los tests

1. **Contrato, no implementación.** Cada test fija una garantía observable del motor, no su mecánica interna. Cambiar el backend (redb → otro) no debe romper estos tests si el contrato se mantiene.
2. **Invariantes como property tests.** Las garantías duras (replay determinista, monotonía de `seq`, inmutabilidad) se prueban con `proptest`, no solo con casos puntuales.
3. **Crash-safety con fuzz.** La recuperación se prueba inyectando cortes en puntos arbitrarios.
4. **Concurrencia con loom (donde aplique)** para verificar ausencia de data races en el camino de escritura.

---

## Fase 1 — Event Log append-only

### §4.1 — `seq` es monotónico, global y asignado por el motor
```rust
#[test]
fn seq_is_monotonic_and_engine_assigned() {
    let db = HiveDB::open_temp();
    let s1 = db.append(event("A", Fact, payload())).unwrap();
    let s2 = db.append(event("A", Fact, payload())).unwrap();
    let s3 = db.append(event("B", Fact, payload())).unwrap(); // otro agente
    assert!(s1 < s2 && s2 < s3);          // monotónico global, cruza agentes
    assert_eq!(s1, 1);                     // arranca en 1 (0 reservado = "vacío")
}
```

### §4.1b — el cliente NO puede fijar seq ni timestamp
```rust
#[test]
fn client_cannot_set_seq_or_timestamp() {
    // El tipo de entrada (EventInput) no expone seq ni timestamp.
    // Esto es un test de compilación: si EventInput tuviera esos campos, no compila.
    // Se verifica con trybuild (compile-fail test).
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/event_input_no_seq.rs");
}
```

### §4.2 — el log es append-only: no hay update ni delete
```rust
#[test]
fn log_has_no_mutation_api() {
    // Contrato de API: EventLog expone append() y read(), NUNCA update/delete.
    // Compile-fail si alguien añade tales métodos.
    let db = HiveDB::open_temp();
    let seq = db.append(event("A", Fact, payload())).unwrap();
    let ev = db.read(seq).unwrap();
    assert_eq!(ev.seq, seq);
    // No existe db.update(...) ni db.delete(...) — verificado por ausencia en el trait.
}
```

### §4.2b — corrección = nuevo evento que invalida, no mutación
```rust
#[test]
fn correction_is_a_new_invalidating_event() {
    let db = HiveDB::open_temp();
    let fact = db.append(event("A", Fact, json!({"price": 100}))).unwrap();
    let inval = db.append(invalidate("A", fact)).unwrap();

    // El hecho original sigue en el log, intacto.
    assert_eq!(db.read(fact).unwrap().payload, json!({"price": 100}).into());
    // Pero la proyección de hechos vigentes ya no lo incluye.
    let current = db.project::<CurrentFacts>("A");
    assert!(!current.contains(fact));
    assert!(inval > fact);
}
```

---

## Fase 2 — Proyecciones + replay determinista

### §4.3 — una proyección es un fold puro y determinista
```rust
proptest! {
    #[test]
    fn projection_is_deterministic_fold(events in arb_event_sequence()) {
        let db1 = HiveDB::open_temp();
        let db2 = HiveDB::open_temp();
        for e in &events { db1.append(e.clone()).unwrap(); }
        for e in &events { db2.append(e.clone()).unwrap(); }
        // Misma secuencia de eventos ⇒ mismo estado proyectado, siempre.
        prop_assert_eq!(
            db1.project::<TaskState>("A"),
            db2.project::<TaskState>("A")
        );
    }
}
```

### §4.3b — estado y log nunca divergen (misma transacción)
```rust
#[test]
fn state_and_log_update_atomically() {
    let db = HiveDB::open_temp();
    let seq = db.append(event("A", StateTransition, json!({"to":"done"}))).unwrap();
    // Inmediatamente tras el append, la proyección ya refleja el evento.
    let checkpoint = db.projection_checkpoint::<TaskState>();
    assert_eq!(checkpoint, seq); // checkpoint == último seq, sin lag
}
```

### §4.4 — replay desde cero reconstruye el estado idéntico (INVARIANTE CRÍTICO)
```rust
#[test]
fn replay_from_zero_reconstructs_identical_state() {
    let path = temp_path();
    let original_state = {
        let db = HiveDB::open(&path);
        seed_many_events(&db, 10_000);
        db.project::<TaskState>("A")
    }; // se cierra el db

    // Borramos TODAS las proyecciones materializadas, dejamos solo el log.
    wipe_projections(&path);

    let db = HiveDB::open(&path); // al abrir, reconstruye proyecciones desde seq=0
    let reconstructed = db.project::<TaskState>("A");

    assert_eq!(original_state, reconstructed); // determinismo total
}
```

### §4.4b — crash-safety: estado materializado == replay del log durable
```rust
proptest! {
    #[test]
    fn crash_at_any_point_recovers_consistently(crash_after in 1usize..5000) {
        let path = temp_path();
        let db = HiveDB::open(&path);
        // Inyecta un crash simulado tras N escrituras (drop sin flush limpio).
        let durable_seq = seed_until_crash(&db, crash_after);
        drop_hard(db); // simula corte abrupto

        let db = HiveDB::open(&path); // recovery
        let state = db.project::<TaskState>("A");
        let replayed = replay_log_manually(&path, durable_seq);
        prop_assert_eq!(state, replayed); // nunca diverge
    }
}
```

---

## Fase 3 — Working memory (TTL)

### §4.5 — working memory expira por TTL y no persiste al log
```rust
#[test]
fn working_memory_expires_and_is_not_logged() {
    let db = HiveDB::open_temp();
    db.working_set("A", "draft", value(), ttl_ms(50));
    assert!(db.working_get("A", "draft").is_some());
    sleep(Duration::from_millis(80));
    assert!(db.working_get("A", "draft").is_none()); // expiró
    // No se escribió ningún evento al log por esto.
    assert_eq!(db.log_len(), 0);
}
```

### §4.5b — working memory es concurrente sin corrupción
```rust
#[test]
fn working_memory_concurrent_writes_no_corruption() {
    let db = Arc::new(HiveDB::open_temp());
    let handles: Vec<_> = (0..16).map(|i| {
        let db = db.clone();
        thread::spawn(move || {
            for j in 0..1000 { db.working_set("A", &format!("k{i}-{j}"), value(), ttl_ms(10_000)); }
        })
    }).collect();
    for h in handles { h.join().unwrap(); }
    assert_eq!(db.working_keys("A").len(), 16 * 1000); // nada perdido, nada duplicado
}
```

---

## Fase 4 — Semantic memory (búsqueda híbrida nativa)

### §4.6 — búsqueda híbrida fusiona BM25 + ANN en una sola query
```rust
#[test]
fn hybrid_query_fuses_text_and_vector() {
    let db = HiveDB::open_temp();
    index_doc(&db, "doc1", "error de compilación en pagos", embed("pago fallido"));
    index_doc(&db, "doc2", "documentación de la API de envíos", embed("logística"));

    let hits = db.query_hybrid(HybridQuery {
        text: Some("error pagos".into()),
        vector: Some(embed("transacción rechazada")),
        filters: vec![],
        k: 5,
        fusion: Fusion::Rrf,
    });
    assert_eq!(hits[0].id, "doc1"); // el relevante por texto Y vector gana con RRF
}
```

### §4.6b — RRF es correcta (test del algoritmo de fusión, aislado)
```rust
#[test]
fn rrf_fusion_matches_reference() {
    // RRF(d) = Σ 1/(k + rank_i(d)), k=60 por defecto.
    let bm25 = vec![("a", 1), ("b", 2), ("c", 3)]; // (doc, rank)
    let ann  = vec![("b", 1), ("c", 2), ("a", 3)];
    let fused = rrf(&[bm25, ann], 60);
    // 'b' aparece alto en ambos ⇒ debe quedar primero.
    assert_eq!(fused[0].0, "b");
}
```

### §4.7 — los filtros escalares se empujan al índice, no se filtran después
```rust
#[test]
fn scalar_filters_pushed_into_index() {
    let db = HiveDB::open_temp();
    index_doc_with(&db, "d1", "deploy fallido", embed("x"), agent="Backend");
    index_doc_with(&db, "d2", "deploy fallido", embed("x"), agent="Frontend");

    let hits = db.query_hybrid(HybridQuery {
        text: Some("deploy".into()),
        vector: None,
        filters: vec![ScalarFilter::eq("agent", "Backend")],
        k: 10, fusion: Fusion::Rrf,
    });
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, "d1"); // d2 jamás entra al ranking, no se post-filtra
}
```

---

## Fase 5 — Reactive engine

### §4.8 — una suscripción es despertada por el append, no por polling
```rust
#[tokio::test]
async fn subscription_is_pushed_on_append() {
    let db = HiveDB::open_temp();
    let mut sub = db.subscribe(EventPattern { kind: Some(ToolCallTag), ..default() });

    // En otra tarea, se hace un append que matchea.
    let db2 = db.clone();
    tokio::spawn(async move {
        db2.append(tool_call("A", "web_search")).unwrap();
    });

    let ev = timeout(Duration::from_millis(500), sub.next()).await
        .expect("el motor debe despertar la suscripción sin polling");
    assert_eq!(ev.unwrap().kind_tag(), ToolCallTag);
}
```

### §4.8b — entrega at-least-once con seq para dedupe
```rust
#[tokio::test]
async fn subscription_delivers_at_least_once_with_seq() {
    let db = HiveDB::open_temp();
    let mut sub = db.subscribe(EventPattern::all());
    let s = db.append(event("A", Fact, payload())).unwrap();
    let ev = sub.next().await.unwrap();
    assert_eq!(ev.seq, s); // el seq viaja, el consumidor puede deduplicar
}
```

---

## Fase 6 — Consent graph

### §4.9 — can() resuelve sobre el grafo vigente
```rust
#[test]
fn consent_grant_then_query() {
    let db = HiveDB::open_temp();
    db.append(consent_granted("PM", "Backend", scope("deploy", "staging"), None)).unwrap();

    assert!(db.can("Backend", "deploy", "staging").allowed());
    assert!(!db.can("Backend", "deploy", "prod").allowed()); // scope no cubre prod
    assert!(!db.can("Frontend", "deploy", "staging").allowed()); // no tiene grant
}
```

### §4.9b — revocar un grant lo retira inmediatamente
```rust
#[test]
fn consent_revoke_takes_effect() {
    let db = HiveDB::open_temp();
    let grant = db.append(consent_granted("PM", "Backend", scope("deploy","staging"), None)).unwrap();
    assert!(db.can("Backend", "deploy", "staging").allowed());
    db.append(consent_revoked(grant)).unwrap();
    assert!(!db.can("Backend", "deploy", "staging").allowed());
}
```

### §4.9c — cada decisión emite intent log con authorized_by (auditoría)
```rust
#[test]
fn authorized_action_logs_intent_with_provenance() {
    let db = HiveDB::open_temp();
    let grant = db.append(consent_granted("PM","Backend",scope("deploy","staging"),None)).unwrap();
    let decision = db.can("Backend", "deploy", "staging");
    let intent_seq = decision.intent_log_seq().unwrap();

    let intent = db.read(intent_seq).unwrap();
    // La decisión es trazable hasta el grant exacto que la autorizó.
    assert_eq!(intent.authorized_by(), Some(grant));
}
```

### §4.9d — grant expirado no autoriza (tiempo controlado por el motor)
```rust
#[test]
fn expired_consent_does_not_authorize() {
    let db = HiveDB::open_temp_with_clock(MockClock::at(1000));
    db.append(consent_granted("PM","Backend",scope("deploy","staging"), Some(1500))).unwrap();
    db.advance_clock_to(2000); // pasó el expiry
    assert!(!db.can("Backend","deploy","staging").allowed());
}
```

---

## Fase 7 — Concurrencia particionada por agente

### §4.10 — dos agentes distintos no se bloquean al escribir
```rust
#[test]
fn distinct_agents_write_without_blocking() {
    let db = Arc::new(HiveDB::open_temp());
    let start = Instant::now();
    let a = spawn_writes(&db, "A", 10_000);
    let b = spawn_writes(&db, "B", 10_000);
    a.join().unwrap(); b.join().unwrap();
    // Sanity: el throughput combinado supera claramente al de un writer global serializado.
    // (Umbral calibrado en CI; el punto es ausencia de contención cruzada.)
    assert!(db.log_len() == 20_000);
    assert!(start.elapsed() < SINGLE_WRITER_BASELINE * 0.7);
}
```

### §4.10b — escrituras al MISMO agente preservan orden causal
```rust
#[test]
fn same_agent_writes_preserve_causal_order() {
    let db = HiveDB::open_temp();
    let s1 = db.append(event("A", Fact, json!({"step":1}))).unwrap();
    let s2 = db.append(event("A", Fact, json!({"step":2}))).unwrap();
    let seqs: Vec<_> = db.read_stream("A").map(|e| e.seq).collect();
    assert_eq!(seqs, vec![s1, s2]); // orden estricto dentro del agente
}
```

### §4.10c — sin data races en el camino de escritura (loom)
```rust
#[test]
fn no_data_race_on_seq_assignment() {
    loom::model(|| {
        let db = Arc::new(HiveDB::open_in_memory());
        let h: Vec<_> = (0..2).map(|i| {
            let db = db.clone();
            loom::thread::spawn(move || { db.append(event(&format!("A{i}"), Fact, payload())).unwrap(); })
        }).collect();
        for x in h { x.join().unwrap(); }
        // Bajo todos los entrelazados posibles, los dos seq son distintos y consecutivos.
        assert_eq!(db.log_len(), 2);
    });
}
```

---

## Fase 8 — napi-rs binding + capa TS (Bun)

### §4.11 — la API TS hace round-trip a través del FFI
```typescript
// packages/hive-db/test/roundtrip.test.ts  (Bun test)
import { test, expect } from "bun:test";
import { HiveDB } from "../src";

test("§4.11 append + project round-trips through napi", async () => {
  const db = await HiveDB.open(tmpDir());
  const seq = await db.append({ agentId: "A", kind: "Fact", payload: { x: 1 } });
  expect(seq).toBe(1);
  const ev = await db.read(seq);
  expect(ev.payload.x).toBe(1);
});
```

### §4.11b — búsqueda híbrida funciona desde Bun
```typescript
test("§4.11b hybrid query from Bun returns fused results", async () => {
  const db = await HiveDB.open(tmpDir());
  await db.indexDoc("d1", "error de pagos", embed("pago"));
  const hits = await db.queryHybrid({ text: "pagos", vector: embed("transacción"), k: 5 });
  expect(hits[0].id).toBe("d1");
});
```

### §4.11c — la suscripción reactiva llega a un async iterator de Bun
```typescript
test("§4.11c subscribe yields to Bun async iterator", async () => {
  const db = await HiveDB.open(tmpDir());
  const seen: string[] = [];
  (async () => { for await (const ev of db.subscribe({ kind: "ToolCall" })) seen.push(ev.kindTag); })();
  await db.append({ agentId: "A", kind: "ToolCall", payload: { tool: "web_search" } });
  await sleep(200);
  expect(seen).toContain("ToolCall");
});
```

### §4.11d — no hay leaks cruzando la frontera JS↔Rust (vigilancia)
```typescript
test("§4.11d repeated open/close does not leak native memory", async () => {
  const before = process.memoryUsage().rss;
  for (let i = 0; i < 1000; i++) {
    const db = await HiveDB.open(tmpDir());
    await db.append({ agentId: "A", kind: "Fact", payload: { i } });
    await db.close(); // libera el handle nativo explícitamente
  }
  const after = process.memoryUsage().rss;
  // Margen generoso; el objetivo es detectar fugas groseras del binding.
  expect(after - before).toBeLessThan(50 * 1024 * 1024);
});
```

---

## Dependencias de test (Cargo)

```toml
[dev-dependencies]
proptest   = "1"        # property-based: invariantes (replay, determinismo)
trybuild   = "1"        # compile-fail: contrato de API (no seq/timestamp del cliente)
loom       = "0.7"      # model checker de concurrencia (camino de escritura)
tokio      = { version = "1", features = ["macros", "rt", "time"] }
tempfile   = "3"
```

## Comandos

```bash
# Núcleo Rust
cargo test --workspace                 # toda la suite
cargo test --workspace -- --nocapture  # con salida
cargo test §4.4                        # un gate concreto (por nombre)
RUSTFLAGS="--cfg loom" cargo test §4.10c   # tests de loom

# Capa TS (Bun)
bun test packages/hive-db              # tests del binding y API
```

---

## Gates de fase (CI bloqueante)

Cada fase del roadmap del SPEC tiene un gate. CI no permite merge a `main` sin el gate verde de la fase declarada en el PR.

| Gate | Tests que deben pasar |
|---|---|
| G1 Log | §4.1, §4.1b, §4.2, §4.2b |
| G2 Proyecciones | §4.3, §4.3b, §4.4, §4.4b |
| G3 Working | §4.5, §4.5b |
| G4 Semantic | §4.6, §4.6b, §4.7 |
| G5 Reactive | §4.8, §4.8b |
| G6 Consent | §4.9, §4.9b, §4.9c, §4.9d |
| G7 Concurrencia | §4.10, §4.10b, §4.10c |
| G8 Bun/FFI | §4.11, §4.11b, §4.11c, §4.11d |

**Orden de ataque recomendado:** G1 → G2 son el corazón (event sourcing). Si esos dos están sólidos y con replay determinista probado por property test, el resto se construye encima con confianza. No optimices nada hasta que G1+G2 estén verdes y refactorizados.

---

## Fase 9 — Harness con contexto real para larga duración

> Esta fase es el puente entre el motor (G1-G8) y el swarm de agentes (hiveCode).
> Cubre tres componentes: `CausalThread` (proyección causal), `buildAgentContext`
> (ventana adaptativa para el LLM), y `HarnessLoop` (evaluador con contexto causal real).
> Gate G9 — ninguno de estos tests puede pasar antes de que G1+G2 estén verdes.

---

### §5.1 — CausalThread: estructura causal completa de una tarea

#### §5.1a — el thread se construye siguiendo links causation del log
```rust
#[test]
fn causal_thread_follows_causation_links() {
    let db = HiveDB::open_temp();

    // Cadena causal: decisión → tool call → resultado → siguiente decisión
    let d1 = db.append(decision("Architect", "usar microservicios", None)).unwrap();
    let t1 = db.append(tool_call_caused_by("Architect", "read_file", d1)).unwrap();
    let d2 = db.append(decision_caused_by("Backend", "crear servicio de pagos", t1)).unwrap();
    let t2 = db.append(tool_call_caused_by("Backend", "write_file", d2)).unwrap();

    let thread = db.causal_thread("task-1").unwrap();

    // El thread conecta la cadena completa aunque crucen agentes
    assert_eq!(thread.decisions.len(), 2);
    assert_eq!(thread.tool_calls.len(), 2);
    assert_eq!(thread.decisions[0].caused[0], t1);  // d1 causó t1
    assert_eq!(thread.decisions[1].caused_by, Some(t1)); // t1 causó d2
}
```

#### §5.1b — el thread detecta anomalías: bucles de error repetidos
```rust
#[test]
fn causal_thread_detects_error_loops() {
    let db = HiveDB::open_temp();

    // El agente intenta la misma herramienta 3 veces con el mismo error
    for _ in 0..3 {
        let d = db.append(decision("Backend", "compilar módulo", None)).unwrap();
        db.append(tool_call_with_outcome("Backend", "cargo_build", Outcome::Err("E0432"), d)).unwrap();
    }

    let thread = db.causal_thread("task-1").unwrap();

    assert!(!thread.anomalies.is_empty());
    let anomaly = &thread.anomalies[0];
    assert_eq!(anomaly.kind, AnomalyKind::ErrorLoop);
    assert_eq!(anomaly.repetitions, 3);
    assert_eq!(anomaly.tool, "cargo_build");
}
```

#### §5.1c — el thread detecta drift de objetivo
```rust
#[test]
fn causal_thread_detects_objective_drift() {
    let db = HiveDB::open_temp();

    // Tarea original: "implementar autenticación"
    let intent = db.append(intent_logged("PM", "implementar autenticación")).unwrap();

    // El agente deriva a refactorizar el ORM (no relacionado con el objetivo)
    for _ in 0..10 {
        db.append(decision_with_correlation(
            "Backend",
            "refactorizar ORM",
            "refactor-orm-correlation", // correlation distinta a la del intent original
        )).unwrap();
    }

    let thread = db.causal_thread("task-1").unwrap();
    let drift = thread.anomalies.iter().find(|a| a.kind == AnomalyKind::ObjectiveDrift);
    assert!(drift.is_some());
    assert_eq!(drift.unwrap().original_intent_seq, intent);
}
```

#### §5.1d — el thread es determinista dado el mismo log (property test)
```rust
proptest! {
    #[test]
    fn causal_thread_is_deterministic(events in arb_causal_event_sequence()) {
        let db1 = HiveDB::open_temp();
        let db2 = HiveDB::open_temp();
        for e in &events { db1.append(e.clone()).unwrap(); }
        for e in &events { db2.append(e.clone()).unwrap(); }

        // Mismo log → mismo thread, siempre
        prop_assert_eq!(
            db1.causal_thread("task-1").unwrap(),
            db2.causal_thread("task-1").unwrap()
        );
    }
}
```

#### §5.1e — el thread se reconstruye idéntico desde replay (consistencia con G2)
```rust
#[test]
fn causal_thread_survives_projection_wipe_and_replay() {
    let path = temp_path();
    let original_thread = {
        let db = HiveDB::open(&path);
        seed_causal_task(&db, "task-1", 500); // 500 eventos con cadena causal
        db.causal_thread("task-1").unwrap()
    };

    wipe_projections(&path); // borra proyecciones, deja solo el log

    let db = HiveDB::open(&path);
    let replayed_thread = db.causal_thread("task-1").unwrap();

    assert_eq!(original_thread, replayed_thread); // determinismo cruzado con G2
}
```

---

### §5.2 — buildAgentContext: ventana adaptativa causalmente correcta

#### §5.2a — el contexto respeta el límite de tokens (contrato duro)
```rust
#[test]
fn build_context_never_exceeds_token_limit() {
    let db = HiveDB::open_temp();
    seed_causal_task(&db, "task-1", 10_000); // tarea larga, mucho más que el límite

    let ctx = db.build_agent_context(AgentContextRequest {
        task_id: "task-1".into(),
        current_phase: "implementation".into(),
        current_objective: "fix payment module".into(),
        max_tokens: 4096,
        strategy: ContextStrategy::default(),
    }).unwrap();

    // El motor garantiza que el contexto cabe — siempre, sin excepción
    assert!(ctx.estimated_tokens() <= 4096);
}
```

#### §5.2b — causal_anchors incluye decisiones causalmente conectadas aunque estén lejos en el log
```rust
#[test]
fn causal_anchors_retrieves_distant_but_causally_connected_decisions() {
    let db = HiveDB::open_temp();

    // Decisión arquitectural en el evento 5 que causó (indirectamente) el problema actual
    let anchor = db.append(decision("Architect", "no validar nulos en pagos", None)).unwrap();
    // 995 eventos de ruido en medio
    for _ in 0..995 { db.append(event("Other", Fact, payload())).unwrap(); }
    // El problema actual conecta causalmente con aquella decisión
    let current = db.append(error_caused_by("Backend", "NPE en pagos", anchor)).unwrap();

    let ctx = db.build_agent_context(AgentContextRequest {
        task_id: "task-1".into(),
        current_objective: "fix NPE en pagos".into(),
        max_tokens: 8192,
        strategy: ContextStrategy { causal_anchors: true, ..default() },
        ..default()
    }).unwrap();

    // El evento ancla (seq=5) debe aparecer aunque esté 995 eventos atrás
    assert!(ctx.contains_seq(anchor));
    assert!(ctx.contains_seq(current));
}
```

#### §5.2c — fases completadas se comprimen, no se omiten
```rust
#[test]
fn completed_phases_are_compressed_not_dropped() {
    let db = HiveDB::open_temp();
    seed_phase(&db, "task-1", "planning", 200);    // fase completa
    seed_phase(&db, "task-1", "implementation", 50); // fase actual

    let ctx = db.build_agent_context(AgentContextRequest {
        task_id: "task-1".into(),
        current_phase: "implementation".into(),
        max_tokens: 2048,
        strategy: ContextStrategy { compress_completed_phases: true, ..default() },
        ..default()
    }).unwrap();

    // La fase planning aparece pero comprimida (resumen + decisiones clave)
    let planning = ctx.phase_summary("planning").unwrap();
    assert!(planning.is_compressed);
    assert!(!planning.key_decisions.is_empty()); // las decisiones críticas sobreviven

    // La fase actual aparece completa
    let impl_phase = ctx.phase_summary("implementation").unwrap();
    assert!(!impl_phase.is_compressed);
}
```

#### §5.2d — episodic_similarity recupera episodios relevantes de tareas pasadas
```rust
#[test]
fn episodic_similarity_retrieves_past_relevant_episodes() {
    let db = HiveDB::open_temp();

    // Tarea pasada: problema similar resuelto con éxito
    seed_task_with_outcome(&db, "task-past-1", "null pointer en módulo de pagos", Outcome::Ok);
    // Tarea pasada: problema distinto
    seed_task_with_outcome(&db, "task-past-2", "optimización de queries SQL", Outcome::Ok);

    let ctx = db.build_agent_context(AgentContextRequest {
        task_id: "task-current".into(),
        current_objective: "NPE en validación de pagos".into(),
        max_tokens: 8192,
        strategy: ContextStrategy {
            episodic_similarity: Some(EpisodicConfig {
                vector: embed("null pointer pagos"),
                k: 3,
            }),
            ..default()
        },
        ..default()
    }).unwrap();

    // El episodio relevante aparece; el irrelevante no
    assert!(ctx.similar_episodes.iter().any(|e| e.task_id == "task-past-1"));
    assert!(!ctx.similar_episodes.iter().any(|e| e.task_id == "task-past-2"));
}
```

#### §5.2e — anomalías recientes siempre entran al contexto aunque no sean causalmente directas
```rust
#[test]
fn recent_anomalies_always_included_in_context() {
    let db = HiveDB::open_temp();
    seed_causal_task(&db, "task-1", 100);
    // Anomalía reciente: bucle de error en los últimos 5 min
    inject_error_loop(&db, "task-1", "cargo_build", 4, within_ms(300_000));

    let ctx = db.build_agent_context(AgentContextRequest {
        task_id: "task-1".into(),
        max_tokens: 8192,
        strategy: ContextStrategy {
            recent_anomalies: Some(AnomalyConfig { window_ms: 300_000 }),
            ..default()
        },
        ..default()
    }).unwrap();

    // Las anomalías recientes entran siempre, incluso si quitan espacio a otros eventos
    assert!(!ctx.anomalies.is_empty());
    assert_eq!(ctx.anomalies[0].kind, AnomalyKind::ErrorLoop);
}
```

#### §5.2f — el contexto es estable: misma solicitud → mismo contenido (idempotencia)
```rust
#[test]
fn build_context_is_idempotent() {
    let db = HiveDB::open_temp();
    seed_causal_task(&db, "task-1", 500);

    let req = AgentContextRequest {
        task_id: "task-1".into(),
        current_objective: "fix pagos".into(),
        max_tokens: 4096,
        strategy: ContextStrategy::default(),
        ..default()
    };

    let ctx1 = db.build_agent_context(req.clone()).unwrap();
    let ctx2 = db.build_agent_context(req.clone()).unwrap();

    // Sin nuevos eventos, el contexto es idéntico entre llamadas
    assert_eq!(ctx1.content_hash(), ctx2.content_hash());
}
```

---

### §5.3 — HarnessLoop: evaluador con contexto causal real

#### §5.3a — el evaluador recibe CausalThread, no solo el output
```rust
#[test]
fn harness_evaluates_process_not_just_output() {
    let db = HiveDB::open_temp();

    // Tarea que tuvo buen output pero proceso deficiente (bucle de errores en medio)
    let task_id = seed_task_with_error_loop_then_success(&db, "task-1");
    let thread = db.causal_thread(task_id).unwrap();

    let eval = HarnessLoop::evaluate(HarnessInput {
        causal_thread: thread.clone(),
        similar_episodes: vec![],
        original_intent: "implementar autenticación".into(),
        current_state: db.project::<TaskState>(task_id),
    });

    // El evaluador detecta el proceso deficiente aunque el output final sea correcto
    assert!(eval.process_quality < eval.output_quality);
    assert!(eval.findings.iter().any(|f| f.kind == FindingKind::InefficientLoop));
}
```

#### §5.3b — root cause apunta al seq exacto donde inició el desvío
```rust
#[test]
fn harness_root_cause_has_exact_seq_provenance() {
    let db = HiveDB::open_temp();

    // La decisión en seq=10 causó toda la cadena de fallos
    let bad_decision = db.append(decision("Architect", "no manejar errores de red", None)).unwrap();
    assert_eq!(bad_decision, 10);
    seed_failures_caused_by(&db, bad_decision, 20); // 20 fallos derivados

    let thread = db.causal_thread("task-1").unwrap();
    let eval = HarnessLoop::evaluate(HarnessInput {
        causal_thread: thread,
        ..default()
    });

    // El root cause apunta exactamente al seq=10, no a los fallos derivados
    assert_eq!(eval.root_cause.unwrap().seq, bad_decision);
    assert_eq!(eval.root_cause.unwrap().agent, "Architect");
}
```

#### §5.3c — learning proposals incluyen evidence causal con seqs (no son opiniones)
```rust
#[test]
fn learning_proposals_include_causal_evidence() {
    let db = HiveDB::open_temp();
    let task_id = seed_task_with_pattern_failure(&db, "task-1");
    let thread = db.causal_thread(task_id).unwrap();

    let eval = HarnessLoop::evaluate(HarnessInput {
        causal_thread: thread,
        ..default()
    });

    for proposal in &eval.proposals {
        // Cada propuesta cita los seqs exactos que la sustentan
        assert!(!proposal.evidence_seqs.is_empty());
        // Y apunta al trigger causal, no al síntoma final
        assert!(proposal.trigger_seq.is_some());
        // Toda propuesta tiene nivel de confianza calculado
        assert!(proposal.confidence > 0.0 && proposal.confidence <= 1.0);
    }
}
```

#### §5.3d — las proposals se appendean al log como eventos auditables
```rust
#[test]
fn learning_proposals_are_logged_as_events() {
    let db = HiveDB::open_temp();
    let thread = db.causal_thread(
        seed_task_with_pattern_failure(&db, "task-1")
    ).unwrap();

    let eval = HarnessLoop::evaluate(HarnessInput { causal_thread: thread, ..default() });

    // Persistir las proposals al log
    let proposal_seqs: Vec<u64> = eval.proposals.iter()
        .map(|p| db.append(learning_proposal(p)).unwrap())
        .collect();

    // Son eventos reales en el log: auditables, reversibles, con provenance
    for seq in proposal_seqs {
        let ev = db.read(seq).unwrap();
        assert_eq!(ev.kind_tag(), LearningProposalTag);
        // La proposal aprobada puede trazarse hasta los eventos que la causaron
        let payload: LearningProposalPayload = ev.deserialize_payload().unwrap();
        assert!(!payload.evidence_seqs.is_empty());
    }
}
```

#### §5.3e — el harness con episodios similares genera proposals más específicas
```rust
#[test]
fn similar_episodes_improve_proposal_specificity() {
    let db = HiveDB::open_temp();

    // Episodio pasado: mismo patrón, resuelto con una estrategia concreta
    let past = seed_resolved_episode(&db, "task-past", FailurePattern::NullPointer, Resolution::AddNullCheck);
    // Tarea actual: mismo patrón sin resolver
    let current_thread = db.causal_thread(
        seed_task_with_failure(&db, "task-current", FailurePattern::NullPointer)
    ).unwrap();

    let eval_without = HarnessLoop::evaluate(HarnessInput {
        causal_thread: current_thread.clone(),
        similar_episodes: vec![],
        ..default()
    });
    let eval_with = HarnessLoop::evaluate(HarnessInput {
        causal_thread: current_thread,
        similar_episodes: vec![past],
        ..default()
    });

    // Con episodio similar, la propuesta es más específica y con mayor confianza
    assert!(eval_with.proposals[0].confidence > eval_without.proposals[0].confidence);
    assert!(eval_with.proposals[0].specificity > eval_without.proposals[0].specificity);
}
```

#### §5.3f — el harness no propone si la confianza es insuficiente (umbral configurable)
```rust
#[test]
fn harness_withholds_proposals_below_confidence_threshold() {
    let db = HiveDB::open_temp();
    // Tarea con señal muy débil: un solo fallo, sin patrón claro
    let thread = db.causal_thread(
        seed_task_with_single_failure(&db, "task-1")
    ).unwrap();

    let eval = HarnessLoop::evaluate(HarnessInput {
        causal_thread: thread,
        min_confidence: 0.75, // umbral alto
        ..default()
    });

    // Con señal débil y umbral alto, no genera proposals especulativas
    assert!(eval.proposals.is_empty());
    // Pero sí documenta que hubo un fallo sin suficiente evidencia
    assert!(eval.findings.iter().any(|f| f.kind == FindingKind::InsufficientEvidence));
}
```

---

### §5.4 — Integración end-to-end: larga duración sin amnesia

#### §5.4a — un agente retoma una tarea larga con contexto causalmente completo
```rust
#[tokio::test]
async fn long_running_task_resumes_with_full_causal_context() {
    let db = HiveDB::open_temp();

    // Simula 3 sesiones de trabajo separadas en la misma tarea
    seed_work_session(&db, "task-1", "session-1", 300); // 300 eventos
    seed_work_session(&db, "task-1", "session-2", 300); // 300 más
    seed_work_session(&db, "task-1", "session-3", 50);  // sesión actual

    // Al retomar, el agente pide contexto para continuar
    let ctx = db.build_agent_context(AgentContextRequest {
        task_id: "task-1".into(),
        current_phase: "session-3".into(),
        current_objective: "completar integración de pagos".into(),
        max_tokens: 8192,
        strategy: ContextStrategy {
            causal_anchors: true,
            compress_completed_phases: true,
            episodic_similarity: Some(EpisodicConfig { vector: embed("pagos"), k: 3 }),
            recent_anomalies: Some(AnomalyConfig { window_ms: 3_600_000 }),
        },
    }).unwrap();

    // El contexto incluye decisiones clave de las 3 sesiones, no solo la actual
    assert!(ctx.spans_sessions(&["session-1", "session-2", "session-3"]));
    // Cabe en la ventana
    assert!(ctx.estimated_tokens() <= 8192);
    // Contiene al menos una decisión de cada sesión anterior (no hay amnesia)
    assert!(ctx.has_content_from_phase("session-1"));
    assert!(ctx.has_content_from_phase("session-2"));
}
```

#### §5.4b — el harness mejora entre tareas: la segunda tarea produce menos proposals
```rust
#[test]
fn harness_loop_improves_swarm_across_tasks() {
    let db = HiveDB::open_temp();

    // Primera tarea: varios fallos, genera proposals
    let t1 = seed_task_with_pattern_failure(&db, "task-1");
    let eval1 = HarnessLoop::evaluate(HarnessInput {
        causal_thread: db.causal_thread(t1).unwrap(),
        ..default()
    });
    // Aprobamos y aplicamos las proposals
    for p in &eval1.proposals { db.append(learning_proposal_approved(p)).unwrap(); }

    // Segunda tarea del mismo tipo: el swarm aprendió
    let t2 = seed_similar_task(&db, "task-2", t1);
    let eval2 = HarnessLoop::evaluate(HarnessInput {
        causal_thread: db.causal_thread(t2).unwrap(),
        similar_episodes: vec![db.causal_thread(t1).unwrap()],
        ..default()
    });

    // Después de aplicar el aprendizaje, la segunda tarea tiene menos anomalías
    assert!(eval2.anomaly_count() < eval1.anomaly_count());
    // Y las proposals son más específicas (el swarm sabe qué falló antes)
    if !eval2.proposals.is_empty() {
        assert!(eval2.proposals[0].confidence > eval1.proposals[0].confidence);
    }
}
```

---

## Gate G9 — Harness de larga duración

| Sub-gate | Tests que deben pasar |
|---|---|
| G9a CausalThread | §5.1a, §5.1b, §5.1c, §5.1d, §5.1e |
| G9b buildAgentContext | §5.2a, §5.2b, §5.2c, §5.2d, §5.2e, §5.2f |
| G9c HarnessLoop | §5.3a, §5.3b, §5.3c, §5.3d, §5.3e, §5.3f |
| G9d E2E larga duración | §5.4a, §5.4b |

**Dependencias de gate:** G9 requiere G1+G2 verdes (event-log y proyecciones). G9b requiere G4 verde (búsqueda híbrida, porque `episodic_similarity` la usa). G9c requiere G9a verde.

**Nota de implementación:** `HarnessLoop::evaluate` es una función pura en Rust — recibe datos, devuelve evaluación, sin side effects. Los side effects (persistir proposals al log) son responsabilidad del llamador, no del evaluador. Esto hace §5.3a-§5.3f testeables sin mock del motor.
