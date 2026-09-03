//! G12 — aislamiento entre inquilinos y apertura perezosa.
//!
//! Una sola base compartida por muchos enjambres cambia dos premisas que el
//! motor daba por buenas cuando cada dueño tenía la suya:
//!
//!  1. Las lecturas que recorren "todos los shards" —`causal_thread`,
//!     `tool_stats` y cualquier proyección con scope `Agent`— dejan de ser una
//!     vista completa y pasan a ser una fuga: devuelven el hilo y las
//!     estadísticas de enjambres ajenos. De ahí las variantes acotadas por
//!     agente, que estos tests fijan como contrato.
//!  2. Abrir la base costaba O(eventos totales) porque se abrían y escaneaban
//!     todos los shards. Con miles de agentes eso es inviable, así que la
//!     apertura pasó a ser perezosa; aquí se comprueba que sigue siendo
//!     correcta en los tres casos que importan: cierre limpio, cierre sucio, y
//!     consulta de un agente que no existe.

mod common;

use hivedb_core::{AgentId, EventInput, EventKind, HiveDB, StreamId};
use serde_json::json;
use tempfile::tempdir;

/// Dos enjambres distintos escribiendo en un stream con el MISMO nombre. Es el
/// caso realista: los ids de stream los elige la aplicación ("task-1"), no son
/// únicos globalmente.
const STREAM: &str = "task-1";

fn agentes(enjambre: &str) -> Vec<AgentId> {
    vec![
        AgentId::from(format!("{enjambre}:coordinator")),
        AgentId::from(format!("{enjambre}:native:search")),
    ]
}

fn tool_call(agente: &str, tool: &str, latencia: u64) -> EventInput {
    EventInput::new(
        agente,
        StreamId::from(STREAM),
        EventKind::ToolCall {
            tool: tool.to_string(),
        },
    )
    .with_payload(json!({"latency_ms": latencia, "cost": 1.0, "outcome": "Ok"}))
}

fn hecho(agente: &str) -> EventInput {
    EventInput::new(agente, StreamId::from(STREAM), EventKind::Fact)
        .with_payload(json!({"msg": "x"}))
}

/// Dos enjambres con eventos en el mismo stream, sobre la misma base.
fn base_con_dos_enjambres() -> HiveDB {
    let db = common::db();
    for latencia in [10, 20] {
        db.append(tool_call("swarmA:native:search", "buscar", latencia))
            .unwrap();
    }
    db.append(tool_call("swarmB:native:search", "buscar", 100))
        .unwrap();
    db
}

#[test]
fn causal_thread_acotado_no_ve_otros_enjambres() {
    let db = base_con_dos_enjambres();

    let solo_a = db
        .causal_thread_for_agents(STREAM, &agentes("swarmA"))
        .unwrap();
    let solo_b = db
        .causal_thread_for_agents(STREAM, &agentes("swarmB"))
        .unwrap();

    assert_eq!(
        solo_a.tool_calls.len(),
        2,
        "A debe ver sólo sus dos eventos"
    );
    assert_eq!(solo_b.tool_calls.len(), 1, "B debe ver sólo el suyo");
}

#[test]
fn causal_thread_sin_acotar_sigue_viendo_toda_la_base() {
    // El comportamiento histórico se conserva a propósito: en una base de un
    // solo dueño es el correcto. Lo que cambia es que ahora hay una alternativa
    // para cuando no lo es.
    let db = base_con_dos_enjambres();
    assert_eq!(db.causal_thread(STREAM).unwrap().tool_calls.len(), 3);
}

#[test]
fn tool_stats_acotado_no_suma_llamadas_ajenas() {
    let db = common::db();
    db.append(tool_call("swarmA:native:search", "buscar", 10))
        .unwrap();
    db.append(tool_call("swarmA:native:search", "buscar", 20))
        .unwrap();
    db.append(tool_call("swarmB:native:search", "buscar", 100))
        .unwrap();

    let a = db
        .tool_stats_for_agents("buscar", &agentes("swarmA"))
        .unwrap()
        .expect("A tiene llamadas");
    let b = db
        .tool_stats_for_agents("buscar", &agentes("swarmB"))
        .unwrap()
        .expect("B tiene llamadas");

    assert_eq!(a.invocations, 2);
    assert_eq!(a.total_latency_ms, 30);
    assert_eq!(b.invocations, 1);
    assert_eq!(b.total_latency_ms, 100);

    // Y la variante sin acotar sí mezcla: es exactamente la fuga que las
    // variantes acotadas existen para evitar.
    let todas = db.tool_stats("buscar").unwrap().unwrap();
    assert_eq!(todas.invocations, 3);
}

#[test]
fn tool_stats_de_un_enjambre_sin_llamadas_no_hereda_las_del_vecino() {
    let db = common::db();
    db.append(tool_call("swarmA:native:search", "buscar", 10))
        .unwrap();

    let b = db
        .tool_stats_for_agents("buscar", &agentes("swarmB"))
        .unwrap();
    assert!(b.is_none(), "B no ha llamado a la herramienta");
}

#[test]
fn reabrir_tras_cierre_limpio_conserva_la_secuencia() {
    let dir = tempdir().unwrap();
    let ultimo = {
        let db = HiveDB::open(dir.path()).unwrap();
        for _ in 0..5 {
            db.append(hecho("swarmA:coordinator")).unwrap();
        }
        db.append(hecho("swarmB:coordinator")).unwrap()
    };
    assert_eq!(ultimo, 6);

    // Reabrir no escanea nada: el contador viene de la meta del shard global.
    let db = HiveDB::open(dir.path()).unwrap();
    assert_eq!(db.append(hecho("swarmA:coordinator")).unwrap(), 7);
}

#[test]
fn reabrir_tras_cierre_sucio_redescubre_la_secuencia() {
    let dir = tempdir().unwrap();
    {
        let db = HiveDB::open(dir.path()).unwrap();
        for _ in 0..4 {
            db.append(hecho("swarmA:coordinator")).unwrap();
        }
    }

    // Simula una caída: la marca de cierre ordenado se borra y el contador
    // queda atrasado, como cuando el proceso muere sin pasar por Drop.
    let global = dir.path().join("shards/_global.redb");
    {
        let redb = redb::Database::create(&global).unwrap();
        let txn = redb.begin_write().unwrap();
        {
            let mut table = txn
                .open_table(redb::TableDefinition::<&str, u64>::new("meta"))
                .unwrap();
            table.insert("clean_shutdown", 0u64).unwrap();
            table.insert("next_seq", 1u64).unwrap();
        }
        txn.commit().unwrap();
    }

    // El escaneo de recuperación tiene que encontrar el seq 4 y continuar en 5,
    // no reescribir encima de eventos existentes.
    let db = HiveDB::open(dir.path()).unwrap();
    assert_eq!(db.append(hecho("swarmA:coordinator")).unwrap(), 5);
}

#[test]
fn las_proyecciones_de_un_shard_se_recuperan_al_abrirlo() {
    let dir = tempdir().unwrap();
    {
        let db = HiveDB::open(dir.path()).unwrap();
        db.append(tool_call("swarmA:native:search", "buscar", 10))
            .unwrap();
        db.append(tool_call("swarmA:native:search", "buscar", 20))
            .unwrap();
    }

    // Tras reabrir, el shard de ese agente no está abierto todavía. Pedir sus
    // estadísticas debe abrirlo y recuperar su proyección, no devolver cero.
    let db = HiveDB::open(dir.path()).unwrap();
    let stats = db
        .tool_stats_for_agents("buscar", &agentes("swarmA"))
        .unwrap()
        .expect("la proyección del shard se recupera al abrirlo");
    assert_eq!(stats.invocations, 2);
    assert_eq!(stats.total_latency_ms, 30);
}

#[test]
fn consultar_un_agente_inexistente_no_crea_su_shard() {
    let dir = tempdir().unwrap();
    let db = HiveDB::open(dir.path()).unwrap();
    db.append(hecho("swarmA:coordinator")).unwrap();

    let vacio = db
        .causal_thread_for_agents(STREAM, &[AgentId::from("swarmZ:fantasma")])
        .unwrap();
    assert_eq!(vacio.tool_calls.len(), 0);

    // Una lectura no debe dejar ficheros detrás: abrir un shard lo crea, así
    // que consultar agentes que no existen engordaría el directorio (y el censo
    // de shards) sin que nadie haya escrito nunca.
    let fantasma = dir.path().join("shards/swarmZ:fantasma.redb");
    assert!(
        !fantasma.exists(),
        "no debe crearse el shard de un agente que no existe"
    );
}
