mod common;

use common::db;
use hivedb_core::{HybridQuery, ScalarFilter};

fn embed(text: &str) -> Vec<f32> {
    let mut v = vec![0.0; 384];
    match text {
        "pago fallido" | "pago" => {
            v[0] = 1.0;
            v[1] = 1.0;
        }
        "transacción rechazada" | "transacción" => {
            v[0] = 1.0;
            v[2] = 1.0;
        }
        "logística" => {
            v[10] = 1.0;
        }
        "x" => {
            v[5] = 1.0;
        }
        _ => {
            v[100] = 1.0;
        }
    }
    v
}

#[test]
fn hive_db_hybrid_query_fuses_text_and_vector() {
    let db = db();

    db.index_doc(
        "doc1",
        "error de compilación en pagos",
        embed("pago fallido"),
    )
    .unwrap();
    db.index_doc(
        "doc2",
        "documentación de la API de envíos",
        embed("logística"),
    )
    .unwrap();

    let hits = db
        .query_hybrid(
            HybridQuery::default()
                .with_text("error pagos")
                .with_vector(embed("transacción rechazada"))
                .with_k(5),
        )
        .unwrap();

    assert!(!hits.is_empty());
    assert_eq!(hits[0].id, "doc1");
}

#[test]
fn hive_db_scalar_filters_pushed_into_index() {
    let db = db();

    db.index_doc_with(
        "d1",
        "deploy fallido",
        embed("x"),
        &[ScalarFilter::eq("agent", "Backend")],
    )
    .unwrap();
    db.index_doc_with(
        "d2",
        "deploy fallido",
        embed("x"),
        &[ScalarFilter::eq("agent", "Frontend")],
    )
    .unwrap();

    let hits = db
        .query_hybrid(
            HybridQuery::default()
                .with_text("deploy")
                .with_filters(vec![ScalarFilter::eq("agent", "Backend")])
                .with_k(10),
        )
        .unwrap();

    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, "d1");
}
