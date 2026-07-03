use hivedb_index::{HybridQuery, ScalarFilter, SemanticIndex};

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
fn hybrid_query_fuses_text_and_vector() {
    let dir = tempfile::tempdir().unwrap();
    let index = SemanticIndex::open(dir.path(), 384).unwrap();

    index
        .index_doc(
            "doc1",
            "error de compilación en pagos",
            &embed("pago fallido"),
            &[],
        )
        .unwrap();
    index
        .index_doc(
            "doc2",
            "documentación de la API de envíos",
            &embed("logística"),
            &[],
        )
        .unwrap();

    let hits = index
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
fn scalar_filters_pushed_into_index() {
    let dir = tempfile::tempdir().unwrap();
    let index = SemanticIndex::open(dir.path(), 384).unwrap();

    index
        .index_doc(
            "d1",
            "deploy fallido",
            &embed("x"),
            &[ScalarFilter::eq("agent", "Backend")],
        )
        .unwrap();
    index
        .index_doc(
            "d2",
            "deploy fallido",
            &embed("x"),
            &[ScalarFilter::eq("agent", "Frontend")],
        )
        .unwrap();

    let hits = index
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

#[test]
fn vector_index_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();

    {
        let index = SemanticIndex::open(dir.path(), 384).unwrap();
        index
            .index_doc("doc1", "torre eiffel en parís", &embed("pago"), &[])
            .unwrap();
        index
            .index_doc("doc2", "sushi en tokio", &embed("logística"), &[])
            .unwrap();
    }

    // Reopen from disk: the ANN index must be rebuilt from persisted vectors.
    let index = SemanticIndex::open(dir.path(), 384).unwrap();
    let hits = index
        .query_hybrid(HybridQuery::default().with_vector(embed("pago")).with_k(1))
        .unwrap();

    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, "doc1");
}

#[test]
fn scalar_filters_work_on_arbitrary_fields() {
    let dir = tempfile::tempdir().unwrap();
    let index = SemanticIndex::open(dir.path(), 384).unwrap();

    index
        .index_doc(
            "d1",
            "guía de viaje",
            &embed("x"),
            &[ScalarFilter::eq("city", "tokyo")],
        )
        .unwrap();
    index
        .index_doc(
            "d2",
            "guía de viaje",
            &embed("x"),
            &[ScalarFilter::eq("city", "paris")],
        )
        .unwrap();

    let hits = index
        .query_hybrid(
            HybridQuery::default()
                .with_text("guía")
                .with_filters(vec![ScalarFilter::eq("city", "paris")])
                .with_k(10),
        )
        .unwrap();

    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, "d2");
}

#[test]
fn hit_has_positive_score() {
    let dir = tempfile::tempdir().unwrap();
    let index = SemanticIndex::open(dir.path(), 384).unwrap();

    index
        .index_doc("doc1", "hello world", &embed("x"), &[])
        .unwrap();

    let hits = index
        .query_hybrid(HybridQuery::default().with_text("hello").with_k(1))
        .unwrap();

    assert_eq!(hits.len(), 1);
    assert!(hits[0].score > 0.0);
}
