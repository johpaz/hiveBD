use hivedb_core::{HiveDB, HybridQuery, IndexDoc, OpenOptions, VectorOptions};
use std::sync::{Arc, Barrier};

fn vector(coordinate: usize) -> Vec<f32> {
    let mut vector = vec![0.0; 8];
    vector[coordinate] = 1.0;
    vector
}

#[test]
fn concurrent_upserts_never_mix_text_and_vector_generations() {
    let db = Arc::new(
        HiveDB::open_temp_with_options(OpenOptions {
            vector: Some(VectorOptions::new(8, "test:8")),
        })
        .unwrap(),
    );
    let barrier = Arc::new(Barrier::new(3));
    let mut workers = Vec::new();
    for (body, coordinate) in [("alpha-unique", 0), ("beta-unique", 1)] {
        let db = db.clone();
        let barrier = barrier.clone();
        workers.push(std::thread::spawn(move || {
            barrier.wait();
            for _ in 0..25 {
                db.upsert_doc(
                    &IndexDoc::new("shared")
                        .with_body(body)
                        .with_vector(vector(coordinate)),
                )
                .unwrap();
            }
        }));
    }
    barrier.wait();
    for worker in workers {
        worker.join().unwrap();
    }

    let alpha_text = db
        .query_hybrid(HybridQuery::default().with_text("alpha-unique").with_k(1))
        .unwrap();
    let alpha_vector = db
        .query_hybrid(HybridQuery::default().with_vector(vector(0)).with_k(1))
        .unwrap();
    let is_alpha = !alpha_text.is_empty();
    let vector_is_alpha = alpha_vector[0].score > 0.99;
    assert_eq!(is_alpha, vector_is_alpha);
}
