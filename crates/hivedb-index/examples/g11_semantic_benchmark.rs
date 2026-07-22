use hivedb_index::{HybridQuery, IndexDoc, ScalarFilter, SemanticIndex, VectorConfig};
use std::time::Instant;

const DOCUMENTS: usize = 10_000;
const DIMENSION: usize = 384;

fn vector(seed: usize) -> Vec<f32> {
    let mut vector = vec![0.0; DIMENSION];
    vector[seed % DIMENSION] = 1.0;
    vector[(seed.wrapping_mul(31) + 7) % DIMENSION] += 0.25;
    vector
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let config = Some(VectorConfig::new(DIMENSION, "benchmark:384"));
    let index = SemanticIndex::open(directory.path(), config.clone())?;
    let documents: Vec<IndexDoc> = (0..DOCUMENTS)
        .map(|id| {
            IndexDoc::new(format!("doc-{id}"))
                .with_body(format!("documento semántico número {id}"))
                .with_vector(vector(id))
                .with_filters(vec![ScalarFilter::eq("tenant", format!("t{}", id % 10))])
        })
        .collect();

    let started = Instant::now();
    index.upsert_batch(&documents)?;
    println!("ingest_10k_ms={}", started.elapsed().as_millis());

    let query = vector(42);
    let started = Instant::now();
    let hits = index.query_hybrid(HybridQuery::default().with_vector(query.clone()).with_k(10))?;
    println!(
        "hnsw_query_us={} hits={}",
        started.elapsed().as_micros(),
        hits.len()
    );

    let started = Instant::now();
    let filtered = index.query_hybrid(
        HybridQuery::default()
            .with_vector(query)
            .with_filters(vec![ScalarFilter::eq("tenant", "t2")])
            .with_k(10),
    )?;
    println!(
        "filtered_exact_us={} hits={}",
        started.elapsed().as_micros(),
        filtered.len()
    );

    let started = Instant::now();
    index.compact()?;
    println!("compact_ms={}", started.elapsed().as_millis());
    drop(index);

    let started = Instant::now();
    let reopened = SemanticIndex::open(directory.path(), config)?;
    println!(
        "reopen_ms={} live_vectors={}",
        started.elapsed().as_millis(),
        reopened.vector_stats().map_or(0, |stats| stats.0)
    );
    Ok(())
}
