use crate::hnsw::VectorIndex;
use crate::rrf::rrf;
use crate::text::TextIndex;
use crate::types::{Fusion, Hit, HybridQuery, ScalarFilter};
use std::path::Path;

/// Combined semantic index: full-text (BM25) + vector (ANN) + RRF fusion.
pub struct SemanticIndex {
    text: TextIndex,
    vector: VectorIndex,
    vector_dimension: usize,
}

impl SemanticIndex {
    /// Open or create a semantic index under the given base directory.
    /// The base directory will contain `fts/` and `vec/` subdirectories.
    pub fn open<P: AsRef<Path>>(base_dir: P, vector_dimension: usize) -> crate::Result<Self> {
        let base = base_dir.as_ref();
        let fts_dir = base.join("fts");
        let vec_dir = base.join("vec");
        std::fs::create_dir_all(&fts_dir)?;
        std::fs::create_dir_all(&vec_dir)?;

        let text = TextIndex::open(fts_dir)?;
        let vector = VectorIndex::open(vec_dir, vector_dimension)?;

        Ok(Self {
            text,
            vector,
            vector_dimension,
        })
    }

    /// Index a document with text and vector. Filters are pushed to the text
    /// index only (the vector index does not support scalar filters).
    pub fn index_doc(
        &self,
        id: &str,
        text: &str,
        vector: &[f32],
        filters: &[ScalarFilter],
    ) -> crate::Result<()> {
        self.text.index_doc(id, text, filters)?;
        self.vector.insert(id.to_string(), vector.to_vec())?;
        Ok(())
    }

    /// Execute a hybrid query over text and/or vector rankings.
    pub fn query_hybrid(&self, query: HybridQuery) -> crate::Result<Vec<Hit>> {
        let text_ranking = match &query.text {
            Some(text) => Some(self.text.search(text, &query.filters, query.k)?),
            None => None,
        };

        let vector_ranking = match &query.vector {
            Some(vector) => Some(self.vector.search(vector, query.k)?),
            None => None,
        };

        let rankings: Vec<Vec<(String, usize)>> = match (text_ranking, vector_ranking) {
            (Some(t), Some(v)) => vec![t, v],
            (Some(t), None) => vec![t],
            (None, Some(v)) => vec![v],
            (None, None) => return Ok(Vec::new()),
        };

        let fusion_k = match query.fusion {
            Fusion::Rrf { k } => k,
            Fusion::WeightedSum => 60, // fallback for unsupported strategy
        };

        let fused = rrf(&rankings, fusion_k);
        let hits: Vec<Hit> = fused
            .into_iter()
            .take(query.k)
            .map(|(id, score)| Hit { id, score })
            .collect();
        Ok(hits)
    }

    /// Returns the configured vector dimension.
    pub fn vector_dimension(&self) -> usize {
        self.vector_dimension
    }
}
