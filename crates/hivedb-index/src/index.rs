use crate::hnsw::VectorIndex;
use crate::rrf::rrf;
use crate::text::{TextIndex, filter_token};
use crate::types::{Fusion, Hit, HybridQuery, IndexDoc, ScalarFilter};
use std::collections::HashMap;
use std::path::Path;

/// Combined semantic index: full-text (BM25) + vector (ANN) + RRF fusion.
///
/// Score semantics: text-only queries return raw BM25 scores, vector-only
/// queries return cosine similarity, and hybrid queries return RRF-fused
/// scores (with the raw per-source scores exposed on each [`Hit`]).
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

    /// Create a semantic index held entirely in memory (no files on disk).
    pub fn open_in_ram(vector_dimension: usize) -> crate::Result<Self> {
        Ok(Self {
            text: TextIndex::open_in_ram()?,
            vector: VectorIndex::new(vector_dimension),
            vector_dimension,
        })
    }

    /// Insert or replace a document. Documents without a vector never touch
    /// the vector index; an upsert that drops the vector also removes the
    /// previously stored one.
    pub fn upsert(&self, doc: &IndexDoc) -> crate::Result<()> {
        self.text.upsert(doc)?;
        self.sync_vector(doc)
    }

    /// Insert or replace a batch of documents. The text side commits once for
    /// the whole batch.
    pub fn upsert_batch(&self, docs: &[IndexDoc]) -> crate::Result<()> {
        self.text.upsert_batch(docs)?;
        for doc in docs {
            self.sync_vector(doc)?;
        }
        Ok(())
    }

    fn sync_vector(&self, doc: &IndexDoc) -> crate::Result<()> {
        match &doc.vector {
            Some(vector) => self.vector.insert(doc.id.clone(), vector.clone()),
            None => self.vector.delete(&doc.id),
        }
    }

    /// Delete a document from both indexes. Missing ids are a no-op.
    pub fn delete(&self, id: &str) -> crate::Result<()> {
        self.text.delete_doc(id)?;
        self.vector.delete(id)
    }

    /// Delete every document carrying the given scalar filter, from both
    /// indexes.
    pub fn delete_by_filter(&self, filter: &ScalarFilter) -> crate::Result<()> {
        for id in self.text.ids_by_filter(filter)? {
            self.vector.delete(&id)?;
        }
        self.text.delete_by_filter(filter)
    }

    /// Remove every document from both indexes.
    pub fn clear(&self) -> crate::Result<()> {
        self.text.clear()?;
        self.vector.clear()
    }

    /// Execute a hybrid query over text and/or vector rankings.
    pub fn query_hybrid(&self, query: HybridQuery) -> crate::Result<Vec<Hit>> {
        let boosts = query.boosts.unwrap_or_default();

        let text_ranking = match &query.text {
            Some(text) => Some(self.text.search(text, &query.filters, boosts, query.k)?),
            None => None,
        };

        let vector_ranking = match &query.vector {
            Some(vector) => Some(self.search_vector_filtered(vector, &query.filters, query.k)?),
            None => None,
        };

        let hits = match (text_ranking, vector_ranking) {
            // Single-source queries return raw scores from that source.
            (Some(t), None) => t
                .into_iter()
                .map(|(id, _rank, score)| Hit {
                    id,
                    score,
                    text_score: Some(score),
                    vector_score: None,
                })
                .collect(),
            (None, Some(v)) => v
                .into_iter()
                .map(|(id, _rank, score)| Hit {
                    id,
                    score,
                    text_score: None,
                    vector_score: Some(score),
                })
                .collect(),
            (Some(t), Some(v)) => {
                let text_scores: HashMap<&str, f32> =
                    t.iter().map(|(id, _, s)| (id.as_str(), *s)).collect();
                let vector_scores: HashMap<&str, f32> =
                    v.iter().map(|(id, _, s)| (id.as_str(), *s)).collect();

                let rankings: Vec<Vec<(String, usize)>> = vec![
                    t.iter().map(|(id, rank, _)| (id.clone(), *rank)).collect(),
                    v.iter().map(|(id, rank, _)| (id.clone(), *rank)).collect(),
                ];
                let Fusion::Rrf { k: fusion_k } = query.fusion;

                rrf(&rankings, fusion_k)
                    .into_iter()
                    .take(query.k)
                    .map(|(id, score)| {
                        let text_score = text_scores.get(id.as_str()).copied();
                        let vector_score = vector_scores.get(id.as_str()).copied();
                        Hit {
                            id,
                            score,
                            text_score,
                            vector_score,
                        }
                    })
                    .collect()
            }
            (None, None) => Vec::new(),
        };
        Ok(hits)
    }

    /// Vector search with scalar filters applied as a post-filter: candidates
    /// are oversampled from the graph, then kept only if their stored filter
    /// tokens contain every requested filter. Heavy filtering over a large
    /// corpus degrades recall; push selective filters to the text side when
    /// possible.
    fn search_vector_filtered(
        &self,
        vector: &[f32],
        filters: &[ScalarFilter],
        k: usize,
    ) -> crate::Result<Vec<(String, usize, f32)>> {
        if filters.is_empty() {
            return self.vector.search(vector, k);
        }

        let required: Vec<String> = filters.iter().map(filter_token).collect();
        let candidates = self.vector.search(vector, (k * 4).max(k))?;

        let mut results = Vec::with_capacity(k);
        for (id, _rank, score) in candidates {
            let tokens = self.text.stored_filter_tokens(&id)?.unwrap_or_default();
            if required.iter().all(|r| tokens.contains(r)) {
                results.push((id, results.len() + 1, score));
                if results.len() == k {
                    break;
                }
            }
        }
        Ok(results)
    }

    /// Returns the configured vector dimension.
    pub fn vector_dimension(&self) -> usize {
        self.vector_dimension
    }
}
