/// A single hybrid search result.
#[derive(Clone, Debug, PartialEq)]
pub struct Hit {
    pub id: String,
    pub score: f32,
}

/// Fusion strategy for combining multiple rankings.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Fusion {
    /// Reciprocal Rank Fusion with the given `k` parameter.
    /// When constructed via `Default`, `k = 60`.
    Rrf { k: usize },
    /// Weighted sum of normalized scores (not implemented in G4).
    WeightedSum,
}

impl Default for Fusion {
    fn default() -> Self {
        Fusion::Rrf { k: 60 }
    }
}

/// Scalar filter pushed down to the underlying index.
#[derive(Clone, Debug, PartialEq)]
pub enum ScalarFilter {
    /// Equality filter on a string field.
    Eq { field: String, value: String },
}

impl ScalarFilter {
    pub fn eq(field: impl Into<String>, value: impl Into<String>) -> Self {
        ScalarFilter::Eq {
            field: field.into(),
            value: value.into(),
        }
    }
}

/// Hybrid query over text and/or vector rankings.
#[derive(Clone, Debug, Default)]
pub struct HybridQuery {
    /// Full-text query passed to `tantivy` BM25.
    pub text: Option<String>,
    /// Vector query passed to `hnsw_rs` ANN.
    pub vector: Option<Vec<f32>>,
    /// Scalar filters pushed to the text index.
    pub filters: Vec<ScalarFilter>,
    /// Maximum number of hits to return.
    pub k: usize,
    /// Fusion strategy.
    pub fusion: Fusion,
}

impl HybridQuery {
    pub fn with_text(mut self, text: impl Into<String>) -> Self {
        self.text = Some(text.into());
        self
    }

    pub fn with_vector(mut self, vector: Vec<f32>) -> Self {
        self.vector = Some(vector);
        self
    }

    pub fn with_filters(mut self, filters: Vec<ScalarFilter>) -> Self {
        self.filters = filters;
        self
    }

    pub fn with_k(mut self, k: usize) -> Self {
        self.k = k;
        self
    }
}
