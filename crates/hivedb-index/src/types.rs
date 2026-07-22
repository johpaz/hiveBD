/// A single hybrid search result.
///
/// `score` semantics depend on the query mode:
/// - text-only: raw BM25 score (positive, higher is better).
/// - vector-only: cosine similarity in `[0, 1]` (higher is better).
/// - hybrid (text + vector): RRF-fused score.
///
/// `text_score` / `vector_score` carry the raw per-source scores when that
/// source participated in the query.
#[derive(Clone, Debug, PartialEq)]
pub struct Hit {
    pub id: String,
    pub score: f32,
    pub text_score: Option<f32>,
    pub vector_score: Option<f32>,
}

/// Fusion strategy for combining multiple rankings.
///
/// Fusion only applies when both text and vector rankings are present; single
/// source queries return raw scores from that source.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Fusion {
    /// Reciprocal Rank Fusion with the given `k` parameter.
    /// When constructed via `Default`, `k = 60`.
    Rrf { k: usize },
}

impl Default for Fusion {
    fn default() -> Self {
        Fusion::Rrf { k: 60 }
    }
}

/// Scalar filter pushed down to the underlying index.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
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

/// Per-field BM25 boosts applied when parsing text queries.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FieldBoosts {
    pub name: f32,
    pub body: f32,
    pub tags: f32,
}

impl Default for FieldBoosts {
    fn default() -> Self {
        FieldBoosts {
            name: 4.0,
            body: 2.0,
            tags: 3.0,
        }
    }
}

/// A document to be indexed for hybrid search.
///
/// All text slots are optional; a document with no text fields is still
/// registered (id + filters) so it can be filtered and deleted. `vector` is
/// optional: text-only documents never touch the vector index.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct IndexDoc {
    pub id: String,
    /// Short, high-signal title (boosted highest by default).
    pub name: Option<String>,
    /// Main text content.
    pub body: Option<String>,
    /// Categories, triggers, keywords.
    pub tags: Option<String>,
    /// Optional embedding; must match the index dimension when present.
    pub vector: Option<Vec<f32>>,
    /// Scalar filters attached to the document.
    pub filters: Vec<ScalarFilter>,
}

/// Identidad inmutable del espacio de embeddings usado por una base.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct VectorConfig {
    pub dimension: usize,
    pub space_id: String,
}

impl VectorConfig {
    pub fn new(dimension: usize, space_id: impl Into<String>) -> Self {
        Self {
            dimension,
            space_id: space_id.into(),
        }
    }
}

impl IndexDoc {
    pub fn new(id: impl Into<String>) -> Self {
        IndexDoc {
            id: id.into(),
            ..Default::default()
        }
    }

    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn with_body(mut self, body: impl Into<String>) -> Self {
        self.body = Some(body.into());
        self
    }

    pub fn with_tags(mut self, tags: impl Into<String>) -> Self {
        self.tags = Some(tags.into());
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
}

/// Hybrid query over text and/or vector rankings.
#[derive(Clone, Debug, Default)]
pub struct HybridQuery {
    /// Full-text query passed to `tantivy` BM25. Parsed leniently: raw user
    /// input never fails the query.
    pub text: Option<String>,
    /// Vector query passed to `hnsw_rs` ANN.
    pub vector: Option<Vec<f32>>,
    /// Scalar filters applied to both text and vector results.
    pub filters: Vec<ScalarFilter>,
    /// Maximum number of hits to return.
    pub k: usize,
    /// Fusion strategy (used only when both text and vector are present).
    pub fusion: Fusion,
    /// Per-field boosts for the text query. `None` uses the defaults.
    pub boosts: Option<FieldBoosts>,
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

    pub fn with_boosts(mut self, boosts: FieldBoosts) -> Self {
        self.boosts = Some(boosts);
        self
    }
}
