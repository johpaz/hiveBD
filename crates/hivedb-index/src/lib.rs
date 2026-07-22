//! HiveDB index layer: BM25 full-text (`tantivy`), ANN vectors (`hnsw_rs`)
//! and Reciprocal Rank Fusion.

pub mod hnsw;
pub mod index;
pub mod rrf;
pub mod text;
pub mod types;

pub use hnsw::VectorIndex;
pub use index::SemanticIndex;
pub use rrf::rrf;
pub use text::TextIndex;
pub use types::{FieldBoosts, Fusion, Hit, HybridQuery, IndexDoc, ScalarFilter, VectorConfig};

/// Result type used across `hivedb-index`.
pub type Result<T> = std::result::Result<T, IndexError>;

/// Errors that can occur in the index layer.
#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    #[error("tantivy error: {0}")]
    Tantivy(#[from] tantivy::TantivyError),

    #[error("tantivy directory error: {0}")]
    TantivyDirectory(#[from] tantivy::directory::error::OpenDirectoryError),

    #[error("tantivy query parser error: {0}")]
    QueryParser(#[from] tantivy::query::QueryParserError),

    #[error("INVALID_VECTOR: dimension mismatch: expected {expected}, got {got}")]
    DimensionMismatch { expected: usize, got: usize },

    #[error("INVALID_VECTOR: {0}")]
    InvalidVector(String),

    #[error("INVALID_VECTOR: open the database with an explicit vector configuration")]
    VectorIndexDisabled,

    #[error("VECTOR_SPACE_MISMATCH: {0}")]
    VectorSpaceMismatch(String),

    #[error(
        "INDEX_DEGRADED: generation {generation} was committed but indexes could not be rebuilt: {cause}"
    )]
    IndexUnavailableAfterCommit { generation: u64, cause: String },

    #[error("semantic storage error: {0}")]
    Storage(String),

    #[error("serialization error: {0}")]
    Serialization(#[from] Box<bincode::ErrorKind>),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
}
