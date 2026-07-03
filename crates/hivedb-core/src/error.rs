/// Result type used across HiveDB core.
pub type HiveResult<T> = std::result::Result<T, HiveError>;

/// Errors that can occur in HiveDB core.
///
/// Large underlying error types are boxed to keep the enum small and avoid
/// `clippy::result_large_err`.
#[derive(Debug, thiserror::Error)]
pub enum HiveError {
    /// Underlying storage error from `redb`.
    #[error("storage error: {0}")]
    Storage(#[source] Box<redb::Error>),

    /// Transaction or database corruption error.
    #[error("database error: {0}")]
    Database(#[source] Box<redb::DatabaseError>),

    /// Table-related error.
    #[error("table error: {0}")]
    Table(#[source] Box<redb::TableError>),

    /// Transaction error.
    #[error("transaction error: {0}")]
    Transaction(#[source] Box<redb::TransactionError>),

    /// Commit error.
    #[error("commit error: {0}")]
    Commit(#[source] Box<redb::CommitError>),

    /// Storage error at the page/IO level.
    #[error("storage error: {0}")]
    StorageError(#[source] Box<redb::StorageError>),

    /// Serialization failure (bincode).
    #[error("serialization error: {0}")]
    Serialization(#[source] Box<bincode::ErrorKind>),

    /// JSON serialization failure.
    #[error("json error: {0}")]
    Json(#[source] Box<serde_json::Error>),

    /// I/O error.
    #[error("io error: {0}")]
    Io(#[source] Box<std::io::Error>),

    /// Requested event or projection not found.
    #[error("not found: {0}")]
    NotFound(String),

    /// Invalid input from the caller.
    #[error("invalid input: {0}")]
    InvalidInput(String),
}

impl From<redb::Error> for HiveError {
    fn from(e: redb::Error) -> Self {
        HiveError::Storage(Box::new(e))
    }
}

impl From<redb::DatabaseError> for HiveError {
    fn from(e: redb::DatabaseError) -> Self {
        HiveError::Database(Box::new(e))
    }
}

impl From<redb::TableError> for HiveError {
    fn from(e: redb::TableError) -> Self {
        HiveError::Table(Box::new(e))
    }
}

impl From<redb::TransactionError> for HiveError {
    fn from(e: redb::TransactionError) -> Self {
        HiveError::Transaction(Box::new(e))
    }
}

impl From<redb::CommitError> for HiveError {
    fn from(e: redb::CommitError) -> Self {
        HiveError::Commit(Box::new(e))
    }
}

impl From<redb::StorageError> for HiveError {
    fn from(e: redb::StorageError) -> Self {
        HiveError::StorageError(Box::new(e))
    }
}

impl From<Box<bincode::ErrorKind>> for HiveError {
    fn from(e: Box<bincode::ErrorKind>) -> Self {
        HiveError::Serialization(e)
    }
}

impl From<serde_json::Error> for HiveError {
    fn from(e: serde_json::Error) -> Self {
        HiveError::Json(Box::new(e))
    }
}

impl From<std::io::Error> for HiveError {
    fn from(e: std::io::Error) -> Self {
        HiveError::Io(Box::new(e))
    }
}
