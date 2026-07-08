//! Document collections over `redb` (gate G10).
//!
//! Mutable CRUD storage for JSON documents, separate from the immutable
//! event log: named collections with optimistic versioning, equality
//! secondary indexes (optionally unique) and atomic multi-op batches.
//!
//! Storage layout (single `collections.redb` file):
//! - `col_docs`:          (collection, id) -> bincode(StoredDoc)
//! - `col_index_entries`: (collection, field, encoded_value, id) -> ()
//! - `col_index_defs`:    (collection, field) -> bincode(IndexDef)

use crate::error::{HiveError, HiveResult};
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;
use std::sync::RwLock;

const DOCS: TableDefinition<(&str, &str), Vec<u8>> = TableDefinition::new("col_docs");
const INDEX_ENTRIES: TableDefinition<(&str, &str, &str, &str), ()> =
    TableDefinition::new("col_index_entries");
const INDEX_DEFS: TableDefinition<(&str, &str), Vec<u8>> = TableDefinition::new("col_index_defs");

const COLLECTIONS_FILE: &str = "collections.redb";

#[derive(Serialize, Deserialize)]
struct StoredDoc {
    version: u64,
    json: String,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
pub struct IndexDef {
    pub unique: bool,
}

/// A document read from a collection.
#[derive(Clone, Debug, PartialEq)]
pub struct DocEntry {
    pub id: String,
    /// Monotonic per-document version, starting at 1 on first put.
    pub version: u64,
    pub doc: Value,
}

/// Options for [`Collections::put`].
#[derive(Clone, Copy, Debug, Default)]
pub struct PutOptions {
    /// Optimistic concurrency check: the current stored version must equal
    /// this value (`0` = the document must not exist yet). `None` skips the
    /// check (unconditional upsert).
    pub expected_version: Option<u64>,
}

/// Options for [`Collections::scan`] and [`Collections::find_by`].
#[derive(Clone, Debug, Default)]
pub struct ScanOptions {
    /// Only ids starting with this prefix.
    pub prefix: Option<String>,
    /// Start at this id (inclusive, ascending id order).
    pub start: Option<String>,
    /// Maximum entries to return (`0` = unlimited).
    pub limit: usize,
    /// Entries to skip before collecting.
    pub offset: usize,
    /// Return entries in descending id order.
    pub reverse: bool,
}

/// One operation inside an atomic [`Collections::batch`].
#[derive(Clone, Debug)]
pub enum ColOp {
    Put {
        collection: String,
        id: String,
        doc: Value,
        expected_version: Option<u64>,
    },
    Delete {
        collection: String,
        id: String,
    },
}

/// Encode a JSON scalar as an index token. Non-scalar and null values are
/// not indexed (a missing field simply has no index entry).
///
/// Numbers use their canonical JSON text for EQUALITY matching only: `1` and
/// `1.0` are distinct tokens, and range queries are not supported.
fn encode_index_value(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(format!("s:{s}")),
        Value::Number(n) => Some(format!("n:{n}")),
        Value::Bool(b) => Some(format!("b:{b}")),
        Value::Null | Value::Array(_) | Value::Object(_) => None,
    }
}

type DefsMap = HashMap<String, HashMap<String, IndexDef>>;

/// Document collections handle. All writes are single `redb` transactions:
/// a put/delete and its index maintenance commit atomically.
pub struct Collections {
    db: Database,
    /// In-memory cache of index definitions, loaded at open and kept in sync
    /// by `create_index` / `drop_index`.
    defs: RwLock<DefsMap>,
}

impl Collections {
    /// Open (or create) the collections store under the given base directory.
    pub fn open<P: AsRef<Path>>(base_dir: P) -> HiveResult<Self> {
        let path = base_dir.as_ref().join(COLLECTIONS_FILE);
        let db = Database::create(path).map_err(|e| HiveError::Database(Box::new(e)))?;

        // Ensure tables exist and load index definitions.
        let mut defs: DefsMap = HashMap::new();
        {
            let txn = db
                .begin_write()
                .map_err(|e| HiveError::Transaction(Box::new(e)))?;
            {
                txn.open_table(DOCS)
                    .map_err(|e| HiveError::Table(Box::new(e)))?;
                txn.open_table(INDEX_ENTRIES)
                    .map_err(|e| HiveError::Table(Box::new(e)))?;
                let defs_table = txn
                    .open_table(INDEX_DEFS)
                    .map_err(|e| HiveError::Table(Box::new(e)))?;
                for entry in defs_table
                    .iter()
                    .map_err(|e| HiveError::StorageError(Box::new(e)))?
                {
                    let (key, value) = entry.map_err(|e| HiveError::StorageError(Box::new(e)))?;
                    let (collection, field) = key.value();
                    let def: IndexDef =
                        bincode::deserialize(&value.value()).map_err(HiveError::Serialization)?;
                    defs.entry(collection.to_string())
                        .or_default()
                        .insert(field.to_string(), def);
                }
            }
            txn.commit().map_err(|e| HiveError::Commit(Box::new(e)))?;
        }

        Ok(Self {
            db,
            defs: RwLock::new(defs),
        })
    }

    fn collection_defs(&self, collection: &str) -> HashMap<String, IndexDef> {
        self.defs
            .read()
            .unwrap()
            .get(collection)
            .cloned()
            .unwrap_or_default()
    }

    /// Insert or replace a document. Returns the new version (starts at 1).
    pub fn put(
        &self,
        collection: &str,
        id: &str,
        doc: &Value,
        options: PutOptions,
    ) -> HiveResult<u64> {
        let defs = self.collection_defs(collection);
        let txn = self
            .db
            .begin_write()
            .map_err(|e| HiveError::Transaction(Box::new(e)))?;
        let new_version;
        {
            let mut docs = txn
                .open_table(DOCS)
                .map_err(|e| HiveError::Table(Box::new(e)))?;
            let mut entries = txn
                .open_table(INDEX_ENTRIES)
                .map_err(|e| HiveError::Table(Box::new(e)))?;
            new_version = put_in_txn(
                &mut docs,
                &mut entries,
                &defs,
                collection,
                id,
                doc,
                options.expected_version,
            )?;
        }
        txn.commit().map_err(|e| HiveError::Commit(Box::new(e)))?;
        Ok(new_version)
    }

    /// Read a document by id.
    pub fn get(&self, collection: &str, id: &str) -> HiveResult<Option<DocEntry>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| HiveError::Transaction(Box::new(e)))?;
        let docs = txn
            .open_table(DOCS)
            .map_err(|e| HiveError::Table(Box::new(e)))?;
        let stored = docs
            .get(&(collection, id))
            .map_err(|e| HiveError::StorageError(Box::new(e)))?;
        match stored {
            Some(guard) => {
                let stored: StoredDoc =
                    bincode::deserialize(&guard.value()).map_err(HiveError::Serialization)?;
                Ok(Some(DocEntry {
                    id: id.to_string(),
                    version: stored.version,
                    doc: serde_json::from_str(&stored.json)
                        .map_err(|e| HiveError::Json(Box::new(e)))?,
                }))
            }
            None => Ok(None),
        }
    }

    /// Delete a document. Returns `true` if it existed.
    pub fn delete(&self, collection: &str, id: &str) -> HiveResult<bool> {
        let defs = self.collection_defs(collection);
        let txn = self
            .db
            .begin_write()
            .map_err(|e| HiveError::Transaction(Box::new(e)))?;
        let existed;
        {
            let mut docs = txn
                .open_table(DOCS)
                .map_err(|e| HiveError::Table(Box::new(e)))?;
            let mut entries = txn
                .open_table(INDEX_ENTRIES)
                .map_err(|e| HiveError::Table(Box::new(e)))?;
            existed = delete_in_txn(&mut docs, &mut entries, &defs, collection, id)?;
        }
        txn.commit().map_err(|e| HiveError::Commit(Box::new(e)))?;
        Ok(existed)
    }

    /// Scan a collection in id order.
    pub fn scan(&self, collection: &str, options: &ScanOptions) -> HiveResult<Vec<DocEntry>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| HiveError::Transaction(Box::new(e)))?;
        let docs = txn
            .open_table(DOCS)
            .map_err(|e| HiveError::Table(Box::new(e)))?;

        let start_id = options.start.clone().unwrap_or_default();
        let mut matched: Vec<DocEntry> = Vec::new();
        let range = docs
            .range((collection, start_id.as_str())..)
            .map_err(|e| HiveError::StorageError(Box::new(e)))?;
        for entry in range {
            let (key, value) = entry.map_err(|e| HiveError::StorageError(Box::new(e)))?;
            let (col, id) = key.value();
            if col != collection {
                break;
            }
            if let Some(prefix) = &options.prefix
                && !id.starts_with(prefix.as_str())
            {
                continue;
            }
            let stored: StoredDoc =
                bincode::deserialize(&value.value()).map_err(HiveError::Serialization)?;
            matched.push(DocEntry {
                id: id.to_string(),
                version: stored.version,
                doc: serde_json::from_str(&stored.json)
                    .map_err(|e| HiveError::Json(Box::new(e)))?,
            });
        }

        if options.reverse {
            matched.reverse();
        }
        Ok(apply_window(matched, options.offset, options.limit))
    }

    /// Number of documents in a collection.
    pub fn count(&self, collection: &str) -> HiveResult<u64> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| HiveError::Transaction(Box::new(e)))?;
        let docs = txn
            .open_table(DOCS)
            .map_err(|e| HiveError::Table(Box::new(e)))?;
        let mut count = 0u64;
        let range = docs
            .range((collection, "")..)
            .map_err(|e| HiveError::StorageError(Box::new(e)))?;
        for entry in range {
            let (key, _) = entry.map_err(|e| HiveError::StorageError(Box::new(e)))?;
            if key.value().0 != collection {
                break;
            }
            count += 1;
        }
        Ok(count)
    }

    /// Create an equality index on a top-level field, backfilling existing
    /// documents. With `unique: true`, existing duplicates make this fail.
    /// Idempotent when the index already exists with the same uniqueness.
    pub fn create_index(&self, collection: &str, field: &str, unique: bool) -> HiveResult<()> {
        {
            let defs = self.defs.read().unwrap();
            if let Some(def) = defs.get(collection).and_then(|m| m.get(field)) {
                if def.unique == unique {
                    return Ok(());
                }
                return Err(HiveError::InvalidInput(format!(
                    "index {collection}.{field} already exists with unique={}",
                    def.unique
                )));
            }
        }

        let txn = self
            .db
            .begin_write()
            .map_err(|e| HiveError::Transaction(Box::new(e)))?;
        {
            let docs = txn
                .open_table(DOCS)
                .map_err(|e| HiveError::Table(Box::new(e)))?;
            let mut entries = txn
                .open_table(INDEX_ENTRIES)
                .map_err(|e| HiveError::Table(Box::new(e)))?;
            let mut defs_table = txn
                .open_table(INDEX_DEFS)
                .map_err(|e| HiveError::Table(Box::new(e)))?;

            // Backfill from existing documents.
            let mut seen: HashMap<String, String> = HashMap::new();
            let mut to_insert: Vec<(String, String)> = Vec::new();
            let range = docs
                .range((collection, "")..)
                .map_err(|e| HiveError::StorageError(Box::new(e)))?;
            for entry in range {
                let (key, value) = entry.map_err(|e| HiveError::StorageError(Box::new(e)))?;
                let (col, id) = key.value();
                if col != collection {
                    break;
                }
                let stored: StoredDoc =
                    bincode::deserialize(&value.value()).map_err(HiveError::Serialization)?;
                let doc: Value =
                    serde_json::from_str(&stored.json).map_err(|e| HiveError::Json(Box::new(e)))?;
                if let Some(encoded) = doc.get(field).and_then(encode_index_value) {
                    if unique && let Some(other) = seen.get(&encoded) {
                        return Err(HiveError::InvalidInput(format!(
                            "unique index {collection}.{field} violated by existing docs \
                             {other:?} and {id:?}"
                        )));
                    }
                    seen.insert(encoded.clone(), id.to_string());
                    to_insert.push((encoded, id.to_string()));
                }
            }
            for (encoded, id) in to_insert {
                entries
                    .insert(&(collection, field, encoded.as_str(), id.as_str()), ())
                    .map_err(|e| HiveError::StorageError(Box::new(e)))?;
            }

            let def = IndexDef { unique };
            defs_table
                .insert(
                    &(collection, field),
                    bincode::serialize(&def).map_err(HiveError::Serialization)?,
                )
                .map_err(|e| HiveError::StorageError(Box::new(e)))?;
        }
        txn.commit().map_err(|e| HiveError::Commit(Box::new(e)))?;

        self.defs
            .write()
            .unwrap()
            .entry(collection.to_string())
            .or_default()
            .insert(field.to_string(), IndexDef { unique });
        Ok(())
    }

    /// Look up documents whose indexed `field` equals `value`.
    /// Requires a previous [`Collections::create_index`] on that field.
    pub fn find_by(
        &self,
        collection: &str,
        field: &str,
        value: &Value,
        options: &ScanOptions,
    ) -> HiveResult<Vec<DocEntry>> {
        if self
            .defs
            .read()
            .unwrap()
            .get(collection)
            .and_then(|m| m.get(field))
            .is_none()
        {
            return Err(HiveError::InvalidInput(format!(
                "no index on {collection}.{field} — call create_index first"
            )));
        }
        let Some(encoded) = encode_index_value(value) else {
            return Ok(Vec::new());
        };

        let txn = self
            .db
            .begin_read()
            .map_err(|e| HiveError::Transaction(Box::new(e)))?;
        let entries = txn
            .open_table(INDEX_ENTRIES)
            .map_err(|e| HiveError::Table(Box::new(e)))?;
        let docs = txn
            .open_table(DOCS)
            .map_err(|e| HiveError::Table(Box::new(e)))?;

        let mut matched: Vec<DocEntry> = Vec::new();
        let range = entries
            .range((collection, field, encoded.as_str(), "")..)
            .map_err(|e| HiveError::StorageError(Box::new(e)))?;
        for entry in range {
            let (key, _) = entry.map_err(|e| HiveError::StorageError(Box::new(e)))?;
            let (col, f, val, id) = key.value();
            if col != collection || f != field || val != encoded {
                break;
            }
            if let Some(guard) = docs
                .get(&(collection, id))
                .map_err(|e| HiveError::StorageError(Box::new(e)))?
            {
                let stored: StoredDoc =
                    bincode::deserialize(&guard.value()).map_err(HiveError::Serialization)?;
                matched.push(DocEntry {
                    id: id.to_string(),
                    version: stored.version,
                    doc: serde_json::from_str(&stored.json)
                        .map_err(|e| HiveError::Json(Box::new(e)))?,
                });
            }
        }

        if options.reverse {
            matched.reverse();
        }
        Ok(apply_window(matched, options.offset, options.limit))
    }

    /// Apply several puts/deletes atomically: either every operation commits
    /// or none does (a version conflict or unique violation aborts all).
    pub fn batch(&self, ops: &[ColOp]) -> HiveResult<()> {
        let defs_snapshot = self.defs.read().unwrap().clone();
        let txn = self
            .db
            .begin_write()
            .map_err(|e| HiveError::Transaction(Box::new(e)))?;
        {
            let mut docs = txn
                .open_table(DOCS)
                .map_err(|e| HiveError::Table(Box::new(e)))?;
            let mut entries = txn
                .open_table(INDEX_ENTRIES)
                .map_err(|e| HiveError::Table(Box::new(e)))?;
            let empty = HashMap::new();
            for op in ops {
                match op {
                    ColOp::Put {
                        collection,
                        id,
                        doc,
                        expected_version,
                    } => {
                        let defs = defs_snapshot.get(collection).unwrap_or(&empty);
                        put_in_txn(
                            &mut docs,
                            &mut entries,
                            defs,
                            collection,
                            id,
                            doc,
                            *expected_version,
                        )?;
                    }
                    ColOp::Delete { collection, id } => {
                        let defs = defs_snapshot.get(collection).unwrap_or(&empty);
                        delete_in_txn(&mut docs, &mut entries, defs, collection, id)?;
                    }
                }
            }
        }
        txn.commit().map_err(|e| HiveError::Commit(Box::new(e)))?;
        Ok(())
    }
}

fn apply_window(matched: Vec<DocEntry>, offset: usize, limit: usize) -> Vec<DocEntry> {
    let iter = matched.into_iter().skip(offset);
    if limit > 0 {
        iter.take(limit).collect()
    } else {
        iter.collect()
    }
}

type DocsTable<'txn> = redb::Table<'txn, (&'static str, &'static str), Vec<u8>>;
type EntriesTable<'txn> =
    redb::Table<'txn, (&'static str, &'static str, &'static str, &'static str), ()>;

fn put_in_txn(
    docs: &mut DocsTable<'_>,
    entries: &mut EntriesTable<'_>,
    defs: &HashMap<String, IndexDef>,
    collection: &str,
    id: &str,
    doc: &Value,
    expected_version: Option<u64>,
) -> HiveResult<u64> {
    let existing: Option<StoredDoc> = docs
        .get(&(collection, id))
        .map_err(|e| HiveError::StorageError(Box::new(e)))?
        .map(|g| bincode::deserialize(&g.value()))
        .transpose()
        .map_err(HiveError::Serialization)?;

    let current_version = existing.as_ref().map(|d| d.version).unwrap_or(0);
    if let Some(expected) = expected_version
        && expected != current_version
    {
        return Err(HiveError::InvalidInput(format!(
            "version conflict on {collection}/{id}: expected {expected}, found {current_version}"
        )));
    }
    let new_version = current_version + 1;

    // Remove index entries of the previous document revision.
    if let Some(old) = &existing {
        let old_doc: Value =
            serde_json::from_str(&old.json).map_err(|e| HiveError::Json(Box::new(e)))?;
        for field in defs.keys() {
            if let Some(encoded) = old_doc.get(field).and_then(encode_index_value) {
                entries
                    .remove(&(collection, field.as_str(), encoded.as_str(), id))
                    .map_err(|e| HiveError::StorageError(Box::new(e)))?;
            }
        }
    }

    // Insert index entries for the new revision, enforcing uniqueness.
    for (field, def) in defs {
        if let Some(encoded) = doc.get(field).and_then(encode_index_value) {
            if def.unique {
                let mut conflict: Option<String> = None;
                let range = entries
                    .range((collection, field.as_str(), encoded.as_str(), "")..)
                    .map_err(|e| HiveError::StorageError(Box::new(e)))?;
                for entry in range {
                    let (key, _) = entry.map_err(|e| HiveError::StorageError(Box::new(e)))?;
                    let (col, f, val, other_id) = key.value();
                    if col != collection || f != field || val != encoded {
                        break;
                    }
                    if other_id != id {
                        conflict = Some(other_id.to_string());
                        break;
                    }
                }
                if let Some(other) = conflict {
                    return Err(HiveError::InvalidInput(format!(
                        "unique index {collection}.{field} violated: value already used by {other:?}"
                    )));
                }
            }
            entries
                .insert(&(collection, field.as_str(), encoded.as_str(), id), ())
                .map_err(|e| HiveError::StorageError(Box::new(e)))?;
        }
    }

    let stored = StoredDoc {
        version: new_version,
        json: doc.to_string(),
    };
    docs.insert(
        &(collection, id),
        bincode::serialize(&stored).map_err(HiveError::Serialization)?,
    )
    .map_err(|e| HiveError::StorageError(Box::new(e)))?;
    Ok(new_version)
}

fn delete_in_txn(
    docs: &mut DocsTable<'_>,
    entries: &mut EntriesTable<'_>,
    defs: &HashMap<String, IndexDef>,
    collection: &str,
    id: &str,
) -> HiveResult<bool> {
    let existing = docs
        .remove(&(collection, id))
        .map_err(|e| HiveError::StorageError(Box::new(e)))?;
    let Some(guard) = existing else {
        return Ok(false);
    };
    let stored: StoredDoc =
        bincode::deserialize(&guard.value()).map_err(HiveError::Serialization)?;
    drop(guard);

    let old_doc: Value =
        serde_json::from_str(&stored.json).map_err(|e| HiveError::Json(Box::new(e)))?;
    for field in defs.keys() {
        if let Some(encoded) = old_doc.get(field).and_then(encode_index_value) {
            entries
                .remove(&(collection, field.as_str(), encoded.as_str(), id))
                .map_err(|e| HiveError::StorageError(Box::new(e)))?;
        }
    }
    Ok(true)
}
