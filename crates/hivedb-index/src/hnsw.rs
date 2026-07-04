use hnsw_rs::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Seek, Write};
use std::path::Path;
use std::sync::Mutex;

/// File that stores every insert/delete operation, append-only, so the HNSW
/// graph can be rebuilt on open. The graph itself is cheap to rebuild
/// relative to recomputing embeddings, which live outside the engine.
///
/// `v2`: records are a tagged enum (insert/delete) instead of bare
/// `(id, vector)` pairs. Old `vectors.bin` files are removed on open.
const VECTORS_FILE: &str = "vectors.v2.bin";
const LEGACY_VECTORS_FILE: &str = "vectors.bin";

#[derive(Serialize, Deserialize)]
enum VecRecord {
    Insert { id: String, vector: Vec<f32> },
    Delete { id: String },
}

/// In-memory state guarded by a single lock: the graph plus the id maps that
/// implement upsert/delete on top of hnsw_rs (which cannot remove points).
struct Inner {
    hnsw: Hnsw<'static, f32, DistCosine>,
    /// internal id -> external document id, in insertion order.
    ids: Vec<String>,
    /// external document id -> live internal id.
    latest: HashMap<String, usize>,
    /// internal ids superseded by an upsert or removed by a delete.
    deleted: HashSet<usize>,
}

impl Inner {
    fn new() -> Self {
        let max_elements = 100_000;
        let hnsw = Hnsw::new(
            16, // max_nb_conn
            max_elements,
            200, // ef_c
            16,  // max_layer
            DistCosine,
        );
        Self {
            hnsw,
            ids: Vec::new(),
            latest: HashMap::new(),
            deleted: HashSet::new(),
        }
    }

    fn insert(&mut self, id: String, vector: &[f32]) {
        if let Some(old) = self.latest.get(&id) {
            self.deleted.insert(*old);
        }
        let internal_id = self.ids.len();
        self.latest.insert(id.clone(), internal_id);
        self.ids.push(id);
        self.hnsw.insert((vector, internal_id));
    }

    /// Returns true if the id was present.
    fn delete(&mut self, id: &str) -> bool {
        match self.latest.remove(id) {
            Some(internal_id) => {
                self.deleted.insert(internal_id);
                true
            }
            None => false,
        }
    }
}

/// Wrapper around an `hnsw_rs` approximate-nearest-neighbors index with
/// upsert/delete support via tombstones. Deleted vectors stay in the graph
/// (skipped at search time) until the index is cleared or rebuilt.
pub struct VectorIndex {
    inner: Mutex<Inner>,
    dimension: usize,
    persist: Option<Mutex<BufWriter<File>>>,
}

impl VectorIndex {
    /// Create a new in-memory HNSW index for vectors of the given dimension.
    pub fn new(dimension: usize) -> Self {
        Self {
            inner: Mutex::new(Inner::new()),
            dimension,
            persist: None,
        }
    }

    /// Open or create a vector index at the given directory.
    ///
    /// Operations are persisted to an append-only file inside the directory
    /// and the HNSW graph is rebuilt from it on open. A partially written
    /// trailing record (e.g. after a crash) is truncated away.
    pub fn open<P: AsRef<Path>>(path: P, dimension: usize) -> crate::Result<Self> {
        let index = Self::new(dimension);
        let file_path = path.as_ref().join(VECTORS_FILE);

        // Pre-v2 format is not readable; the engine rebuilds from scratch.
        let legacy = path.as_ref().join(LEGACY_VECTORS_FILE);
        if legacy.exists() {
            std::fs::remove_file(&legacy)?;
        }

        if file_path.exists() {
            let mut reader = BufReader::new(File::open(&file_path)?);
            let mut good_offset: u64 = 0;
            let mut inner = index.inner.lock().unwrap();
            // Read until EOF or a torn trailing record: keep the last whole one.
            while let Ok(record) = bincode::deserialize_from::<_, VecRecord>(&mut reader) {
                match record {
                    VecRecord::Insert { id, vector } => {
                        if vector.len() != dimension {
                            return Err(crate::IndexError::DimensionMismatch {
                                expected: dimension,
                                got: vector.len(),
                            });
                        }
                        inner.insert(id, &vector);
                    }
                    VecRecord::Delete { id } => {
                        inner.delete(&id);
                    }
                }
                good_offset = reader.stream_position()?;
            }
            drop(inner);
            drop(reader);

            let file_len = std::fs::metadata(&file_path)?.len();
            if good_offset < file_len {
                OpenOptions::new()
                    .write(true)
                    .open(&file_path)?
                    .set_len(good_offset)?;
            }
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)?;
        Ok(Self {
            persist: Some(Mutex::new(BufWriter::new(file))),
            ..index
        })
    }

    fn check_dimension(&self, len: usize) -> crate::Result<()> {
        if len != self.dimension {
            return Err(crate::IndexError::DimensionMismatch {
                expected: self.dimension,
                got: len,
            });
        }
        Ok(())
    }

    fn persist_record(&self, record: &VecRecord) -> crate::Result<()> {
        if let Some(persist) = &self.persist {
            let mut writer = persist.lock().unwrap();
            bincode::serialize_into(&mut *writer, record)?;
            writer.flush()?;
        }
        Ok(())
    }

    /// Insert or replace the vector associated with an external document id.
    pub fn insert(&self, id: String, vector: Vec<f32>) -> crate::Result<()> {
        self.check_dimension(vector.len())?;

        // Persist first: if the write fails, the in-memory index stays
        // consistent with what is on disk.
        self.persist_record(&VecRecord::Insert {
            id: id.clone(),
            vector: vector.clone(),
        })?;

        self.inner.lock().unwrap().insert(id, &vector);
        Ok(())
    }

    /// Delete the vector for an external document id. Missing ids are a no-op.
    pub fn delete(&self, id: &str) -> crate::Result<()> {
        {
            let inner = self.inner.lock().unwrap();
            if !inner.latest.contains_key(id) {
                return Ok(());
            }
        }
        self.persist_record(&VecRecord::Delete { id: id.to_string() })?;
        self.inner.lock().unwrap().delete(id);
        Ok(())
    }

    /// Remove every vector, resetting the graph and truncating the
    /// persistence file.
    pub fn clear(&self) -> crate::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        if let Some(persist) = &self.persist {
            let mut writer = persist.lock().unwrap();
            writer.flush()?;
            writer.get_ref().set_len(0)?;
        }
        *inner = Inner::new();
        Ok(())
    }

    /// Search for the `k` nearest live neighbors of `vector`. Returns
    /// `(id, rank, cosine_similarity)` triples, best first.
    pub fn search(&self, vector: &[f32], k: usize) -> crate::Result<Vec<(String, usize, f32)>> {
        self.check_dimension(vector.len())?;

        let inner = self.inner.lock().unwrap();
        // Oversample so tombstoned points don't starve the result set.
        let want = (k * 4).max(k + inner.deleted.len()).max(1);
        let ef = want.max(50);
        let neighbors = inner.hnsw.search(vector, want, ef);

        let mut results = Vec::with_capacity(k);
        for neighbor in neighbors {
            let internal_id = neighbor.d_id;
            if inner.deleted.contains(&internal_id) {
                continue;
            }
            if let Some(id) = inner.ids.get(internal_id) {
                let similarity = 1.0 - neighbor.distance;
                results.push((id.clone(), results.len() + 1, similarity));
                if results.len() == k {
                    break;
                }
            }
        }
        Ok(results)
    }
}
