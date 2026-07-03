use hnsw_rs::prelude::*;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Seek, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

/// File that stores every inserted `(id, vector)` pair, append-only, so the
/// HNSW graph can be rebuilt on open. The graph itself is cheap to rebuild
/// relative to recomputing embeddings, which live outside the engine.
const VECTORS_FILE: &str = "vectors.bin";

/// Wrapper around an `hnsw_rs` approximate-nearest-neighbors index.
pub struct VectorIndex {
    hnsw: Arc<Mutex<Hnsw<'static, f32, DistCosine>>>,
    dimension: usize,
    next_internal_id: Mutex<usize>,
    ids: Mutex<Vec<String>>,
    persist: Option<Mutex<BufWriter<File>>>,
}

impl VectorIndex {
    /// Create a new in-memory HNSW index for vectors of the given dimension.
    pub fn new(dimension: usize) -> Self {
        let max_elements = 100_000;
        let hnsw = Hnsw::new(
            16, // max_nb_conn
            max_elements,
            200, // ef_c
            16,  // max_layer
            DistCosine,
        );
        Self {
            hnsw: Arc::new(Mutex::new(hnsw)),
            dimension,
            next_internal_id: Mutex::new(0),
            ids: Mutex::new(Vec::new()),
            persist: None,
        }
    }

    /// Open or create a vector index at the given directory.
    ///
    /// Vectors are persisted to an append-only file inside the directory and
    /// the HNSW graph is rebuilt from it on open. A partially written trailing
    /// record (e.g. after a crash) is truncated away.
    pub fn open<P: AsRef<Path>>(path: P, dimension: usize) -> crate::Result<Self> {
        let index = Self::new(dimension);
        let file_path = path.as_ref().join(VECTORS_FILE);

        if file_path.exists() {
            let mut reader = BufReader::new(File::open(&file_path)?);
            let mut good_offset: u64 = 0;
            // Read until EOF or a torn trailing record: keep the last whole one.
            while let Ok((id, vector)) =
                bincode::deserialize_from::<_, (String, Vec<f32>)>(&mut reader)
            {
                index.insert_in_memory(id, vector)?;
                good_offset = reader.stream_position()?;
            }
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

    /// Insert a vector associated with an external document id.
    pub fn insert(&self, id: String, vector: Vec<f32>) -> crate::Result<()> {
        if vector.len() != self.dimension {
            return Err(crate::IndexError::DimensionMismatch {
                expected: self.dimension,
                got: vector.len(),
            });
        }

        // Persist first: if the write fails, the in-memory index stays
        // consistent with what is on disk.
        if let Some(persist) = &self.persist {
            let mut writer = persist.lock().unwrap();
            bincode::serialize_into(&mut *writer, &(&id, &vector))?;
            writer.flush()?;
        }

        self.insert_in_memory(id, vector)
    }

    fn insert_in_memory(&self, id: String, vector: Vec<f32>) -> crate::Result<()> {
        if vector.len() != self.dimension {
            return Err(crate::IndexError::DimensionMismatch {
                expected: self.dimension,
                got: vector.len(),
            });
        }

        let mut next_id = self.next_internal_id.lock().unwrap();
        let internal_id = *next_id;
        *next_id += 1;

        self.ids.lock().unwrap().push(id);
        self.hnsw.lock().unwrap().insert((&vector, internal_id));
        Ok(())
    }

    /// Search for the `k` nearest neighbors of `vector`.
    pub fn search(&self, vector: &[f32], k: usize) -> crate::Result<Vec<(String, usize)>> {
        if vector.len() != self.dimension {
            return Err(crate::IndexError::DimensionMismatch {
                expected: self.dimension,
                got: vector.len(),
            });
        }

        let guard = self.hnsw.lock().unwrap();
        let neighbors = guard.search(vector, k, 50);

        let ids = self.ids.lock().unwrap();
        let mut results = Vec::with_capacity(neighbors.len());
        for (rank, neighbor) in neighbors.iter().enumerate() {
            let internal_id = neighbor.d_id;
            if let Some(id) = ids.get(internal_id) {
                results.push((id.clone(), rank + 1));
            }
        }
        Ok(results)
    }
}
