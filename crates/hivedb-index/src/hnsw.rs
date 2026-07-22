use hnsw_rs::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

pub const MAX_VECTOR_DIMENSION: usize = 65_536;

/// In-memory HNSW state. Durable vectors live in the semantic redb store;
/// this graph is a derived index that can always be rebuilt.
struct Inner {
    hnsw: Hnsw<'static, f32, DistCosine>,
    ids: Vec<String>,
    latest: HashMap<String, usize>,
    deleted: HashSet<usize>,
}

impl Inner {
    fn new() -> Self {
        Self {
            hnsw: Hnsw::new(16, 100_000, 200, 16, DistCosine),
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

/// Índice ANN derivado con upsert/delete mediante tombstones en memoria.
pub struct VectorIndex {
    inner: Mutex<Inner>,
    dimension: usize,
}

impl VectorIndex {
    pub fn new(dimension: usize) -> Self {
        Self {
            inner: Mutex::new(Inner::new()),
            dimension,
        }
    }

    pub fn insert(&self, id: String, vector: Vec<f32>) -> crate::Result<()> {
        validate_vector(&vector, self.dimension)?;
        self.inner.lock().unwrap().insert(id, &vector);
        Ok(())
    }

    pub fn delete(&self, id: &str) -> crate::Result<()> {
        self.inner.lock().unwrap().delete(id);
        Ok(())
    }

    pub fn clear(&self) -> crate::Result<()> {
        *self.inner.lock().unwrap() = Inner::new();
        Ok(())
    }

    /// Reemplaza el grafo por los vectores vivos suministrados.
    pub fn rebuild<'a, I>(&self, vectors: I) -> crate::Result<()>
    where
        I: IntoIterator<Item = (&'a str, &'a [f32])>,
    {
        let mut rebuilt = Inner::new();
        for (id, vector) in vectors {
            validate_vector(vector, self.dimension)?;
            rebuilt.insert(id.to_string(), vector);
        }
        *self.inner.lock().unwrap() = rebuilt;
        Ok(())
    }

    pub fn search(&self, vector: &[f32], k: usize) -> crate::Result<Vec<(String, usize, f32)>> {
        validate_vector(vector, self.dimension)?;
        if k == 0 {
            return Err(crate::IndexError::InvalidVector(
                "k must be greater than zero".into(),
            ));
        }

        let inner = self.inner.lock().unwrap();
        let want = k
            .saturating_mul(4)
            .max(k.saturating_add(inner.deleted.len()))
            .max(1);
        let ef = want.max(50);
        let neighbors = inner.hnsw.search(vector, want, ef);

        let mut results = Vec::with_capacity(k);
        for neighbor in neighbors {
            let internal_id = neighbor.d_id;
            if inner.deleted.contains(&internal_id) {
                continue;
            }
            if let Some(id) = inner.ids.get(internal_id) {
                let similarity = (1.0 - neighbor.distance).clamp(-1.0, 1.0);
                results.push((id.clone(), results.len() + 1, similarity));
                if results.len() == k {
                    break;
                }
            }
        }
        Ok(results)
    }

    pub fn should_compact(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.deleted.len() >= 1_024 && inner.deleted.len().saturating_mul(4) >= inner.ids.len()
    }

    pub fn stats(&self) -> (usize, usize) {
        let inner = self.inner.lock().unwrap();
        (inner.latest.len(), inner.deleted.len())
    }
}

pub fn validate_vector(vector: &[f32], expected_dimension: usize) -> crate::Result<()> {
    if vector.len() != expected_dimension {
        return Err(crate::IndexError::DimensionMismatch {
            expected: expected_dimension,
            got: vector.len(),
        });
    }
    let mut norm_squared = 0.0f64;
    for (index, value) in vector.iter().copied().enumerate() {
        if !value.is_finite() {
            return Err(crate::IndexError::InvalidVector(format!(
                "coordinate {index} is not finite"
            )));
        }
        let value = f64::from(value);
        norm_squared += value * value;
    }
    if norm_squared == 0.0 {
        return Err(crate::IndexError::InvalidVector(
            "vector norm must be greater than zero".into(),
        ));
    }
    Ok(())
}

pub fn cosine_similarity(left: &[f32], right: &[f32]) -> f32 {
    let (mut dot, mut left_norm, mut right_norm) = (0.0f64, 0.0f64, 0.0f64);
    for (&left, &right) in left.iter().zip(right) {
        let left = f64::from(left);
        let right = f64::from(right);
        dot += left * right;
        left_norm += left * left;
        right_norm += right * right;
    }
    (dot / (left_norm * right_norm).sqrt()).clamp(-1.0, 1.0) as f32
}
