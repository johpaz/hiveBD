use crate::hnsw::{MAX_VECTOR_DIMENSION, VectorIndex, cosine_similarity, validate_vector};
use crate::rrf::rrf;
use crate::text::TextIndex;
use crate::types::{Fusion, Hit, HybridQuery, IndexDoc, ScalarFilter, VectorConfig};
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::RwLock;

const DOCS: TableDefinition<&str, Vec<u8>> = TableDefinition::new("semantic_docs");
const META: TableDefinition<&str, u64> = TableDefinition::new("semantic_meta");
const GENERATION_KEY: &str = "generation";
const STORE_FILE: &str = "semantic.redb";
const DATABASE_META_FILE: &str = "meta.json";
const SCHEMA_VERSION: u32 = 2;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct DatabaseMeta {
    schema_version: u32,
    metric: String,
    vector: Option<VectorConfig>,
}

struct SemanticStore {
    db: Database,
}

impl SemanticStore {
    fn open(base: &Path) -> crate::Result<Self> {
        let db = Database::create(base.join(STORE_FILE)).map_err(storage_error)?;
        let txn = db.begin_write().map_err(storage_error)?;
        {
            txn.open_table(DOCS).map_err(storage_error)?;
            txn.open_table(META).map_err(storage_error)?;
        }
        txn.commit().map_err(storage_error)?;
        Ok(Self { db })
    }

    fn generation(&self) -> crate::Result<u64> {
        let txn = self.db.begin_read().map_err(storage_error)?;
        let meta = txn.open_table(META).map_err(storage_error)?;
        Ok(meta
            .get(GENERATION_KEY)
            .map_err(storage_error)?
            .map(|value| value.value())
            .unwrap_or(0))
    }

    fn load_all(&self) -> crate::Result<Vec<IndexDoc>> {
        let txn = self.db.begin_read().map_err(storage_error)?;
        let docs = txn.open_table(DOCS).map_err(storage_error)?;
        let mut loaded = Vec::new();
        for entry in docs.iter().map_err(storage_error)? {
            let (_id, value) = entry.map_err(storage_error)?;
            loaded.push(bincode::deserialize(&value.value())?);
        }
        Ok(loaded)
    }

    fn load_ids(&self, ids: &[String]) -> crate::Result<Vec<IndexDoc>> {
        let txn = self.db.begin_read().map_err(storage_error)?;
        let docs = txn.open_table(DOCS).map_err(storage_error)?;
        let mut loaded = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(value) = docs.get(id.as_str()).map_err(storage_error)? {
                loaded.push(bincode::deserialize(&value.value())?);
            }
        }
        Ok(loaded)
    }

    fn upsert_batch(&self, documents: &[IndexDoc]) -> crate::Result<u64> {
        let txn = self.db.begin_write().map_err(storage_error)?;
        let generation;
        {
            let mut docs = txn.open_table(DOCS).map_err(storage_error)?;
            for doc in documents {
                let encoded = bincode::serialize(doc)?;
                docs.insert(doc.id.as_str(), encoded)
                    .map_err(storage_error)?;
            }
            let mut meta = txn.open_table(META).map_err(storage_error)?;
            generation = meta
                .get(GENERATION_KEY)
                .map_err(storage_error)?
                .map(|value| value.value())
                .unwrap_or(0)
                .checked_add(1)
                .ok_or_else(|| crate::IndexError::Storage("semantic generation overflow".into()))?;
            meta.insert(GENERATION_KEY, generation)
                .map_err(storage_error)?;
        }
        txn.commit().map_err(storage_error)?;
        Ok(generation)
    }

    fn delete_ids(&self, ids: &[String]) -> crate::Result<u64> {
        let txn = self.db.begin_write().map_err(storage_error)?;
        let generation;
        {
            let mut docs = txn.open_table(DOCS).map_err(storage_error)?;
            for id in ids {
                docs.remove(id.as_str()).map_err(storage_error)?;
            }
            let mut meta = txn.open_table(META).map_err(storage_error)?;
            generation = next_generation(&mut meta)?;
        }
        txn.commit().map_err(storage_error)?;
        Ok(generation)
    }

    fn clear(&self) -> crate::Result<u64> {
        let txn = self.db.begin_write().map_err(storage_error)?;
        let generation;
        {
            let mut docs = txn.open_table(DOCS).map_err(storage_error)?;
            docs.retain(|_, _| false).map_err(storage_error)?;
            let mut meta = txn.open_table(META).map_err(storage_error)?;
            generation = next_generation(&mut meta)?;
        }
        txn.commit().map_err(storage_error)?;
        Ok(generation)
    }
}

fn next_generation(meta: &mut redb::Table<'_, &str, u64>) -> crate::Result<u64> {
    let generation = meta
        .get(GENERATION_KEY)
        .map_err(storage_error)?
        .map(|value| value.value())
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| crate::IndexError::Storage("semantic generation overflow".into()))?;
    meta.insert(GENERATION_KEY, generation)
        .map_err(storage_error)?;
    Ok(generation)
}

fn storage_error(error: impl std::fmt::Display) -> crate::IndexError {
    crate::IndexError::Storage(error.to_string())
}

struct DerivedState {
    text: TextIndex,
    vector: Option<VectorIndex>,
    generation: u64,
}

/// Índice semántico híbrido respaldado por documentos autoritativos en redb.
pub struct SemanticIndex {
    store: SemanticStore,
    vector_config: Option<VectorConfig>,
    state: RwLock<DerivedState>,
    _temp_dir: Option<tempfile::TempDir>,
}

impl SemanticIndex {
    pub fn open<P: AsRef<Path>>(
        base_dir: P,
        vector_config: Option<VectorConfig>,
    ) -> crate::Result<Self> {
        Self::open_inner(base_dir.as_ref(), vector_config, None)
    }

    pub fn open_in_ram(vector_config: Option<VectorConfig>) -> crate::Result<Self> {
        let temp_dir = tempfile::tempdir()?;
        let base = temp_dir.path().to_path_buf();
        Self::open_inner(&base, vector_config, Some(temp_dir))
    }

    fn open_inner(
        base: &Path,
        vector_config: Option<VectorConfig>,
        temp_dir: Option<tempfile::TempDir>,
    ) -> crate::Result<Self> {
        std::fs::create_dir_all(base)?;
        validate_config(vector_config.as_ref())?;
        resolve_database_meta(base, vector_config.as_ref())?;

        let fts_dir = base.join("fts");
        std::fs::create_dir_all(&fts_dir)?;
        let store = SemanticStore::open(base)?;
        let documents = store.load_all()?;
        validate_documents(&documents, vector_config.as_ref())?;
        let generation = store.generation()?;
        let text = TextIndex::open(fts_dir)?;
        text.clear()?;
        text.upsert_batch(&documents)?;
        let vector = build_vector_index(vector_config.as_ref(), &documents)?;

        Ok(Self {
            store,
            vector_config,
            state: RwLock::new(DerivedState {
                text,
                vector,
                generation,
            }),
            _temp_dir: temp_dir,
        })
    }

    pub fn upsert(&self, doc: &IndexDoc) -> crate::Result<()> {
        self.upsert_batch(std::slice::from_ref(doc))
    }

    pub fn upsert_batch(&self, docs: &[IndexDoc]) -> crate::Result<()> {
        validate_documents(docs, self.vector_config.as_ref())?;
        if docs.is_empty() {
            return Ok(());
        }
        let mut state = self.state.write().unwrap();
        let generation = self.store.upsert_batch(docs)?;
        let update = state.text.upsert_batch(docs).and_then(|()| {
            for doc in docs {
                sync_vector(state.vector.as_ref(), doc)?;
            }
            Ok(())
        });
        self.finish_update(&mut state, generation, update)
    }

    pub fn delete(&self, id: &str) -> crate::Result<()> {
        let mut state = self.state.write().unwrap();
        let generation = self.store.delete_ids(&[id.to_string()])?;
        let update = state
            .text
            .delete_doc(id)
            .and_then(|()| match state.vector.as_ref() {
                Some(vector) => vector.delete(id),
                None => Ok(()),
            });
        self.finish_update(&mut state, generation, update)
    }

    pub fn delete_by_filter(&self, filter: &ScalarFilter) -> crate::Result<()> {
        let mut state = self.state.write().unwrap();
        let ids = state.text.ids_by_filter(filter)?;
        if ids.is_empty() {
            return Ok(());
        }
        let generation = self.store.delete_ids(&ids)?;
        let update = (|| {
            if let Some(vector) = state.vector.as_ref() {
                for id in &ids {
                    vector.delete(id)?;
                }
            }
            state.text.delete_by_filter(filter)
        })();
        self.finish_update(&mut state, generation, update)
    }

    pub fn clear(&self) -> crate::Result<()> {
        let mut state = self.state.write().unwrap();
        let generation = self.store.clear()?;
        let update = state
            .text
            .clear()
            .and_then(|()| match state.vector.as_ref() {
                Some(vector) => vector.clear(),
                None => Ok(()),
            });
        self.finish_update(&mut state, generation, update)
    }

    pub fn compact(&self) -> crate::Result<()> {
        let mut state = self.state.write().unwrap();
        self.rebuild_state(&mut state)
    }

    pub fn query_hybrid(&self, query: HybridQuery) -> crate::Result<Vec<Hit>> {
        if query.k == 0 {
            return Err(crate::IndexError::InvalidVector(
                "k must be greater than zero".into(),
            ));
        }
        if let Some(vector) = query.vector.as_ref() {
            let config = self
                .vector_config
                .as_ref()
                .ok_or(crate::IndexError::VectorIndexDisabled)?;
            validate_vector(vector, config.dimension)?;
        }
        self.ensure_synced()?;
        let state = self.state.read().unwrap();
        let boosts = query.boosts.unwrap_or_default();

        let text_ranking = match &query.text {
            Some(text) => Some(state.text.search(text, &query.filters, boosts, query.k)?),
            None => None,
        };
        let vector_ranking = match &query.vector {
            Some(vector) if query.filters.is_empty() => Some(
                state
                    .vector
                    .as_ref()
                    .ok_or(crate::IndexError::VectorIndexDisabled)?
                    .search(vector, query.k)?,
            ),
            Some(vector) => Some(self.search_vector_filtered_exact(
                &state.text,
                vector,
                &query.filters,
                query.k,
            )?),
            None => None,
        };

        Ok(merge_rankings(
            text_ranking,
            vector_ranking,
            query.fusion,
            query.k,
        ))
    }

    fn search_vector_filtered_exact(
        &self,
        text: &TextIndex,
        query: &[f32],
        filters: &[ScalarFilter],
        k: usize,
    ) -> crate::Result<Vec<(String, usize, f32)>> {
        let ids = text.ids_by_filters(filters)?;
        let documents = self.store.load_ids(&ids)?;
        let mut scores: Vec<(String, f32)> = documents
            .into_iter()
            .filter_map(|doc| {
                doc.vector
                    .map(|vector| (doc.id, cosine_similarity(query, &vector)))
            })
            .collect();
        scores.sort_by(|left, right| {
            right
                .1
                .total_cmp(&left.1)
                .then_with(|| left.0.cmp(&right.0))
        });
        Ok(scores
            .into_iter()
            .take(k)
            .enumerate()
            .map(|(rank, (id, score))| (id, rank + 1, score))
            .collect())
    }

    fn finish_update(
        &self,
        state: &mut DerivedState,
        generation: u64,
        update: crate::Result<()>,
    ) -> crate::Result<()> {
        if let Err(update_error) = update {
            if let Err(rebuild_error) = self.rebuild_state(state) {
                return Err(crate::IndexError::IndexUnavailableAfterCommit {
                    generation,
                    cause: format!(
                        "update failed: {update_error}; rebuild failed: {rebuild_error}"
                    ),
                });
            }
            return Ok(());
        }
        state.generation = generation;
        if state
            .vector
            .as_ref()
            .is_some_and(VectorIndex::should_compact)
        {
            self.rebuild_vector(state)?;
        }
        Ok(())
    }

    fn ensure_synced(&self) -> crate::Result<()> {
        let generation = self.store.generation()?;
        if self.state.read().unwrap().generation == generation {
            return Ok(());
        }
        let mut state = self.state.write().unwrap();
        if state.generation != generation {
            self.rebuild_state(&mut state)?;
        }
        Ok(())
    }

    fn rebuild_state(&self, state: &mut DerivedState) -> crate::Result<()> {
        let documents = self.store.load_all()?;
        validate_documents(&documents, self.vector_config.as_ref())?;
        state.text.clear()?;
        state.text.upsert_batch(&documents)?;
        state.vector = build_vector_index(self.vector_config.as_ref(), &documents)?;
        state.generation = self.store.generation()?;
        Ok(())
    }

    fn rebuild_vector(&self, state: &mut DerivedState) -> crate::Result<()> {
        let documents = self.store.load_all()?;
        state.vector = build_vector_index(self.vector_config.as_ref(), &documents)?;
        Ok(())
    }

    pub fn vector_dimension(&self) -> Option<usize> {
        self.vector_config.as_ref().map(|config| config.dimension)
    }

    pub fn vector_stats(&self) -> Option<(usize, usize)> {
        self.state
            .read()
            .unwrap()
            .vector
            .as_ref()
            .map(VectorIndex::stats)
    }
}

fn validate_config(config: Option<&VectorConfig>) -> crate::Result<()> {
    let Some(config) = config else {
        return Ok(());
    };
    if config.dimension == 0 || config.dimension > MAX_VECTOR_DIMENSION {
        return Err(crate::IndexError::InvalidVector(format!(
            "dimension must be in 1..={MAX_VECTOR_DIMENSION}, got {}",
            config.dimension
        )));
    }
    if config.space_id.trim().is_empty() {
        return Err(crate::IndexError::VectorSpaceMismatch(
            "space_id must not be empty".into(),
        ));
    }
    Ok(())
}

fn validate_documents(docs: &[IndexDoc], config: Option<&VectorConfig>) -> crate::Result<()> {
    for doc in docs {
        if let Some(vector) = doc.vector.as_ref() {
            let config = config.ok_or(crate::IndexError::VectorIndexDisabled)?;
            validate_vector(vector, config.dimension)?;
        }
    }
    Ok(())
}

fn resolve_database_meta(base: &Path, vector: Option<&VectorConfig>) -> crate::Result<()> {
    let path = base.join(DATABASE_META_FILE);
    let requested = DatabaseMeta {
        schema_version: SCHEMA_VERSION,
        metric: "cosine".into(),
        vector: vector.cloned(),
    };
    if path.exists() {
        let stored: DatabaseMeta = serde_json::from_str(&std::fs::read_to_string(&path)?)?;
        if stored != requested {
            return Err(crate::IndexError::VectorSpaceMismatch(format!(
                "stored configuration {stored:?} does not match requested {requested:?}"
            )));
        }
        return Ok(());
    }
    std::fs::write(path, serde_json::to_vec_pretty(&requested)?)?;
    Ok(())
}

fn build_vector_index(
    config: Option<&VectorConfig>,
    documents: &[IndexDoc],
) -> crate::Result<Option<VectorIndex>> {
    let Some(config) = config else {
        return Ok(None);
    };
    let index = VectorIndex::new(config.dimension);
    let vectors: Vec<(&str, &[f32])> = documents
        .iter()
        .filter_map(|doc| {
            doc.vector
                .as_deref()
                .map(|vector| (doc.id.as_str(), vector))
        })
        .collect();
    index.rebuild(vectors)?;
    Ok(Some(index))
}

fn sync_vector(vector: Option<&VectorIndex>, doc: &IndexDoc) -> crate::Result<()> {
    match (vector, &doc.vector) {
        (Some(index), Some(vector)) => index.insert(doc.id.clone(), vector.clone()),
        (Some(index), None) => index.delete(&doc.id),
        (None, Some(_)) => Err(crate::IndexError::VectorIndexDisabled),
        (None, None) => Ok(()),
    }
}

fn merge_rankings(
    text: Option<Vec<(String, usize, f32)>>,
    vector: Option<Vec<(String, usize, f32)>>,
    fusion: Fusion,
    k: usize,
) -> Vec<Hit> {
    match (text, vector) {
        (Some(text), None) => text
            .into_iter()
            .map(|(id, _, score)| Hit {
                id,
                score,
                text_score: Some(score),
                vector_score: None,
            })
            .collect(),
        (None, Some(vector)) => vector
            .into_iter()
            .map(|(id, _, score)| Hit {
                id,
                score,
                text_score: None,
                vector_score: Some(score),
            })
            .collect(),
        (Some(text), Some(vector)) => {
            let text_scores: HashMap<&str, f32> = text
                .iter()
                .map(|(id, _, score)| (id.as_str(), *score))
                .collect();
            let vector_scores: HashMap<&str, f32> = vector
                .iter()
                .map(|(id, _, score)| (id.as_str(), *score))
                .collect();
            let rankings = vec![
                text.iter()
                    .map(|(id, rank, _)| (id.clone(), *rank))
                    .collect(),
                vector
                    .iter()
                    .map(|(id, rank, _)| (id.clone(), *rank))
                    .collect(),
            ];
            let Fusion::Rrf { k: fusion_k } = fusion;
            rrf(&rankings, fusion_k)
                .into_iter()
                .take(k)
                .map(|(id, score)| Hit {
                    text_score: text_scores.get(id.as_str()).copied(),
                    vector_score: vector_scores.get(id.as_str()).copied(),
                    id,
                    score,
                })
                .collect()
        }
        (None, None) => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generation_gap_is_rebuilt_before_query() {
        let index = SemanticIndex::open_in_ram(Some(VectorConfig::new(8, "test:8"))).unwrap();
        let mut vector = vec![0.0; 8];
        vector[2] = 1.0;
        let doc = IndexDoc::new("committed")
            .with_body("confirmado antes del fallo")
            .with_vector(vector.clone());

        // Simula un proceso que confirmó redb y se detuvo antes de actualizar
        // los índices derivados.
        index.store.upsert_batch(&[doc]).unwrap();
        let hits = index
            .query_hybrid(
                HybridQuery::default()
                    .with_text("confirmado")
                    .with_vector(vector)
                    .with_k(1),
            )
            .unwrap();
        assert_eq!(hits[0].id, "committed");
    }
}
