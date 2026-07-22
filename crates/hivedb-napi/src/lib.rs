use hivedb_core::{
    AgentContextRequest, AgentId, ColOp, Decision, DocEntry, EventInput, EventKind, EventPattern,
    HarnessInput, HarnessLoop, HiveDB, OpenOptions, PutOptions, ScanOptions, StreamId, ToolStats,
    VectorOptions,
};
use hivedb_index::{FieldBoosts, Fusion, Hit as CoreHit, HybridQuery, IndexDoc, ScalarFilter};
use napi::bindgen_prelude::*;
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi_derive::*;
use serde_json::Value;
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;

#[napi(object)]
pub struct JsEventInput {
    pub agent_id: String,
    pub stream_id: String,
    pub kind: String,
    pub payload: String,
    pub causation: Option<i64>,
    /// UUID string. Links this event to others sharing the same objective/
    /// intent, so `causalThread()`'s objectiveDrift detector can tell a
    /// decision apart from the stream's original `IntentLogged` correlation.
    pub correlation: Option<String>,
}

#[napi(object)]
pub struct JsEvent {
    pub seq: i64,
    pub agent_id: String,
    pub stream_id: String,
    pub kind_tag: String,
    pub timestamp: i64,
    pub causation: Option<i64>,
    pub correlation: Option<String>,
    pub payload: String,
}

#[napi(object)]
pub struct JsDecision {
    pub allowed: bool,
    pub intent_log_seq: Option<i64>,
}

#[napi(object)]
pub struct JsScalarFilter {
    pub field: String,
    pub value: String,
}

#[napi(object)]
pub struct JsFusion {
    /// Fusion strategy. Only `"rrf"` is supported.
    pub kind: String,
    /// RRF `k` parameter (default 60).
    pub k: Option<u32>,
}

#[napi(object)]
pub struct JsFieldBoosts {
    pub name: Option<f64>,
    pub body: Option<f64>,
    pub tags: Option<f64>,
}

#[napi(object)]
pub struct JsToolStats {
    pub invocations: i64,
    pub errors: i64,
    pub total_latency_ms: i64,
    pub total_cost: f64,
    pub last_outcome: Option<String>,
    pub last_seq: i64,
}

#[napi(object)]
pub struct JsHybridQuery {
    pub text: Option<String>,
    pub vector: Option<Float32Array>,
    pub k: u32,
    pub filters: Option<Vec<JsScalarFilter>>,
    pub fusion: Option<JsFusion>,
    pub boosts: Option<JsFieldBoosts>,
}

#[napi(object)]
pub struct JsHit {
    pub id: String,
    pub score: f64,
    pub text_score: Option<f64>,
    pub vector_score: Option<f64>,
}

#[napi(object)]
pub struct JsIndexDoc {
    pub id: String,
    pub name: Option<String>,
    pub body: Option<String>,
    pub tags: Option<String>,
    pub vector: Option<Float32Array>,
    pub filters: Option<Vec<JsScalarFilter>>,
}

#[napi(object)]
pub struct JsVectorOptions {
    pub dimension: u32,
    pub space_id: String,
}

#[napi(object)]
pub struct JsOpenOptions {
    /// Omitir para usar el modo solo texto.
    pub vector: Option<JsVectorOptions>,
}

#[napi(object)]
pub struct JsDocEntry {
    pub id: String,
    /// Monotonic per-document version, starting at 1 on first put.
    pub version: i64,
    /// The stored document as a JSON string.
    pub json: String,
}

#[napi(object)]
pub struct JsPutOptions {
    /// Optimistic concurrency: current version must equal this value
    /// (0 = the document must not exist yet). Omit for unconditional upsert.
    pub expected_version: Option<i64>,
}

#[napi(object)]
pub struct JsScanOptions {
    pub prefix: Option<String>,
    pub start: Option<String>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    pub reverse: Option<bool>,
}

#[napi(object)]
pub struct JsColOp {
    /// "put" | "delete"
    pub op: String,
    pub collection: String,
    pub id: String,
    /// JSON document (required for "put").
    pub json: Option<String>,
    pub expected_version: Option<i64>,
}

#[napi(object)]
pub struct JsEventPattern {
    pub agent_id: Option<String>,
    pub kind: Option<String>,
    pub stream_id: Option<String>,
    pub predicate: Option<JsPredicate>,
}

#[napi(object)]
pub struct JsPredicate {
    /// `"Eq"` | `"Contains"` | `"Always"`.
    pub kind: String,
    /// JSON pointer path inside the payload (e.g. `"/temperature"` or `"temperature"`).
    pub path: Option<String>,
    /// JSON-encoded value used by `Eq` and `Contains`.
    pub value: Option<String>,
}

fn js_err<E: std::fmt::Display>(e: E) -> Error {
    Error::from_reason(e.to_string())
}

fn parse_payload(payload: &str) -> Result<Value> {
    serde_json::from_str(payload).map_err(|e| Error::from_reason(format!("invalid payload: {e}")))
}

fn js_to_event_input(input: JsEventInput) -> Result<EventInput> {
    let payload = parse_payload(&input.payload)?;
    let kind = match input.kind.as_str() {
        "Fact" => EventKind::Fact,
        "StateTransition" => EventKind::StateTransition,
        "MemoryInvalidate" => {
            let target_seq = payload
                .get("target_seq")
                .and_then(|v| v.as_u64())
                .ok_or_else(|| {
                    Error::from_reason("MemoryInvalidate requires payload.target_seq")
                })?;
            EventKind::MemoryInvalidate { target_seq }
        }
        "ToolCall" => {
            let tool = payload
                .get("tool")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .ok_or_else(|| Error::from_reason("ToolCall requires payload.tool"))?;
            EventKind::ToolCall { tool }
        }
        "ConsentGranted" => {
            let from = payload
                .get("from")
                .and_then(|v| v.as_str())
                .map(AgentId::from)
                .ok_or_else(|| Error::from_reason("ConsentGranted requires payload.from"))?;
            let to = payload
                .get("to")
                .and_then(|v| v.as_str())
                .map(AgentId::from)
                .ok_or_else(|| Error::from_reason("ConsentGranted requires payload.to"))?;
            let action = payload
                .get("action")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .ok_or_else(|| Error::from_reason("ConsentGranted requires payload.action"))?;
            let resource = payload
                .get("resource")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .ok_or_else(|| Error::from_reason("ConsentGranted requires payload.resource"))?;
            let expires = payload.get("expires").and_then(|v| v.as_u64());
            EventKind::ConsentGranted {
                from,
                to,
                scope: hivedb_core::Scope::new(action, resource),
                expires,
            }
        }
        "ConsentRevoked" => {
            let grant_seq = payload
                .get("grant_seq")
                .and_then(|v| v.as_u64())
                .ok_or_else(|| Error::from_reason("ConsentRevoked requires payload.grant_seq"))?;
            EventKind::ConsentRevoked { grant_seq }
        }
        "IntentLogged" => {
            let actor = payload
                .get("actor")
                .and_then(|v| v.as_str())
                .map(AgentId::from)
                .ok_or_else(|| Error::from_reason("IntentLogged requires payload.actor"))?;
            let intent = payload
                .get("intent")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .ok_or_else(|| Error::from_reason("IntentLogged requires payload.intent"))?;
            let authorized_by = payload.get("authorized_by").and_then(|v| v.as_u64());
            EventKind::IntentLogged {
                actor,
                intent,
                authorized_by,
            }
        }
        "LearningProposal" => EventKind::LearningProposal,
        other => return Err(Error::from_reason(format!("unknown event kind: {other}"))),
    };

    let mut event_input =
        EventInput::new(input.agent_id, input.stream_id, kind).with_payload(payload);
    if let Some(seq) = input.causation {
        event_input = event_input.with_causation(seq as u64);
    }
    if let Some(corr) = input.correlation {
        let parsed = uuid::Uuid::parse_str(&corr)
            .map_err(|e| Error::from_reason(format!("invalid correlation UUID: {e}")))?;
        event_input.correlation = Some(parsed);
    }
    Ok(event_input)
}

fn event_to_js(event: &hivedb_core::Event) -> JsEvent {
    JsEvent {
        seq: event.seq as i64,
        agent_id: event.agent_id.0.clone(),
        stream_id: event.stream_id.0.clone(),
        kind_tag: event.kind_tag().to_string(),
        timestamp: event.timestamp as i64,
        causation: event.causation.map(|v| v as i64),
        correlation: event.correlation.map(|u| u.to_string()),
        payload: event.payload.to_string(),
    }
}

fn js_to_scalar_filter(filter: JsScalarFilter) -> hivedb_index::ScalarFilter {
    hivedb_index::ScalarFilter::Eq {
        field: filter.field,
        value: filter.value,
    }
}

fn js_to_hybrid_query(query: JsHybridQuery) -> Result<HybridQuery> {
    let text = query.text;
    let vector = query.vector.map(|v| v.to_vec());
    let filters = query
        .filters
        .map(|fs| fs.into_iter().map(js_to_scalar_filter).collect())
        .unwrap_or_default();

    let fusion = match query.fusion {
        Some(fusion) => match fusion.kind.as_str() {
            "rrf" => Fusion::Rrf {
                k: fusion.k.unwrap_or(60) as usize,
            },
            other => {
                return Err(Error::from_reason(format!(
                    "unknown fusion kind: {other} (only \"rrf\" is supported)"
                )));
            }
        },
        None => Fusion::default(),
    };

    let boosts = query.boosts.map(|b| {
        let defaults = FieldBoosts::default();
        FieldBoosts {
            name: b.name.map(|v| v as f32).unwrap_or(defaults.name),
            body: b.body.map(|v| v as f32).unwrap_or(defaults.body),
            tags: b.tags.map(|v| v as f32).unwrap_or(defaults.tags),
        }
    });

    Ok(HybridQuery {
        text,
        vector,
        k: query.k as usize,
        filters,
        fusion,
        boosts,
    })
}

fn js_to_index_doc(doc: JsIndexDoc) -> IndexDoc {
    IndexDoc {
        id: doc.id,
        name: doc.name,
        body: doc.body,
        tags: doc.tags,
        vector: doc.vector.map(|v| v.to_vec()),
        filters: doc
            .filters
            .map(|fs| fs.into_iter().map(js_to_scalar_filter).collect())
            .unwrap_or_default(),
    }
}

fn hit_to_js(hit: CoreHit) -> JsHit {
    JsHit {
        id: hit.id,
        score: hit.score as f64,
        text_score: hit.text_score.map(|s| s as f64),
        vector_score: hit.vector_score.map(|s| s as f64),
    }
}

fn decision_to_js(decision: Decision) -> JsDecision {
    JsDecision {
        allowed: decision.allowed(),
        intent_log_seq: decision.intent_log_seq().map(|v| v as i64),
    }
}

fn tool_stats_to_js(stats: ToolStats) -> JsToolStats {
    JsToolStats {
        invocations: stats.invocations as i64,
        errors: stats.errors as i64,
        total_latency_ms: stats.total_latency_ms as i64,
        total_cost: stats.total_cost,
        last_outcome: stats.last_outcome,
        last_seq: stats.last_seq as i64,
    }
}

fn js_to_event_pattern(pattern: JsEventPattern) -> Result<EventPattern> {
    use hivedb_core::{EventKindTag, Predicate};

    let kind = match pattern.kind.as_deref() {
        Some("Fact") => Some(EventKindTag::Fact),
        Some("StateTransition") => Some(EventKindTag::StateTransition),
        Some("MemoryInvalidate") => Some(EventKindTag::MemoryInvalidate),
        Some("ToolCall") => Some(EventKindTag::ToolCall),
        Some("ConsentGranted") => Some(EventKindTag::ConsentGranted),
        Some("ConsentRevoked") => Some(EventKindTag::ConsentRevoked),
        Some("IntentLogged") => Some(EventKindTag::IntentLogged),
        Some(other) => return Err(Error::from_reason(format!("unknown kind: {other}"))),
        None => None,
    };

    let predicate = match pattern.predicate {
        Some(p) => match p.kind.as_str() {
            "Eq" => {
                let path = p
                    .path
                    .ok_or_else(|| Error::from_reason("Eq predicate requires path"))?;
                let value_json = p
                    .value
                    .ok_or_else(|| Error::from_reason("Eq predicate requires value"))?;
                let value = parse_payload(&value_json)?;
                Some(Predicate::Eq { path, value })
            }
            "Contains" => {
                let path = p
                    .path
                    .ok_or_else(|| Error::from_reason("Contains predicate requires path"))?;
                let value_json = p
                    .value
                    .ok_or_else(|| Error::from_reason("Contains predicate requires value"))?;
                let value = parse_payload(&value_json)?;
                Some(Predicate::Contains { path, value })
            }
            "Always" => Some(Predicate::Always),
            other => {
                return Err(Error::from_reason(format!(
                    "unknown predicate kind: {other} (expected Eq, Contains or Always)"
                )));
            }
        },
        None => None,
    };

    Ok(EventPattern {
        agent_id: pattern.agent_id.map(AgentId::from),
        kind,
        stream_id: pattern.stream_id.map(StreamId::from),
        predicate,
    })
}

fn doc_entry_to_js(entry: DocEntry) -> JsDocEntry {
    JsDocEntry {
        id: entry.id,
        version: entry.version as i64,
        json: entry.doc.to_string(),
    }
}

fn js_to_scan_options(options: Option<JsScanOptions>) -> ScanOptions {
    let options = options.unwrap_or(JsScanOptions {
        prefix: None,
        start: None,
        limit: None,
        offset: None,
        reverse: None,
    });
    ScanOptions {
        prefix: options.prefix,
        start: options.start,
        limit: options.limit.unwrap_or(0) as usize,
        offset: options.offset.unwrap_or(0) as usize,
        reverse: options.reverse.unwrap_or(false),
    }
}

fn parse_json_doc(json: &str) -> Result<serde_json::Value> {
    serde_json::from_str(json)
        .map_err(|e| Error::from_reason(format!("invalid document JSON: {e}")))
}

#[napi]
pub struct JsHiveDB {
    inner: Mutex<Option<Arc<HiveDB>>>,
    runtime: tokio::runtime::Handle,
}

impl JsHiveDB {
    fn with_db<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&HiveDB) -> Result<T>,
    {
        let lock = self
            .inner
            .lock()
            .map_err(|_| Error::from_reason("database lock poisoned"))?;
        match lock.as_ref() {
            Some(db) => f(db),
            None => Err(Error::from_reason("database is closed")),
        }
    }

    fn db_arc(&self) -> Result<Arc<HiveDB>> {
        let lock = self
            .inner
            .lock()
            .map_err(|_| Error::from_reason("database lock poisoned"))?;
        lock.clone()
            .ok_or_else(|| Error::from_reason("database is closed"))
    }
}

#[napi]
impl JsHiveDB {
    #[napi(factory)]
    pub async fn open(path: String, options: Option<JsOpenOptions>) -> Result<Self> {
        let open_options = OpenOptions {
            vector: options
                .and_then(|o| o.vector)
                .map(|vector| VectorOptions::new(vector.dimension as usize, vector.space_id)),
        };
        // ":memory:" opens an ephemeral database backed by a process-lifetime
        // temporary directory, so tests never touch persistent storage.
        let db = if path == ":memory:" {
            HiveDB::open_temp_with_options(open_options).map_err(js_err)?
        } else {
            HiveDB::open_with_options(path, open_options).map_err(js_err)?
        };
        Ok(Self {
            inner: Mutex::new(Some(Arc::new(db))),
            runtime: tokio::runtime::Handle::current(),
        })
    }

    #[napi]
    pub async fn append(&self, input: JsEventInput) -> Result<i64> {
        let event_input = js_to_event_input(input)?;
        self.with_db(|db| db.append(event_input).map_err(js_err).map(|seq| seq as i64))
    }

    #[napi]
    pub async fn read(&self, seq: i64) -> Result<JsEvent> {
        if seq < 0 {
            return Err(Error::from_reason("seq must be non-negative"));
        }
        self.with_db(|db| {
            let event = db.read(seq as u64).map_err(js_err)?;
            Ok(event_to_js(&event))
        })
    }

    #[napi]
    pub async fn log_len(&self) -> Result<i64> {
        self.with_db(|db| db.log_len().map_err(js_err).map(|len| len as i64))
    }

    #[napi(js_name = "lastSeq")]
    pub async fn last_seq(&self) -> Result<i64> {
        self.with_db(|db| db.last_seq().map_err(js_err).map(|seq| seq as i64))
    }

    #[napi(js_name = "toolStats")]
    pub async fn tool_stats(&self, tool: String) -> Result<Option<JsToolStats>> {
        self.with_db(|db| {
            let stats = db.tool_stats(&tool).map_err(js_err)?;
            Ok(stats.map(tool_stats_to_js))
        })
    }

    #[napi]
    pub async fn project_task_state(
        &self,
        agent_id: String,
        stream_id: String,
    ) -> Result<Option<String>> {
        use hivedb_core::{TaskState, TaskStateState};
        self.with_db(|db| {
            let state: TaskStateState = db.project::<TaskState>().map_err(js_err)?;
            Ok(state
                .get(&AgentId::from(agent_id), &StreamId::from(stream_id))
                .map(|v| v.to_string()))
        })
    }

    #[napi]
    pub async fn can(&self, agent: String, action: String, resource: String) -> Result<JsDecision> {
        self.with_db(|db| {
            let decision = db.can(agent, action, resource).map_err(js_err)?;
            Ok(decision_to_js(decision))
        })
    }

    /// Store a value in working memory with an optional TTL (milliseconds).
    #[napi(js_name = "workingSet")]
    pub async fn working_set(
        &self,
        agent_id: String,
        key: String,
        json: String,
        ttl_ms: Option<i64>,
    ) -> Result<()> {
        let value = parse_payload(&json)?;
        let ttl = ttl_ms.map(|ms| std::time::Duration::from_millis(ms.max(0) as u64));
        self.with_db(|db| {
            db.working_set(agent_id, key, value, ttl);
            Ok(())
        })
    }

    /// Retrieve a value from working memory, returning `None` if expired or missing.
    #[napi(js_name = "workingGet")]
    pub async fn working_get(&self, agent_id: String, key: String) -> Result<Option<String>> {
        self.with_db(|db| {
            Ok(db
                .working_get(agent_id.clone(), &key)
                .map(|value| value.to_string()))
        })
    }

    /// Return all non-expired keys for an agent.
    #[napi(js_name = "workingKeys")]
    pub async fn working_keys(&self, agent_id: String) -> Result<Vec<String>> {
        self.with_db(|db| Ok(db.working_keys(agent_id.clone())))
    }

    /// Build the causal thread for a task as a JSON string.
    #[napi(js_name = "causalThread")]
    pub async fn causal_thread(&self, stream_id: String) -> Result<String> {
        self.with_db(|db| {
            let thread = db.causal_thread(stream_id).map_err(js_err)?;
            serde_json::to_string(&thread)
                .map_err(|e| Error::from_reason(format!("serialization error: {e}")))
        })
    }

    /// Build an agent context window for a task as a JSON string.
    #[napi(js_name = "buildAgentContext")]
    pub async fn build_agent_context(&self, req_json: String) -> Result<String> {
        self.with_db(|db| {
            let req: AgentContextRequest = serde_json::from_str(&req_json)
                .map_err(|e| Error::from_reason(format!("invalid request: {e}")))?;
            let ctx = db.build_agent_context(req).map_err(js_err)?;
            serde_json::to_string(&ctx)
                .map_err(|e| Error::from_reason(format!("serialization error: {e}")))
        })
    }

    /// Evaluate a task with the harness loop. Input and output are JSON strings.
    #[napi(js_name = "evaluateHarness")]
    pub async fn evaluate_harness(&self, input_json: String) -> Result<String> {
        let input: HarnessInput = serde_json::from_str(&input_json)
            .map_err(|e| Error::from_reason(format!("invalid input: {e}")))?;
        let eval = HarnessLoop::evaluate(input);
        serde_json::to_string(&eval)
            .map_err(|e| Error::from_reason(format!("serialization error: {e}")))
    }

    /// Deprecated: use `upsertDoc`. Kept for one version; `text` maps to the
    /// `body` field.
    #[napi]
    pub async fn index_doc(
        &self,
        id: String,
        text: String,
        vector: Float32Array,
        filters: Option<Vec<JsScalarFilter>>,
    ) -> Result<()> {
        let vector = vector.to_vec();
        let filters: Vec<ScalarFilter> = filters
            .map(|fs| fs.into_iter().map(js_to_scalar_filter).collect::<Vec<_>>())
            .unwrap_or_default();
        self.with_db(|db| {
            db.index_doc_with(id, text, vector, &filters)
                .map_err(js_err)
        })
    }

    /// Insert or replace a document in the semantic index.
    #[napi]
    pub async fn upsert_doc(&self, doc: JsIndexDoc) -> Result<()> {
        let doc = js_to_index_doc(doc);
        self.with_db(|db| db.upsert_doc(&doc).map_err(js_err))
    }

    /// Insert or replace a batch of documents under a single text-index
    /// commit. Much faster than repeated `upsertDoc` calls.
    #[napi]
    pub async fn upsert_batch(&self, docs: Vec<JsIndexDoc>) -> Result<()> {
        let docs: Vec<IndexDoc> = docs.into_iter().map(js_to_index_doc).collect();
        self.with_db(|db| db.upsert_batch(&docs).map_err(js_err))
    }

    /// Delete a document from the semantic index. Missing ids are a no-op.
    #[napi]
    pub async fn delete_doc(&self, id: String) -> Result<()> {
        self.with_db(|db| db.delete_doc(&id).map_err(js_err))
    }

    /// Delete every indexed document carrying the given scalar filter.
    #[napi]
    pub async fn delete_by_filter(&self, filter: JsScalarFilter) -> Result<()> {
        let filter = js_to_scalar_filter(filter);
        self.with_db(|db| db.delete_by_filter(&filter).map_err(js_err))
    }

    /// Remove every document from the semantic index.
    #[napi]
    pub async fn clear_index(&self) -> Result<()> {
        self.with_db(|db| db.clear_index().map_err(js_err))
    }

    /// Reconstruye los índices semánticos desde sus documentos autoritativos.
    #[napi]
    pub async fn compact_index(&self) -> Result<()> {
        self.with_db(|db| db.compact_index().map_err(js_err))
    }

    /// Insert or replace a JSON document in a collection. Returns the new
    /// version (starts at 1).
    #[napi]
    pub async fn col_put(
        &self,
        collection: String,
        id: String,
        json: String,
        options: Option<JsPutOptions>,
    ) -> Result<i64> {
        let doc = parse_json_doc(&json)?;
        let put_options = PutOptions {
            expected_version: options.and_then(|o| o.expected_version).map(|v| v as u64),
        };
        self.with_db(|db| {
            db.col_put(&collection, &id, &doc, put_options)
                .map_err(js_err)
                .map(|v| v as i64)
        })
    }

    /// Read a document by id.
    #[napi]
    pub async fn col_get(&self, collection: String, id: String) -> Result<Option<JsDocEntry>> {
        self.with_db(|db| {
            Ok(db
                .col_get(&collection, &id)
                .map_err(js_err)?
                .map(doc_entry_to_js))
        })
    }

    /// Delete a document. Returns true if it existed.
    #[napi]
    pub async fn col_delete(&self, collection: String, id: String) -> Result<bool> {
        self.with_db(|db| db.col_delete(&collection, &id).map_err(js_err))
    }

    /// Scan a collection in id order.
    #[napi]
    pub async fn col_scan(
        &self,
        collection: String,
        options: Option<JsScanOptions>,
    ) -> Result<Vec<JsDocEntry>> {
        let scan = js_to_scan_options(options);
        self.with_db(|db| {
            Ok(db
                .col_scan(&collection, &scan)
                .map_err(js_err)?
                .into_iter()
                .map(doc_entry_to_js)
                .collect())
        })
    }

    /// Number of documents in a collection.
    #[napi]
    pub async fn col_count(&self, collection: String) -> Result<i64> {
        self.with_db(|db| db.col_count(&collection).map_err(js_err).map(|c| c as i64))
    }

    /// Create an equality index on a top-level field (optionally unique).
    /// Backfills existing documents; idempotent for an identical definition.
    #[napi]
    pub async fn col_create_index(
        &self,
        collection: String,
        field: String,
        unique: bool,
    ) -> Result<()> {
        self.with_db(|db| {
            db.col_create_index(&collection, &field, unique)
                .map_err(js_err)
        })
    }

    /// Look up documents whose indexed field equals the given JSON scalar
    /// (e.g. `"\"abc\""`, `"42"`, `"true"`). Requires colCreateIndex first.
    #[napi]
    pub async fn col_find_by(
        &self,
        collection: String,
        field: String,
        value_json: String,
        options: Option<JsScanOptions>,
    ) -> Result<Vec<JsDocEntry>> {
        let value = parse_json_doc(&value_json)?;
        let scan = js_to_scan_options(options);
        self.with_db(|db| {
            Ok(db
                .col_find_by(&collection, &field, &value, &scan)
                .map_err(js_err)?
                .into_iter()
                .map(doc_entry_to_js)
                .collect())
        })
    }

    /// Apply several puts/deletes atomically: either every operation commits
    /// or none does.
    #[napi]
    pub async fn col_batch(&self, ops: Vec<JsColOp>) -> Result<()> {
        let mut parsed: Vec<ColOp> = Vec::with_capacity(ops.len());
        for op in ops {
            match op.op.as_str() {
                "put" => {
                    let json = op
                        .json
                        .ok_or_else(|| Error::from_reason("batch put requires the json field"))?;
                    parsed.push(ColOp::Put {
                        collection: op.collection,
                        id: op.id,
                        doc: parse_json_doc(&json)?,
                        expected_version: op.expected_version.map(|v| v as u64),
                    });
                }
                "delete" => parsed.push(ColOp::Delete {
                    collection: op.collection,
                    id: op.id,
                }),
                other => {
                    return Err(Error::from_reason(format!(
                        "unknown batch op: {other} (expected \"put\" or \"delete\")"
                    )));
                }
            }
        }
        self.with_db(|db| db.col_batch(&parsed).map_err(js_err))
    }

    #[napi]
    pub async fn query_hybrid(&self, query: JsHybridQuery) -> Result<Vec<JsHit>> {
        let query = js_to_hybrid_query(query)?;
        self.with_db(|db| {
            let hits = db.query_hybrid(query).map_err(js_err)?;
            Ok(hits.into_iter().map(hit_to_js).collect())
        })
    }

    #[napi]
    pub fn subscribe(
        &self,
        pattern: JsEventPattern,
        callback: ThreadsafeFunction<JsEvent>,
    ) -> Result<JsSubscription> {
        let pattern = js_to_event_pattern(pattern)?;
        let db = self.db_arc()?;
        let subscription = db.subscribe(pattern);

        let handle = self.runtime.spawn(async move {
            let mut subscription = subscription;
            while let Some(event) = subscription.next().await {
                let js_event = event_to_js(&event);
                if callback.call(Ok(js_event), ThreadsafeFunctionCallMode::NonBlocking)
                    != napi::Status::Ok
                {
                    break;
                }
            }
        });

        Ok(JsSubscription { handle })
    }

    #[napi]
    pub fn close(&mut self) {
        let mut lock = self.inner.lock().unwrap();
        *lock = None;
    }
}

#[napi]
pub struct JsSubscription {
    handle: JoinHandle<()>,
}

#[napi]
impl JsSubscription {
    #[napi]
    pub fn close(&mut self) {
        self.handle.abort();
    }
}

impl Drop for JsSubscription {
    fn drop(&mut self) {
        self.handle.abort();
    }
}
