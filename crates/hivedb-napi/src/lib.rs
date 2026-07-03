use hivedb_core::{AgentId, Decision, EventInput, EventKind, EventPattern, HiveDB, StreamId};
use hivedb_index::{Fusion, Hit as CoreHit, HybridQuery, ScalarFilter};
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
pub struct JsHybridQuery {
    pub text: Option<String>,
    pub vector: Option<Float32Array>,
    pub k: u32,
    pub filters: Option<Vec<JsScalarFilter>>,
}

#[napi(object)]
pub struct JsHit {
    pub id: String,
    pub score: f64,
}

#[napi(object)]
pub struct JsEventPattern {
    pub agent_id: Option<String>,
    pub kind: Option<String>,
    pub stream_id: Option<String>,
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
        other => return Err(Error::from_reason(format!("unknown event kind: {other}"))),
    };

    Ok(EventInput::new(input.agent_id, input.stream_id, kind).with_payload(payload))
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
    Ok(HybridQuery {
        text,
        vector,
        k: query.k as usize,
        filters,
        fusion: Fusion::Rrf { k: 60 },
    })
}

fn hit_to_js(hit: CoreHit) -> JsHit {
    JsHit {
        id: hit.id,
        score: hit.score as f64,
    }
}

fn decision_to_js(decision: Decision) -> JsDecision {
    JsDecision {
        allowed: decision.allowed(),
        intent_log_seq: decision.intent_log_seq().map(|v| v as i64),
    }
}

fn js_to_event_pattern(pattern: JsEventPattern) -> Result<EventPattern> {
    use hivedb_core::EventKindTag;

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

    Ok(EventPattern {
        agent_id: pattern.agent_id.map(AgentId::from),
        kind,
        stream_id: pattern.stream_id.map(StreamId::from),
        predicate: None,
    })
}

#[napi]
pub struct JsHiveDB {
    inner: Mutex<Option<Arc<HiveDB>>>,
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
    pub async fn open(path: String) -> Result<Self> {
        let db = HiveDB::open(path).map_err(js_err)?;
        Ok(Self {
            inner: Mutex::new(Some(Arc::new(db))),
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

        let handle = tokio::spawn(async move {
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
