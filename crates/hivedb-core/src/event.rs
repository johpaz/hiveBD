use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Visitor};
use std::fmt;

/// Logical writer partition. Two different agents never block each other on write.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AgentId(pub String);

impl From<&str> for AgentId {
    fn from(value: &str) -> Self {
        AgentId(value.to_string())
    }
}

impl From<String> for AgentId {
    fn from(value: String) -> Self {
        AgentId(value)
    }
}

impl fmt::Display for AgentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Sub-stream within an agent (e.g., a task).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StreamId(pub String);

impl From<&str> for StreamId {
    fn from(value: &str) -> Self {
        StreamId(value.to_string())
    }
}

impl From<String> for StreamId {
    fn from(value: String) -> Self {
        StreamId(value)
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Scope of a consent grant.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Scope {
    pub action: String,
    pub resource: String,
}

impl Scope {
    pub fn new(action: impl Into<String>, resource: impl Into<String>) -> Self {
        Self {
            action: action.into(),
            resource: resource.into(),
        }
    }
}

/// Discriminated event kind. Variants for G1-G6 are included.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum EventKind {
    /// A generic fact known by the agent.
    Fact,

    /// A state transition for a task/agent.
    StateTransition,

    /// Invalidates a previous fact without mutating the log.
    MemoryInvalidate { target_seq: u64 },

    /// A tool invocation.
    ToolCall { tool: String },

    /// Grants consent from one agent to another for a specific scope.
    ConsentGranted {
        from: AgentId,
        to: AgentId,
        scope: Scope,
        expires: Option<u64>,
    },

    /// Revokes a previously granted consent by its sequence number.
    ConsentRevoked { grant_seq: u64 },

    /// Audits an authorization decision.
    IntentLogged {
        actor: AgentId,
        intent: String,
        authorized_by: Option<u64>,
    },
}

impl EventKind {
    /// Returns a string tag used for pattern matching and tests.
    pub fn tag(&self) -> &'static str {
        match self {
            EventKind::Fact => "Fact",
            EventKind::StateTransition => "StateTransition",
            EventKind::MemoryInvalidate { .. } => "MemoryInvalidate",
            EventKind::ToolCall { .. } => "ToolCall",
            EventKind::ConsentGranted { .. } => "ConsentGranted",
            EventKind::ConsentRevoked { .. } => "ConsentRevoked",
            EventKind::IntentLogged { .. } => "IntentLogged",
        }
    }
}

/// Lightweight tag used in reactive subscriptions.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum EventKindTag {
    Fact,
    StateTransition,
    MemoryInvalidate,
    ToolCall,
    ConsentGranted,
    ConsentRevoked,
    IntentLogged,
}

impl From<&EventKind> for EventKindTag {
    fn from(kind: &EventKind) -> Self {
        match kind {
            EventKind::Fact => EventKindTag::Fact,
            EventKind::StateTransition => EventKindTag::StateTransition,
            EventKind::MemoryInvalidate { .. } => EventKindTag::MemoryInvalidate,
            EventKind::ToolCall { .. } => EventKindTag::ToolCall,
            EventKind::ConsentGranted { .. } => EventKindTag::ConsentGranted,
            EventKind::ConsentRevoked { .. } => EventKindTag::ConsentRevoked,
            EventKind::IntentLogged { .. } => EventKindTag::IntentLogged,
        }
    }
}

/// Input provided by the caller to append an event.
///
/// The engine assigns `seq` and `timestamp`; the client cannot set them.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventInput {
    pub agent_id: AgentId,
    pub stream_id: StreamId,
    pub kind: EventKind,
    pub causation: Option<u64>,
    pub correlation: Option<uuid::Uuid>,
    pub payload: serde_json::Value,
}

impl EventInput {
    pub fn new(
        agent_id: impl Into<AgentId>,
        stream_id: impl Into<StreamId>,
        kind: EventKind,
    ) -> Self {
        Self {
            agent_id: agent_id.into(),
            stream_id: stream_id.into(),
            kind,
            causation: None,
            correlation: None,
            payload: serde_json::Value::Null,
        }
    }

    pub fn with_payload(mut self, payload: serde_json::Value) -> Self {
        self.payload = payload;
        self
    }

    pub fn with_causation(mut self, seq: u64) -> Self {
        self.causation = Some(seq);
        self
    }
}

/// A persisted event. Immutable once assigned a `seq`.
#[derive(Clone, Debug, PartialEq)]
pub struct Event {
    pub seq: u64,
    pub agent_id: AgentId,
    pub stream_id: StreamId,
    pub kind: EventKind,
    pub timestamp: u64,
    pub causation: Option<u64>,
    pub correlation: Option<uuid::Uuid>,
    pub payload: serde_json::Value,
}

impl Event {
    /// Returns the kind tag for reactive pattern matching.
    pub fn kind_tag(&self) -> &'static str {
        self.kind.tag()
    }

    /// Returns the grant sequence referenced by an `IntentLogged` event, if any.
    pub fn authorized_by(&self) -> Option<u64> {
        match &self.kind {
            EventKind::IntentLogged { authorized_by, .. } => *authorized_by,
            _ => None,
        }
    }
}

impl Serialize for Event {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("Event", 8)?;
        state.serialize_field("seq", &self.seq)?;
        state.serialize_field("agent_id", &self.agent_id)?;
        state.serialize_field("stream_id", &self.stream_id)?;
        state.serialize_field("kind", &self.kind)?;
        state.serialize_field("timestamp", &self.timestamp)?;
        state.serialize_field("causation", &self.causation)?;
        state.serialize_field("correlation", &self.correlation)?;
        state.serialize_field("payload", &self.payload.to_string())?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for Event {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "snake_case")]
        enum Field {
            Seq,
            AgentId,
            StreamId,
            Kind,
            Timestamp,
            Causation,
            Correlation,
            Payload,
        }

        struct EventVisitor;

        impl<'de> Visitor<'de> for EventVisitor {
            type Value = Event;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct Event")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let seq_val: u64 = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                let agent_id: AgentId = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(1, &self))?;
                let stream_id: StreamId = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(2, &self))?;
                let kind: EventKind = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(3, &self))?;
                let timestamp: u64 = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(4, &self))?;
                let causation: Option<u64> = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(5, &self))?;
                let correlation: Option<uuid::Uuid> = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(6, &self))?;
                let payload_str: String = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(7, &self))?;
                let payload =
                    serde_json::from_str(&payload_str).map_err(serde::de::Error::custom)?;
                Ok(Event {
                    seq: seq_val,
                    agent_id,
                    stream_id,
                    kind,
                    timestamp,
                    causation,
                    correlation,
                    payload,
                })
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut seq = None;
                let mut agent_id = None;
                let mut stream_id = None;
                let mut kind = None;
                let mut timestamp = None;
                let mut causation = None;
                let mut correlation = None;
                let mut payload = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Seq => {
                            if seq.is_some() {
                                return Err(serde::de::Error::duplicate_field("seq"));
                            }
                            seq = Some(map.next_value()?);
                        }
                        Field::AgentId => {
                            if agent_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("agent_id"));
                            }
                            agent_id = Some(map.next_value()?);
                        }
                        Field::StreamId => {
                            if stream_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("stream_id"));
                            }
                            stream_id = Some(map.next_value()?);
                        }
                        Field::Kind => {
                            if kind.is_some() {
                                return Err(serde::de::Error::duplicate_field("kind"));
                            }
                            kind = Some(map.next_value()?);
                        }
                        Field::Timestamp => {
                            if timestamp.is_some() {
                                return Err(serde::de::Error::duplicate_field("timestamp"));
                            }
                            timestamp = Some(map.next_value()?);
                        }
                        Field::Causation => {
                            if causation.is_some() {
                                return Err(serde::de::Error::duplicate_field("causation"));
                            }
                            causation = Some(map.next_value()?);
                        }
                        Field::Correlation => {
                            if correlation.is_some() {
                                return Err(serde::de::Error::duplicate_field("correlation"));
                            }
                            correlation = Some(map.next_value()?);
                        }
                        Field::Payload => {
                            if payload.is_some() {
                                return Err(serde::de::Error::duplicate_field("payload"));
                            }
                            let payload_str: String = map.next_value()?;
                            payload = Some(
                                serde_json::from_str(&payload_str)
                                    .map_err(serde::de::Error::custom)?,
                            );
                        }
                    }
                }
                let seq = seq.ok_or_else(|| serde::de::Error::missing_field("seq"))?;
                let agent_id =
                    agent_id.ok_or_else(|| serde::de::Error::missing_field("agent_id"))?;
                let stream_id =
                    stream_id.ok_or_else(|| serde::de::Error::missing_field("stream_id"))?;
                let kind = kind.ok_or_else(|| serde::de::Error::missing_field("kind"))?;
                let timestamp =
                    timestamp.ok_or_else(|| serde::de::Error::missing_field("timestamp"))?;
                let causation =
                    causation.ok_or_else(|| serde::de::Error::missing_field("causation"))?;
                let correlation =
                    correlation.ok_or_else(|| serde::de::Error::missing_field("correlation"))?;
                let payload = payload.ok_or_else(|| serde::de::Error::missing_field("payload"))?;
                Ok(Event {
                    seq,
                    agent_id,
                    stream_id,
                    kind,
                    timestamp,
                    causation,
                    correlation,
                    payload,
                })
            }
        }

        const FIELDS: &[&str] = &[
            "seq",
            "agent_id",
            "stream_id",
            "kind",
            "timestamp",
            "causation",
            "correlation",
            "payload",
        ];
        deserializer.deserialize_struct("Event", FIELDS, EventVisitor)
    }
}
