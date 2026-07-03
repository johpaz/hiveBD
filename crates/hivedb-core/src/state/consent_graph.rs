use crate::event::{AgentId, Event, EventKind, Scope};
use crate::projection::{Projection, ProjectionScope};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// State of the consent graph projection.
///
/// Keeps a map of active grants indexed by the sequence number of the
/// `ConsentGranted` event that created them. Revocation removes the entry.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ConsentGraphState {
    grants: BTreeMap<u64, Grant>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Grant {
    from: AgentId,
    to: AgentId,
    scope: Scope,
    expires: Option<u64>,
}

impl ConsentGraphState {
    /// Find the sequence number of an active grant that authorizes `to` to
    /// perform `action` on `resource` at the given timestamp.
    ///
    /// Returns `None` if no matching, non-expired grant exists.
    pub fn find_active_grant(
        &self,
        to: &AgentId,
        action: &str,
        resource: &str,
        now_ms: u64,
    ) -> Option<u64> {
        self.grants
            .iter()
            .find(|(_, grant)| {
                grant.to == *to
                    && grant.scope.action == action
                    && grant.scope.resource == resource
                    && !is_expired(grant.expires, now_ms)
            })
            .map(|(seq, _)| *seq)
    }

    /// Returns true if there is at least one active grant for `to` matching
    /// the given action/resource.
    pub fn is_authorized(&self, to: &AgentId, action: &str, resource: &str, now_ms: u64) -> bool {
        self.find_active_grant(to, action, resource, now_ms)
            .is_some()
    }
}

fn is_expired(expires: Option<u64>, now_ms: u64) -> bool {
    expires.map(|e| now_ms >= e).unwrap_or(false)
}

/// Marker type for the `ConsentGraph` projection.
pub struct ConsentGraph;

impl Projection for ConsentGraph {
    type State = ConsentGraphState;

    fn name() -> &'static str {
        "ConsentGraph"
    }

    fn scope() -> ProjectionScope {
        ProjectionScope::Global
    }

    fn apply(state: &mut Self::State, event: &Event) {
        match &event.kind {
            EventKind::ConsentGranted {
                from,
                to,
                scope,
                expires,
            } => {
                state.grants.insert(
                    event.seq,
                    Grant {
                        from: from.clone(),
                        to: to.clone(),
                        scope: scope.clone(),
                        expires: *expires,
                    },
                );
            }
            EventKind::ConsentRevoked { grant_seq } => {
                state.grants.remove(grant_seq);
            }
            _ => {}
        }
    }
}
