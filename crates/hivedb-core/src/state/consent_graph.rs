use crate::event::{AgentId, Event, EventKind, Scope};
use crate::projection::{Projection, ProjectionScope};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};

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
    /// Returns `None` if no matching, non-expired grant exists. Delegation
    /// chains are followed transitively: a grant `A → B` and another grant
    /// `B → C` authorize `C` as long as every link in the chain is valid for
    /// the same scope and has not expired.
    ///
    /// The returned sequence number is always the **direct** grant closest to
    /// the requesting agent (`to`), which preserves the existing audit
    /// contract for `IntentLogged.authorized_by`.
    pub fn find_active_grant(
        &self,
        to: &AgentId,
        action: &str,
        resource: &str,
        now_ms: u64,
    ) -> Option<u64> {
        let mut visited = HashSet::new();
        self.find_active_grant_rec(to, action, resource, now_ms, &mut visited)
    }

    fn find_active_grant_rec(
        &self,
        current: &AgentId,
        action: &str,
        resource: &str,
        now_ms: u64,
        visited: &mut HashSet<AgentId>,
    ) -> Option<u64> {
        if !visited.insert(current.clone()) {
            return None;
        }

        for (seq, grant) in self.grants.iter().filter(|(_, g)| {
            g.to == *current
                && g.scope.action == action
                && g.scope.resource == resource
                && !is_expired(g.expires, now_ms)
        }) {
            // A grantor is a root for this scope only if it has never received
            // a grant for the same scope (expired or not). If it has, it must
            // itself be authorized through a valid, non-expired chain.
            let grantor_has_incoming = self.grants.values().any(|g| {
                g.to == grant.from && g.scope.action == action && g.scope.resource == resource
            });

            if !grantor_has_incoming {
                return Some(*seq);
            }

            if self
                .find_active_grant_rec(&grant.from, action, resource, now_ms, visited)
                .is_some()
            {
                return Some(*seq);
            }
        }

        None
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
