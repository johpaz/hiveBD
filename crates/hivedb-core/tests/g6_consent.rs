mod common;

use common::db;
use hivedb_core::{AgentId, EventInput, EventKind, HiveDB, MockClock, Scope, StreamId};
use std::sync::Arc;

#[test]
fn consent_grant_then_query() {
    let db = db();
    db.append(consent_granted(
        "PM",
        "Backend",
        scope("deploy", "staging"),
        None,
    ))
    .unwrap();

    assert!(db.can("Backend", "deploy", "staging").unwrap().allowed());
    assert!(!db.can("Backend", "deploy", "prod").unwrap().allowed());
    assert!(!db.can("Frontend", "deploy", "staging").unwrap().allowed());
}

#[test]
fn consent_revoke_takes_effect() {
    let db = db();
    let grant = db
        .append(consent_granted(
            "PM",
            "Backend",
            scope("deploy", "staging"),
            None,
        ))
        .unwrap();

    assert!(db.can("Backend", "deploy", "staging").unwrap().allowed());
    db.append(consent_revoked(grant)).unwrap();
    assert!(!db.can("Backend", "deploy", "staging").unwrap().allowed());
}

#[test]
fn authorized_action_logs_intent_with_provenance() {
    let db = db();
    let grant = db
        .append(consent_granted(
            "PM",
            "Backend",
            scope("deploy", "staging"),
            None,
        ))
        .unwrap();

    let decision = db.can("Backend", "deploy", "staging").unwrap();
    let intent_seq = decision.intent_log_seq().unwrap();

    let intent = db.read(intent_seq).unwrap();
    assert_eq!(intent.authorized_by(), Some(grant));
}

#[test]
fn expired_consent_does_not_authorize() {
    let db = HiveDB::open_temp_with_clock(Arc::new(MockClock::at(1000))).unwrap();
    db.append(consent_granted(
        "PM",
        "Backend",
        scope("deploy", "staging"),
        Some(1500),
    ))
    .unwrap();

    db.advance_clock_to(2000);
    assert!(!db.can("Backend", "deploy", "staging").unwrap().allowed());
}

fn consent_granted(
    from: impl Into<AgentId>,
    to: impl Into<AgentId>,
    scope: Scope,
    expires: Option<u64>,
) -> EventInput {
    let from = from.into();
    let to = to.into();
    EventInput::new(
        from.clone(),
        StreamId::from("consent"),
        EventKind::ConsentGranted {
            from,
            to,
            scope,
            expires,
        },
    )
}

fn consent_revoked(grant_seq: u64) -> EventInput {
    EventInput::new(
        AgentId::from("PM"),
        StreamId::from("consent"),
        EventKind::ConsentRevoked { grant_seq },
    )
}

fn scope(action: impl Into<String>, resource: impl Into<String>) -> Scope {
    Scope::new(action, resource)
}
