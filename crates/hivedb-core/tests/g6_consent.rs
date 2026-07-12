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

#[test]
fn consent_transitive_authorization() {
    let db = db();
    let _pm_to_lead = db
        .append(consent_granted(
            "PM",
            "Lead",
            scope("deploy", "staging"),
            None,
        ))
        .unwrap();
    let lead_to_backend = db
        .append(consent_granted(
            "Lead",
            "Backend",
            scope("deploy", "staging"),
            None,
        ))
        .unwrap();

    let decision = db.can("Backend", "deploy", "staging").unwrap();
    assert!(decision.allowed());

    let intent = db.read(decision.intent_log_seq().unwrap()).unwrap();
    // `authorized_by` must point to the direct grant that closes the chain.
    assert_eq!(intent.authorized_by(), Some(lead_to_backend));
}

#[test]
fn consent_cycle_does_not_loop_forever() {
    let db = db();
    db.append(consent_granted(
        "PM",
        "Lead",
        scope("deploy", "staging"),
        None,
    ))
    .unwrap();
    db.append(consent_granted(
        "Lead",
        "Backend",
        scope("deploy", "staging"),
        None,
    ))
    .unwrap();
    db.append(consent_granted(
        "Backend",
        "PM",
        scope("deploy", "staging"),
        None,
    ))
    .unwrap();

    // A pure cycle has no root, so it must not authorize. The important thing
    // is that the query terminates instead of looping forever.
    assert!(!db.can("Backend", "deploy", "staging").unwrap().allowed());
}

#[test]
fn transitive_consent_honors_expiration_on_any_link() {
    let db = HiveDB::open_temp_with_clock(Arc::new(MockClock::at(1000))).unwrap();
    db.append(consent_granted(
        "PM",
        "Lead",
        scope("deploy", "staging"),
        Some(1500),
    ))
    .unwrap();
    db.append(consent_granted(
        "Lead",
        "Backend",
        scope("deploy", "staging"),
        None,
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
