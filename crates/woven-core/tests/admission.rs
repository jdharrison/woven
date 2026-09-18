#![allow(clippy::manual_let_else)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use woven_core::{
    AdmissionController, AdmissionMetadata, CapacityUpdate, IdempotencyKey, JoinDecision,
    JoinRequest, NamespaceId, NodeId, PrincipalId, QueuePolicy, QueueStatus, QueueTicketId,
    RejectionReason, ReleaseReason, SessionId, SessionKey, UsageCounters,
};

fn metadata() -> AdmissionMetadata {
    AdmissionMetadata {
        node_id: NodeId::new(1),
        session: SessionKey::new(NamespaceId::new(1), SessionId::new(1)),
    }
}

fn policy() -> QueuePolicy {
    QueuePolicy {
        max_depth: 16,
        heartbeat_timeout: Duration::from_secs(60),
        ..QueuePolicy::default()
    }
}

fn controller(capacity: u32) -> AdmissionController {
    AdmissionController::new(
        metadata(),
        policy(),
        CapacityUpdate {
            allocated_ccu: capacity,
            revision: 1,
        },
        Arc::new(UsageCounters::new(metadata())),
    )
}

fn join(principal: u64, key: &str) -> JoinRequest {
    JoinRequest::new(
        PrincipalId::new(principal),
        IdempotencyKey::new(key).unwrap(),
    )
}

#[test]
fn ten_slots_admit_ten_and_queue_five() {
    let now = Instant::now();
    let mut controller = controller(10);
    let mut admitted = 0;
    let mut queued = 0;
    for value in 1..=15 {
        match controller.request_join_at(join(value, "key"), now) {
            JoinDecision::Admitted(_) => admitted += 1,
            JoinDecision::Queued(_) => queued += 1,
            decision => panic!("unexpected {decision:?}"),
        }
    }
    assert_eq!((admitted, queued), (10, 5));
    assert_eq!(controller.snapshot().active_ccu, 10);
}

#[test]
fn cancellation_reclaims_bounded_queue_capacity() {
    let now = Instant::now();
    let mut controller = controller(1);
    let _ = controller.request_join_at(join(1, "a"), now);
    let ticket = match controller.request_join_at(join(2, "b"), now) {
        JoinDecision::Queued(ticket) => ticket,
        decision => panic!("unexpected {decision:?}"),
    };

    assert_eq!(
        controller.cancel_at(ticket.id, now),
        woven_core::CancelResult::Cancelled
    );
    assert!(matches!(
        controller.request_join_at(join(3, "c"), now),
        JoinDecision::Queued(_)
    ));
}

#[test]
fn capacity_decrease_waits_for_reconnect_reservations() {
    let now = Instant::now();
    let mut controller = controller(2);
    let first = match controller.request_join_at(join(1, "a"), now) {
        JoinDecision::Admitted(lease) => lease,
        decision => panic!("unexpected {decision:?}"),
    };
    let second = match controller.request_join_at(join(2, "b"), now) {
        JoinDecision::Admitted(lease) => lease,
        decision => panic!("unexpected {decision:?}"),
    };

    controller.release_at(first, ReleaseReason::Unexpected, now);
    controller.apply_capacity_at(
        CapacityUpdate {
            allocated_ccu: 1,
            revision: 2,
        },
        now,
    );
    assert_eq!(controller.snapshot().pending_target, Some(1));
    assert_eq!(controller.snapshot().allocated_ccu, 2);

    controller.release_at(second, ReleaseReason::Intentional, now);
    assert_eq!(controller.snapshot().pending_target, None);
    assert_eq!(controller.snapshot().allocated_ccu, 1);
    assert!(matches!(
        controller.request_join_at(join(3, "c"), now),
        JoinDecision::Queued(_)
    ));
}

#[test]
fn concurrent_joins_never_oversubscribe() {
    use std::sync::Mutex;
    use std::thread;

    let now = Instant::now();
    let controller = Arc::new(Mutex::new(controller(5)));
    let mut handles = Vec::new();
    for index in 1..=50 {
        let controller = controller.clone();
        handles.push(thread::spawn(move || {
            let mut guard = controller.lock().unwrap();
            guard.request_join_at(join(index, "key"), now)
        }));
    }
    let mut admitted = 0;
    let mut queued = 0;
    let mut rejected = 0;
    for handle in handles {
        match handle.join().unwrap() {
            JoinDecision::Admitted(_) => admitted += 1,
            JoinDecision::Queued(_) => queued += 1,
            JoinDecision::Rejected(_) | JoinDecision::Paused => rejected += 1,
        }
    }
    let snapshot = controller.lock().unwrap().snapshot();
    assert_eq!(admitted, 5);
    assert_eq!(snapshot.active_ccu, 5);
    assert_eq!(admitted + queued + rejected, 50);
}

#[test]
fn release_offers_oldest_waiter() {
    let now = Instant::now();
    let mut controller = controller(1);
    let lease = match controller.request_join_at(join(1, "a"), now) {
        JoinDecision::Admitted(lease) => lease,
        _ => panic!(),
    };
    let first = match controller.request_join_at(join(2, "b"), now) {
        JoinDecision::Queued(ticket) => ticket,
        _ => panic!(),
    };
    let second = match controller.request_join_at(join(3, "c"), now) {
        JoinDecision::Queued(ticket) => ticket,
        _ => panic!(),
    };
    controller.release_at(lease, ReleaseReason::Intentional, now);
    assert_eq!(
        controller.queue_status_at(first.id, now),
        QueueStatus::Offered
    );
    assert_eq!(
        controller.queue_status_at(second.id, now),
        QueueStatus::Waiting { position: 1 }
    );
}

#[test]
fn reconnect_grace_blocks_promotion_until_expiry() {
    let now = Instant::now();
    let mut controller = controller(1);
    let lease = match controller.request_join_at(join(1, "a"), now) {
        JoinDecision::Admitted(lease) => lease,
        _ => panic!(),
    };
    let ticket = match controller.request_join_at(join(2, "b"), now) {
        JoinDecision::Queued(ticket) => ticket,
        _ => panic!(),
    };
    controller.release_at(lease, ReleaseReason::Unexpected, now);
    assert_eq!(
        controller.queue_status_at(ticket.id, now),
        QueueStatus::Waiting { position: 1 }
    );
    assert_eq!(
        controller.queue_status_at(ticket.id, now + policy().reconnect_grace),
        QueueStatus::Offered
    );
}

#[test]
fn allocation_increase_promotes_waiters_and_stale_update_is_ignored() {
    let now = Instant::now();
    let mut controller = controller(1);
    let _ = controller.request_join_at(join(1, "a"), now);
    let first = match controller.request_join_at(join(2, "b"), now) {
        JoinDecision::Queued(ticket) => ticket,
        _ => panic!(),
    };
    let second = match controller.request_join_at(join(3, "c"), now) {
        JoinDecision::Queued(ticket) => ticket,
        _ => panic!(),
    };
    controller.apply_capacity_at(
        CapacityUpdate {
            allocated_ccu: 3,
            revision: 2,
        },
        now,
    );
    assert_eq!(
        controller.queue_status_at(first.id, now),
        QueueStatus::Offered
    );
    assert_eq!(
        controller.queue_status_at(second.id, now),
        QueueStatus::Offered
    );
    assert_eq!(
        controller
            .apply_capacity_at(
                CapacityUpdate {
                    allocated_ccu: 1,
                    revision: 1,
                },
                now,
            )
            .allocated_ccu,
        3
    );
}

#[test]
fn decrease_drains_without_disconnect_and_paused_rejects() {
    let now = Instant::now();
    let mut controller = controller(2);
    let first = match controller.request_join_at(join(1, "a"), now) {
        JoinDecision::Admitted(lease) => lease,
        _ => panic!(),
    };
    let _ = controller.request_join_at(join(2, "b"), now);
    let snapshot = controller.apply_capacity_at(
        CapacityUpdate {
            allocated_ccu: 1,
            revision: 2,
        },
        now,
    );
    assert_eq!(snapshot.pending_target, Some(1));
    assert_eq!(snapshot.active_ccu, 2);
    controller.release_at(first, ReleaseReason::Intentional, now);
    assert_eq!(controller.snapshot().allocated_ccu, 1);
    let paused = controller.apply_capacity_at(
        CapacityUpdate {
            allocated_ccu: 0,
            revision: 3,
        },
        now,
    );
    assert_eq!(paused.pending_target, Some(0));
}

#[test]
fn paused_and_full_queue_are_structured_rejections() {
    let now = Instant::now();
    let mut paused = controller(0);
    assert_eq!(
        paused.request_join_at(join(1, "a"), now),
        JoinDecision::Paused
    );
    let mut full = AdmissionController::new(
        metadata(),
        QueuePolicy {
            max_depth: 0,
            ..policy()
        },
        CapacityUpdate {
            allocated_ccu: 1,
            revision: 1,
        },
        Arc::new(UsageCounters::new(metadata())),
    );
    let _ = full.request_join_at(join(1, "a"), now);
    assert_eq!(
        full.request_join_at(join(2, "b"), now),
        JoinDecision::Rejected(RejectionReason::QueueFull)
    );
}

#[test]
fn idempotency_keys_are_principal_scoped() {
    let now = Instant::now();
    let mut controller = controller(1);
    let _ = controller.request_join_at(join(1, "a"), now);
    let first = match controller.request_join_at(join(2, "b"), now) {
        JoinDecision::Queued(ticket) => ticket,
        _ => panic!(),
    };
    let second = match controller.request_join_at(join(3, "b"), now) {
        JoinDecision::Queued(ticket) => ticket,
        _ => panic!(),
    };
    assert_ne!(first.id, second.id);
    assert_ne!(first.principal, second.principal);
    assert_eq!(
        controller.request_join_at(join(2, "b"), now),
        JoinDecision::Queued(first)
    );
}

#[test]
fn same_principal_cannot_hold_two_queue_positions() {
    let now = Instant::now();
    let mut controller = controller(1);
    let _ = controller.request_join_at(join(1, "a"), now);
    let _ = controller.request_join_at(join(2, "b"), now);
    assert!(matches!(
        controller.request_join_at(join(2, "c"), now),
        JoinDecision::Rejected(RejectionReason::AlreadyQueued)
    ));
}

#[test]
fn resume_reclaims_reservation() {
    let now = Instant::now();
    let mut controller = controller(1);
    let lease = match controller.request_join_at(join(1, "a"), now) {
        JoinDecision::Admitted(lease) => lease,
        _ => panic!(),
    };
    controller.release_at(lease, ReleaseReason::Unexpected, now);
    let resumed = controller
        .resume_at(lease.resume_token, PrincipalId::new(1), now)
        .expect("valid resume");
    assert_eq!(resumed.id, lease.id);
    assert!(
        controller
            .resume_at(lease.resume_token, PrincipalId::new(1), now)
            .is_none()
    );
}

#[test]
fn cancellation_updates_subsequent_positions() {
    let now = Instant::now();
    let mut controller = controller(1);
    let _ = controller.request_join_at(join(1, "a"), now);
    let first = match controller.request_join_at(join(2, "b"), now) {
        JoinDecision::Queued(ticket) => ticket,
        _ => panic!(),
    };
    let second = match controller.request_join_at(join(3, "c"), now) {
        JoinDecision::Queued(ticket) => ticket,
        _ => panic!(),
    };
    assert_eq!(
        controller.queue_status_at(second.id, now),
        QueueStatus::Waiting { position: 2 }
    );
    controller.cancel_at(first.id, now);
    assert_eq!(
        controller.queue_status_at(second.id, now),
        QueueStatus::Waiting { position: 1 }
    );
}

#[test]
fn expired_and_abandoned_offers_release_permits() {
    let now = Instant::now();
    let mut controller = controller(1);
    let lease = match controller.request_join_at(join(1, "a"), now) {
        JoinDecision::Admitted(lease) => lease,
        _ => panic!(),
    };
    let ticket = match controller.request_join_at(join(2, "b"), now) {
        JoinDecision::Queued(ticket) => ticket,
        _ => panic!(),
    };
    controller.release_at(lease, ReleaseReason::Intentional, now);
    assert_eq!(
        controller.queue_status_at(ticket.id, now),
        QueueStatus::Offered
    );
    let after_offer = now + policy().offer_ttl + Duration::from_secs(1);
    assert_eq!(
        controller.queue_status_at(ticket.id, after_offer),
        QueueStatus::Expired
    );
}

#[test]
fn time_regression_does_not_panic() {
    let now = Instant::now();
    let mut controller = controller(1);
    let lease = match controller.request_join_at(join(1, "a"), now) {
        JoinDecision::Admitted(lease) => lease,
        _ => panic!(),
    };
    controller.release_at(lease, ReleaseReason::Unexpected, now);
    // Calling expire_at with an earlier instant must not panic.
    let earlier = now.checked_sub(Duration::from_secs(1)).unwrap();
    assert_eq!(
        controller.queue_status_at(QueueTicketId::new(999), earlier),
        QueueStatus::Missing
    );
}

#[test]
fn idempotency_index_is_cleaned_up_after_expiry() {
    let now = Instant::now();
    let mut controller = controller(1);
    let _ = controller.request_join_at(join(1, "a"), now);
    let ticket = match controller.request_join_at(join(2, "b"), now) {
        JoinDecision::Queued(ticket) => ticket,
        _ => panic!(),
    };
    controller.cancel_at(ticket.id, now);
    // The same idempotency key can now be reused for a new principal.
    let replacement = match controller.request_join_at(join(3, "b"), now) {
        JoinDecision::Queued(ticket) => ticket,
        _ => panic!(),
    };
    assert_ne!(ticket.id, replacement.id);
    assert_eq!(replacement.principal, PrincipalId::new(3));
}

#[test]
fn offer_claim_after_capacity_decrease_uses_current_allocation() {
    let now = Instant::now();
    let mut controller = controller(2);
    let first = match controller.request_join_at(join(1, "a"), now) {
        JoinDecision::Admitted(lease) => lease,
        _ => panic!(),
    };
    let second = match controller.request_join_at(join(2, "b"), now) {
        JoinDecision::Admitted(lease) => lease,
        _ => panic!(),
    };
    let ticket = match controller.request_join_at(join(3, "c"), now) {
        JoinDecision::Queued(ticket) => ticket,
        _ => panic!(),
    };
    controller.release_at(first, ReleaseReason::Intentional, now);
    // Offer goes out while allocation is still 2.
    assert_eq!(
        controller.queue_status_at(ticket.id, now),
        QueueStatus::Offered
    );
    // Decrease allocation to 1; active count is still 2, so pending target is set.
    controller.apply_capacity_at(
        CapacityUpdate {
            allocated_ccu: 1,
            revision: 2,
        },
        now,
    );
    // The outstanding offer can still be claimed; it reserves a slot that was already free.
    assert!(controller.claim_offer_at(ticket.id, now).is_ok());
    // Releasing the second active client should not promote anyone because target is 1.
    controller.release_at(second, ReleaseReason::Intentional, now);
    assert_eq!(controller.snapshot().active_ccu, 1);
    assert_eq!(controller.snapshot().pending_target, None);
}
