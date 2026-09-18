use woven_protocol::*;

fn scoped(control: ControlPayload) -> Envelope {
    let mut e = Envelope::control(DeliveryClass::ReliableOrdered, control);
    e.namespace_id = 1;
    e.session_id = 2;
    e.correlation_id = Some(3);
    e
}

fn update(state: QueueState) -> QueueUpdate {
    QueueUpdate {
        ticket_id: u64::MAX,
        state,
        position: u32::from(state == QueueState::Waiting),
        poll_after_ms: 0,
        ticket_remaining_ms: 0,
        offer_remaining_ms: 0,
    }
}

#[test]
fn all_managed_controls_and_results_roundtrip() {
    let mut controls = vec![
        ControlPayload::RequestAdmission(RequestAdmission {
            idempotency_key: "x".repeat(256),
        }),
        ControlPayload::QueueStatusRequest(QueueStatusRequest {
            ticket_id: u64::MAX,
        }),
        ControlPayload::QueueHeartbeat(QueueHeartbeat {
            ticket_id: u64::MAX,
        }),
        ControlPayload::QueueClaim(QueueClaim {
            ticket_id: u64::MAX,
        }),
        ControlPayload::QueueCancel(QueueCancel {
            ticket_id: u64::MAX,
        }),
    ];
    for status in [
        AdmissionStatus::Admitted,
        AdmissionStatus::Queued,
        AdmissionStatus::Paused,
        AdmissionStatus::Rejected,
    ] {
        controls.push(ControlPayload::AdmissionResult(AdmissionResult {
            status,
            rejection_code: if status == AdmissionStatus::Rejected {
                AdmissionRejectionCode::QueueFull
            } else {
                AdmissionRejectionCode::None
            },
            ticket_id: (status == AdmissionStatus::Queued).then_some(u64::MAX),
            poll_after_ms: 0,
            ticket_remaining_ms: 0,
        }));
    }
    for state in [
        QueueState::Waiting,
        QueueState::Offered,
        QueueState::Admitted,
        QueueState::Cancelled,
        QueueState::Expired,
        QueueState::Missing,
    ] {
        controls.push(ControlPayload::QueueUpdate(update(state)));
    }
    let codec = Codec::default();
    for control in controls {
        let e = scoped(control);
        assert_eq!(codec.decode(&codec.encode(&e).unwrap()).unwrap(), e);
        for invalid in 0..7 {
            let mut e = e.clone();
            match invalid {
                0 => e.namespace_id = 0,
                1 => e.session_id = 0,
                2 => e.space_id = 1,
                3 => e.channel_id = Some(1),
                4 => e.entity_id = Some(1),
                5 => e.correlation_id = None,
                _ => e.delivery_class = DeliveryClass::ReliableUnordered,
            }
            assert!(codec.encode(&e).is_err());
        }
    }
}

#[test]
fn rejects_invalid_fields_and_result_combinations() {
    let codec = Codec::default();
    for key in [String::new(), "x".repeat(257)] {
        assert!(
            codec
                .encode(&scoped(ControlPayload::RequestAdmission(
                    RequestAdmission {
                        idempotency_key: key
                    }
                )))
                .is_err()
        );
    }
    assert!(
        codec
            .encode(&scoped(ControlPayload::QueueClaim(QueueClaim {
                ticket_id: 0
            })))
            .is_err()
    );
    for invalid in 0..6 {
        let mut value = update(QueueState::Waiting);
        match invalid {
            0 => value.ticket_id = 0,
            1 => value.state = QueueState::Unknown,
            2 => value.position = 0,
            3 => value.poll_after_ms = 30_001,
            4 => value.ticket_remaining_ms = 900_001,
            _ => value.offer_remaining_ms = 1,
        }
        assert!(
            codec
                .encode(&scoped(ControlPayload::QueueUpdate(value)))
                .is_err()
        );
    }
    let value = AdmissionResult {
        status: AdmissionStatus::Admitted,
        rejection_code: AdmissionRejectionCode::None,
        ticket_id: Some(1),
        poll_after_ms: 0,
        ticket_remaining_ms: 0,
    };
    assert!(
        codec
            .encode(&scoped(ControlPayload::AdmissionResult(value)))
            .is_err()
    );
}

#[test]
fn managed_golden_is_stable_and_sanitized() {
    let bytes = include_bytes!("fixtures/queue_update_v1.swp");
    let mut value = update(QueueState::Offered);
    value.poll_after_ms = 1_000;
    value.ticket_remaining_ms = 120_000;
    value.offer_remaining_ms = 30_000;
    let e = scoped(ControlPayload::QueueUpdate(value));
    let codec = Codec::default();
    assert_eq!(codec.encode(&e).unwrap(), bytes);
    assert_eq!(codec.decode(bytes).unwrap(), e);
    assert_eq!(MessageKind::RequestAdmission.value(), 33);
    assert_eq!(MessageKind::QueueUpdate.value(), 39);
}
