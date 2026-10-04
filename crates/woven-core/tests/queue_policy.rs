use woven_core::{
    ChannelId, CoalesceKey, DeliveryClass, EntityId, NamespaceId, OutboundMessage, OutboundQueue,
    OutboundQueueConfig, PersistenceClass, QueueError, QueueEviction, QueuePush, SessionId,
    SpaceEpoch, SpaceId,
};

fn message(
    namespace: u64,
    delivery: DeliveryClass,
    sequence: u64,
    key: Option<CoalesceKey>,
) -> OutboundMessage {
    OutboundMessage {
        namespace: NamespaceId::new(namespace),
        session: SessionId::new(1),
        space: SpaceId::new(1),
        space_epoch: SpaceEpoch::new(1),
        entity: Some(EntityId::new(1)),
        channel: ChannelId::new(1),
        sequence,
        delivery,
        persistence: PersistenceClass::Ephemeral,
        coalesce_key: key,
        routing_position: None,
        payload: sequence.to_be_bytes().to_vec(),
    }
}

fn key(component: u64) -> CoalesceKey {
    CoalesceKey::new(ChannelId::new(1), Some(EntityId::new(1)), component)
}

#[test]
fn queue_reserves_critical_capacity_drops_best_effort_and_coalesces_latest() {
    let config = OutboundQueueConfig {
        total_capacity: 3,
        critical_capacity: 1,
        latest_capacity: 1,
        best_effort_capacity: 1,
    };
    let mut queue = OutboundQueue::new(config).expect("valid queue config");
    let state_key = key(9);

    assert_eq!(
        queue.push(message(1, DeliveryClass::BestEffortEvent, 1, None)),
        Ok(QueuePush::Queued)
    );
    assert_eq!(
        queue.push(message(1, DeliveryClass::BestEffortEvent, 2, None)),
        Ok(QueuePush::DroppedBestEffort)
    );
    assert_eq!(
        queue.push(message(1, DeliveryClass::LatestValue, 3, Some(state_key))),
        Ok(QueuePush::Queued)
    );
    assert_eq!(
        queue.push(message(1, DeliveryClass::LatestValue, 4, Some(state_key))),
        Ok(QueuePush::ReplacedLatest)
    );
    assert_eq!(
        queue.push(message(1, DeliveryClass::ReliableOrdered, 5, None)),
        Ok(QueuePush::Queued)
    );

    assert_eq!(queue.len(), 3);
    assert_eq!(queue.capacity(), 3);
    assert_eq!(queue.pop().expect("critical first").sequence, 5);
    assert_eq!(queue.pop().expect("latest second").sequence, 4);
    assert_eq!(queue.pop().expect("best effort last").sequence, 1);
}

#[test]
fn critical_traffic_evicts_lower_priority_data_then_reports_exhaustion() {
    let config = OutboundQueueConfig {
        total_capacity: 2,
        critical_capacity: 2,
        latest_capacity: 2,
        best_effort_capacity: 2,
    };
    let mut queue = OutboundQueue::new(config).expect("valid queue config");
    let state_key = key(1);
    let scoped_key = message(1, DeliveryClass::LatestValue, 1, Some(state_key))
        .scoped_coalesce_key()
        .expect("scoped key");

    queue
        .push(message(1, DeliveryClass::LatestValue, 1, Some(state_key)))
        .expect("latest enqueue");
    queue
        .push(message(1, DeliveryClass::BestEffortEvent, 2, None))
        .expect("best-effort enqueue");
    assert_eq!(
        queue.push(message(1, DeliveryClass::ReliableOrdered, 3, None)),
        Ok(QueuePush::QueuedCriticalAfterEviction(
            QueueEviction::Latest { key: scoped_key }
        ))
    );
    assert_eq!(
        queue.push(message(1, DeliveryClass::ReliableOrdered, 4, None)),
        Ok(QueuePush::QueuedCriticalAfterEviction(
            QueueEviction::BestEffort
        ))
    );
    assert_eq!(
        queue.push(message(1, DeliveryClass::ReliableOrdered, 5, None)),
        Ok(QueuePush::CriticalCapacityExhausted)
    );
    assert!(queue.is_slow_consumer());
}

#[test]
fn latest_keys_are_fully_scoped_and_refresh_eviction_recency() {
    let config = OutboundQueueConfig {
        total_capacity: 3,
        critical_capacity: 1,
        latest_capacity: 3,
        best_effort_capacity: 0,
    };
    let mut queue = OutboundQueue::new(config).expect("valid queue config");
    let application_key = key(1);

    for (namespace, sequence) in [(1, 1), (2, 2)] {
        queue
            .push(message(
                namespace,
                DeliveryClass::LatestValue,
                sequence,
                Some(application_key),
            ))
            .expect("scoped latest enqueue");
    }
    assert_eq!(queue.len(), 2, "different namespaces must not coalesce");
    assert_eq!(
        queue.push(message(
            1,
            DeliveryClass::LatestValue,
            3,
            Some(application_key),
        )),
        Ok(QueuePush::ReplacedLatest)
    );
    queue
        .push(message(
            3,
            DeliveryClass::LatestValue,
            4,
            Some(application_key),
        ))
        .expect("third latest enqueue");
    let namespace_two_key = message(2, DeliveryClass::LatestValue, 2, Some(application_key))
        .scoped_coalesce_key()
        .expect("scoped key");
    assert_eq!(
        queue.push(message(
            4,
            DeliveryClass::LatestValue,
            5,
            Some(application_key),
        )),
        Ok(QueuePush::EvictedLatest {
            key: namespace_two_key
        })
    );
    assert_eq!(queue.pop().expect("refreshed value remains").sequence, 3);
}

#[test]
fn purge_removes_matching_messages_across_all_delivery_classes() {
    let config = OutboundQueueConfig {
        total_capacity: 4,
        critical_capacity: 2,
        latest_capacity: 1,
        best_effort_capacity: 1,
    };
    let mut queue = OutboundQueue::new(config).expect("valid queue config");
    queue
        .push(message(1, DeliveryClass::ReliableOrdered, 1, None))
        .expect("critical");
    queue
        .push(message(1, DeliveryClass::LatestValue, 2, Some(key(1))))
        .expect("latest");
    queue
        .push(message(2, DeliveryClass::BestEffortEvent, 3, None))
        .expect("best effort");

    assert_eq!(
        queue.purge(|queued| queued.namespace == NamespaceId::new(1)),
        2
    );
    assert_eq!(
        queue.drain(),
        vec![message(2, DeliveryClass::BestEffortEvent, 3, None)]
    );
}

#[test]
fn replaceable_messages_require_an_explicit_coalesce_key() {
    let mut queue = OutboundQueue::new(OutboundQueueConfig::default()).expect("valid queue");
    assert_eq!(
        queue.push(message(1, DeliveryClass::LatestValue, 1, None)),
        Err(QueueError::MissingCoalesceKey)
    );
    assert!(queue.is_empty());
}
