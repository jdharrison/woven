use super::*;
use std::{future::Future, pin::Pin};
use woven_core::{
    ChannelDefinition, ChannelId, CoordinateFrame, CoreConfig, Credentials, DevAuthenticator,
    EntityId, IdempotencyKey, JoinDecision, MANAGED_MIGRATION_BASELINE_VERSION, ManagedCheckpoint,
    ManagedCheckpointMutation, ManagedError, ManagedMigrationBaseline, ManagedOutcome,
    ManagedRequest, NamespaceId, RoutingPolicy, SessionId, SessionKey, SpaceDescriptor, SpaceId,
    SpatialBounds3D, WovenCore,
};

const SESSION: SessionKey = SessionKey::new(NamespaceId::new(1), SessionId::new(2));
const ADMIN: &str = "independent-managed-persistence-test-admin";

type Save = (
    ManagedCheckpoint,
    oneshot::Sender<Result<(), ManagedPersistenceError>>,
);

struct GatedStore {
    saves: mpsc::Sender<Save>,
}

impl ManagedPersistence for GatedStore {
    fn save(
        &self,
        checkpoint: ManagedCheckpoint,
    ) -> Pin<Box<dyn Future<Output = Result<(), ManagedPersistenceError>> + Send + '_>> {
        Box::pin(async move {
            let (reply, receive) = oneshot::channel();
            self.saves
                .send((checkpoint, reply))
                .await
                .map_err(|_| ManagedPersistenceError)?;
            receive.await.map_err(|_| ManagedPersistenceError)?
        })
    }
}

fn core() -> WovenCore<DevAuthenticator> {
    let mut core = WovenCore::new(DevAuthenticator::new(), CoreConfig::default()).unwrap();
    core.enable_managed(&Credentials::new(ADMIN)).unwrap();
    for (id, delivery) in [
        (1, CoreDelivery::ReliableOrdered),
        (4, CoreDelivery::UnreliableSequenced),
    ] {
        core.register_channel(ChannelDefinition::relay_owned(
            ChannelId::new(id),
            delivery,
            PersistenceClass::Ephemeral,
            65_536,
        ))
        .unwrap();
    }
    core
}

fn put() -> ManagedRequest {
    ManagedRequest::Put {
        session: SESSION,
        revision: 1,
        allocated_ccu: 1,
        tick_rate_hz: Some(60),
        credentials: Credentials::new("a".repeat(64)),
    }
}

fn patch(revision: u64) -> ManagedRequest {
    ManagedRequest::Patch {
        session: SESSION,
        revision,
        allocated_ccu: 2,
        tick_rate_hz: None,
    }
}

fn put_space() -> ManagedRequest {
    ManagedRequest::PutSpace {
        session: SESSION,
        revision: 3,
        descriptor: SpaceDescriptor {
            id: SpaceId::new(3),
            epoch: SpaceEpoch::new(1),
            local_frame: CoordinateFrame::Cartesian3D {
                meters_per_unit: 0.5,
            },
            bounds: Some(SpatialBounds3D {
                min_x: -100.0,
                min_y: -100.0,
                min_z: -100.0,
                max_x: 100.0,
                max_y: 100.0,
                max_z: 100.0,
            }),
            parent: None,
            routing: RoutingPolicy::SpatialGrid3D {
                cell_size: 10.0,
                interest_radius: 20.0,
                exact_distance: false,
            },
        },
    }
}

fn spawn(core: WovenCore<DevAuthenticator>) -> (WorkerHandle, mpsc::Receiver<Save>) {
    let (saves, receive) = mpsc::channel(1);
    let worker = spawn_worker_with_managed_persistence(
        TransportIndependentWorker::new(core),
        Arc::new(GatedStore { saves }),
    );
    (worker, receive)
}

fn manage(
    worker: &WorkerHandle,
    request: ManagedRequest,
) -> tokio::task::JoinHandle<Result<ManagedOutcome, ManagedError>> {
    let worker = worker.clone();
    tokio::spawn(async move { worker.manage(request).await })
}

async fn save(saves: &mut mpsc::Receiver<Save>) -> Save {
    tokio::time::timeout(Duration::from_secs(2), saves.recv())
        .await
        .unwrap()
        .unwrap()
}

async fn peer(worker: &WorkerHandle) -> (ConnectionId, mpsc::Receiver<()>) {
    let CommandResult::Connected(connection) =
        worker.execute(Command::TransportConnected).await.unwrap()
    else {
        panic!("connected expected")
    };
    worker
        .execute(Command::Authenticate {
            connection,
            credentials: Credentials::new("a".repeat(64)),
        })
        .await
        .unwrap();
    let (write, _read) = mpsc::channel(4);
    let (shutdown, receive) = mpsc::channel(1);
    worker
        .register_lifecycle(connection, write, shutdown)
        .await
        .unwrap();
    (connection, receive)
}

fn event(connection: ConnectionId, entity: EntityId, sequence: u64) -> PublishRequest {
    PublishRequest {
        connection,
        session: SESSION,
        space: SpaceId::new(1),
        space_epoch: SpaceEpoch::new(1),
        entity: Some(entity),
        channel: ChannelId::new(1),
        sequence,
        delivery: CoreDelivery::ReliableOrdered,
        persistence: PersistenceClass::Ephemeral,
        coalesce_key: None,
        routing_position: None,
        payload: sequence.to_be_bytes().to_vec(),
    }
}

fn populated_core() -> (WovenCore<DevAuthenticator>, ConnectionId, EntityId) {
    let mut core = core();
    core.manage_at(put(), Instant::now()).unwrap();
    let connection = core.transport_connected().unwrap();
    core.authenticate(connection, &Credentials::new("a".repeat(64)))
        .unwrap();
    assert!(matches!(
        core.admit_and_join_session_at(
            connection,
            SESSION,
            IdempotencyKey::new("populated").unwrap(),
            Instant::now(),
        )
        .unwrap(),
        JoinDecision::Admitted(_)
    ));
    let space = SpaceKey::new(SESSION, SpaceId::new(1));
    core.subscribe(connection, space).unwrap();
    let entity = core
        .spawn_entity(connection, space, SpaceEpoch::new(1))
        .unwrap();
    assert_eq!(
        core.publish(event(connection, entity, 1))
            .unwrap()
            .recipient_attempts,
        1
    );
    (core, connection, entity)
}

async fn register_populated_peer(
    worker: &WorkerHandle,
    connection: ConnectionId,
) -> (mpsc::Receiver<Envelope>, mpsc::Receiver<()>) {
    let (write, read) = mpsc::channel(4);
    let (shutdown, receive) = mpsc::channel(1);
    worker
        .register_lifecycle(connection, write, shutdown)
        .await
        .unwrap();
    worker
        .activate_subscription(connection, SpaceKey::new(SESSION, SpaceId::new(1)))
        .await
        .unwrap();
    (read, receive)
}

// Enqueue directly so FIFO ordering and submission before the save gate opens are guaranteed,
// rather than depending on when a spawned API caller first gets polled by the runtime.
async fn enqueue_command(
    worker: &WorkerHandle,
    command: Command,
) -> oneshot::Receiver<Result<CommandResult, CoreError>> {
    let (reply, receive) = oneshot::channel();
    worker
        .sender
        .send(WorkerRequest::Command { command, reply })
        .await
        .unwrap();
    receive
}

async fn enqueue_managed(
    worker: &WorkerHandle,
    request: ManagedRequest,
) -> oneshot::Receiver<Result<ManagedOutcome, ManagedError>> {
    let (reply, receive) = oneshot::channel();
    worker
        .sender
        .send(WorkerRequest::Managed { request, reply })
        .await
        .unwrap();
    receive
}

fn assert_pending<T>(reply: &mut oneshot::Receiver<T>) {
    assert!(matches!(
        reply.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
}

#[tokio::test]
async fn gated_save_blocks_real_publish_snapshot_drain_and_the_next_mutation() {
    let (initial, connection, entity) = populated_core();
    let (worker, mut saves) = spawn(initial);
    let (_writes, mut shutdown) = register_populated_peer(&worker, connection).await;
    let mut mutation = manage(&worker, patch(2));
    let (checkpoint, finish) = save(&mut saves).await;
    assert_eq!(checkpoint.scopes[0].revision, 2);
    let mut publish =
        enqueue_command(&worker, Command::Publish(event(connection, entity, 2))).await;
    let mut snapshot = enqueue_command(
        &worker,
        Command::Snapshot {
            connection,
            session: SESSION,
        },
    )
    .await;
    let mut drain = enqueue_command(&worker, Command::DrainOutbound { connection }).await;
    let mut next = enqueue_managed(&worker, put_space()).await;
    let mut tail = enqueue_command(
        &worker,
        Command::Snapshot {
            connection,
            session: SESSION,
        },
    )
    .await;
    assert_eq!(worker.sender.capacity(), COMMAND_CAPACITY - 5);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut mutation)
            .await
            .is_err()
    );
    assert_pending(&mut publish);
    assert_pending(&mut snapshot);
    assert_pending(&mut drain);
    assert_pending(&mut next);
    assert_pending(&mut tail);
    assert_eq!(worker.metrics().snapshot().publishes_total, 0);
    assert!(matches!(
        shutdown.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        saves.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));

    finish.send(Ok(())).unwrap();
    mutation.await.unwrap().unwrap();
    let (checkpoint, finish_next) = save(&mut saves).await;
    assert_eq!(checkpoint.scopes[0].revision, 3);
    assert_eq!(checkpoint.scopes[0].spatial_spaces[0].space_id, 3);
    assert!(matches!(
        publish.await.unwrap().unwrap(),
        CommandResult::Published(_)
    ));
    let CommandResult::Snapshot(snapshot) = snapshot.await.unwrap().unwrap() else {
        panic!("snapshot expected")
    };
    assert_eq!(snapshot.member_count, 1);
    assert_eq!(snapshot.entities.len(), 1);
    assert_eq!(snapshot.entities[0].id, entity);
    let CommandResult::Outbound(messages) = drain.await.unwrap().unwrap() else {
        panic!("outbound expected")
    };
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0].payload, 1_u64.to_be_bytes());
    assert_eq!(messages[1].payload, 2_u64.to_be_bytes());
    assert_eq!(worker.metrics().snapshot().publishes_total, 1);
    assert_pending(&mut next);
    assert_pending(&mut tail);
    finish_next.send(Ok(())).unwrap();
    next.await.unwrap().unwrap();
    assert!(matches!(
        tail.await.unwrap().unwrap(),
        CommandResult::Snapshot(_)
    ));
    assert!(!worker.is_closed());
}

#[tokio::test]
async fn failed_save_never_executes_queued_publish_snapshot_drain_or_next_mutation() {
    let (initial, connection, entity) = populated_core();
    let (worker, mut saves) = spawn(initial);
    let (mut writes, mut shutdown) = register_populated_peer(&worker, connection).await;
    let mutation = manage(&worker, patch(2));
    let (_, finish) = save(&mut saves).await;
    let mut publish =
        enqueue_command(&worker, Command::Publish(event(connection, entity, 2))).await;
    let mut snapshot = enqueue_command(
        &worker,
        Command::Snapshot {
            connection,
            session: SESSION,
        },
    )
    .await;
    let mut drain = enqueue_command(&worker, Command::DrainOutbound { connection }).await;
    let mut next = enqueue_managed(&worker, put_space()).await;
    assert_eq!(worker.sender.capacity(), COMMAND_CAPACITY - 4);
    assert_pending(&mut publish);
    assert_pending(&mut snapshot);
    assert_pending(&mut drain);
    assert_pending(&mut next);
    assert_eq!(worker.metrics().snapshot().publishes_total, 0);
    finish.send(Err(ManagedPersistenceError)).unwrap();
    assert_eq!(
        mutation.await.unwrap(),
        Err(ManagedError::WorkerUnavailable)
    );
    worker.closed().await;
    assert!(publish.await.is_err());
    assert!(snapshot.await.is_err());
    assert!(drain.await.is_err());
    assert!(next.await.is_err());
    // This counter increments whenever a publish command executes, even if it is rejected.
    assert_eq!(worker.metrics().snapshot().publishes_total, 0);
    assert_eq!(worker.metrics().snapshot().events_delivered_total, 0);
    assert_eq!(shutdown.recv().await, Some(()));
    assert!(writes.recv().await.is_none());
    assert!(saves.recv().await.is_none());
}

#[tokio::test]
async fn saving_blocks_ack_reads_commands_and_later_mutations_until_durability() {
    let (worker, mut saves) = spawn(core());
    let mut first = manage(&worker, put());
    let (checkpoint, finish) = save(&mut saves).await;
    assert_eq!(checkpoint.scopes.len(), 1);
    assert_eq!(checkpoint.scopes[0].revision, 1);
    let mut query = manage(&worker, ManagedRequest::Get { session: SESSION });
    let command_worker = worker.clone();
    let mut command =
        tokio::spawn(async move { command_worker.execute(Command::TransportConnected).await });
    let second = manage(&worker, patch(2));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut first)
            .await
            .is_err()
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut query)
            .await
            .is_err()
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut command)
            .await
            .is_err()
    );
    assert!(!second.is_finished());
    assert!(saves.try_recv().is_err()); // No overlapping save despite a queued mutation.
    finish.send(Ok(())).unwrap();
    first.await.unwrap().unwrap();
    let (checkpoint, finish) = save(&mut saves).await;
    assert_eq!(checkpoint.scopes[0].revision, 2);
    assert_eq!(checkpoint.scopes[0].tick_rate_hz, Some(60));
    assert!(!second.is_finished());
    finish.send(Ok(())).unwrap();
    second.await.unwrap().unwrap();
    query.await.unwrap().unwrap();
    command.await.unwrap().unwrap();
    assert!(!worker.is_closed());
}

#[tokio::test]
async fn save_failure_never_returns_success_closes_readiness_and_discards_queued_work() {
    let mut initial = core();
    initial.manage_at(put(), Instant::now()).unwrap();
    let durable_before = initial.export_managed_checkpoint().unwrap();
    let (worker, mut saves) = spawn(initial);
    let (_, mut shutdown) = peer(&worker).await;
    let mutation = manage(&worker, patch(2));
    let (undurable, finish) = save(&mut saves).await;
    assert_eq!(undurable.scopes[0].revision, 2);
    let queued = manage(&worker, patch(3));
    let query = manage(&worker, ManagedRequest::Get { session: SESSION });
    let command_worker = worker.clone();
    let command =
        tokio::spawn(async move { command_worker.execute(Command::TransportConnected).await });
    assert!(shutdown.try_recv().is_err());
    finish.send(Err(ManagedPersistenceError)).unwrap();
    assert_eq!(
        mutation.await.unwrap(),
        Err(ManagedError::WorkerUnavailable)
    );
    tokio::time::timeout(Duration::from_secs(2), worker.closed())
        .await
        .unwrap();
    assert!(worker.is_closed());
    assert_eq!(queued.await.unwrap(), Err(ManagedError::WorkerUnavailable));
    assert_eq!(query.await.unwrap(), Err(ManagedError::WorkerUnavailable));
    assert!(matches!(
        command.await.unwrap(),
        Err(TransportError::WorkerUnavailable)
    ));
    assert_eq!(shutdown.recv().await, Some(()));
    assert_eq!(
        worker.manage(put()).await,
        Err(ManagedError::WorkerUnavailable)
    );
    assert!(matches!(
        worker.live_counts().await,
        Err(TransportError::WorkerUnavailable)
    ));
    assert!(saves.recv().await.is_none());
    let mut restarted = core();
    restarted
        .restore_managed_checkpoint(durable_before)
        .unwrap();
    let ManagedOutcome::Snapshot { snapshot, .. } = restarted
        .manage_at(ManagedRequest::Get { session: SESSION }, Instant::now())
        .unwrap()
    else {
        unreachable!()
    };
    assert_eq!(snapshot.revision, "1");
    assert_eq!(snapshot.allocated_ccu, 1);
}

#[tokio::test]
async fn delete_saves_tombstones_before_reply_or_transport_peer_teardown() {
    for successful in [true, false] {
        let (initial, connection, entity) = populated_core();
        let (worker, mut saves) = spawn(initial);
        let (mut writes, mut shutdown) = register_populated_peer(&worker, connection).await;
        let CommandResult::Snapshot(before) = worker
            .execute(Command::Snapshot {
                connection,
                session: SESSION,
            })
            .await
            .unwrap()
        else {
            panic!("populated snapshot expected")
        };
        assert_eq!(before.member_count, 1);
        assert_eq!(before.subscription_count, 1);
        assert_eq!(before.entities.len(), 1);
        assert_eq!(before.entities[0].id, entity);
        let mut deletion = manage(
            &worker,
            ManagedRequest::Delete {
                session: SESSION,
                revision: 1,
            },
        );
        let (checkpoint, finish) = save(&mut saves).await;
        assert!(checkpoint.scopes[0].retired);
        let mut drain = enqueue_command(&worker, Command::DrainOutbound { connection }).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut deletion)
                .await
                .is_err()
        );
        assert_pending(&mut drain);
        assert!(matches!(
            writes.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            shutdown.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        finish
            .send(if successful {
                Ok(())
            } else {
                Err(ManagedPersistenceError)
            })
            .unwrap();
        let result = deletion.await.unwrap();
        if successful {
            assert_eq!(
                result,
                Ok(ManagedOutcome::Deleted {
                    connections: vec![connection]
                })
            );
            assert!(!worker.is_closed());
            assert_eq!(
                drain.await.unwrap(),
                Err(CoreError::UnknownConnection(connection))
            );
            let counts = worker.live_counts().await.unwrap();
            assert_eq!(counts.sessions_active, 0);
            assert_eq!(counts.connections_active, 0);
            let mut restarted = core();
            restarted.restore_managed_checkpoint(checkpoint).unwrap();
            let connection = restarted.transport_connected().unwrap();
            assert!(
                restarted
                    .authenticate(connection, &Credentials::new("a".repeat(64)))
                    .is_err()
            );
            assert_eq!(
                restarted.manage_at(put(), Instant::now()),
                Err(ManagedError::ScopeRetired)
            );
        } else {
            assert_eq!(result, Err(ManagedError::WorkerUnavailable));
            worker.closed().await;
            assert!(drain.await.is_err());
        }
        assert_eq!(shutdown.recv().await, Some(()));
        assert!(writes.recv().await.is_none());
    }
}

#[tokio::test]
async fn every_mutation_and_idempotent_retry_saves_but_reads_and_rejections_do_not() {
    let (worker, mut saves) = spawn(core());
    let requests = [
        put(),
        put(),
        patch(2),
        patch(2),
        put_space(),
        put_space(),
        ManagedRequest::Delete {
            session: SESSION,
            revision: 3,
        },
        ManagedRequest::Delete {
            session: SESSION,
            revision: 3,
        },
    ];
    for request in requests {
        let mutation = manage(&worker, request);
        let (checkpoint, finish) = save(&mut saves).await;
        assert_eq!(checkpoint.scopes.len(), 1);
        finish.send(Ok(())).unwrap();
        mutation.await.unwrap().unwrap();
        let query = worker
            .manage(ManagedRequest::Get { session: SESSION })
            .await;
        if query.is_ok() {
            assert!(saves.try_recv().is_err());
            assert_eq!(
                worker.manage(patch(1)).await,
                Err(ManagedError::RevisionConflict)
            );
        } else {
            assert_eq!(query, Err(ManagedError::ScopeNotFound));
            assert_eq!(worker.manage(put()).await, Err(ManagedError::ScopeRetired));
        }
        assert!(saves.try_recv().is_err());
    }
}

#[tokio::test]
async fn cancelling_the_admin_caller_does_not_cancel_or_skip_its_save() {
    let (worker, mut saves) = spawn(core());
    let mutation = manage(&worker, put());
    let (checkpoint, finish) = save(&mut saves).await;
    mutation.abort();
    assert!(mutation.await.unwrap_err().is_cancelled());
    finish.send(Ok(())).unwrap();
    let ManagedOutcome::Snapshot { snapshot, .. } = worker
        .manage(ManagedRequest::Get { session: SESSION })
        .await
        .unwrap()
    else {
        unreachable!()
    };
    assert_eq!(snapshot.revision, "1");
    assert!(!worker.is_closed());
    let mut restarted = core();
    restarted.restore_managed_checkpoint(checkpoint).unwrap();
}

#[tokio::test]
async fn cancelling_a_caller_while_queued_still_persists_after_the_first_save() {
    use std::task::Poll;

    let (worker, mut saves) = spawn(core());
    let first = manage(&worker, put());
    let (_, finish_first) = save(&mut saves).await;
    let capacity = worker.sender.capacity();
    let mut queued_caller = Box::pin(worker.manage(patch(2)));
    std::future::poll_fn(|cx| {
        assert!(matches!(queued_caller.as_mut().poll(cx), Poll::Pending));
        Poll::Ready(())
    })
    .await;
    assert_eq!(worker.sender.capacity(), capacity - 1);
    drop(queued_caller); // Submission happened, but the worker has not read this request yet.
    assert!(matches!(
        saves.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    finish_first.send(Ok(())).unwrap();
    first.await.unwrap().unwrap();

    let (checkpoint, finish_second) = save(&mut saves).await;
    assert_eq!(checkpoint.scopes[0].revision, 2);
    assert_eq!(checkpoint.scopes[0].allocated_ccu, 2);
    assert_eq!(checkpoint.scopes[0].tick_rate_hz, Some(60));
    let mut observation = enqueue_managed(&worker, ManagedRequest::Get { session: SESSION }).await;
    assert_pending(&mut observation);
    finish_second.send(Ok(())).unwrap();
    let ManagedOutcome::Snapshot { snapshot, .. } = observation.await.unwrap().unwrap() else {
        panic!("snapshot expected")
    };
    assert_eq!(snapshot.revision, "2");
    assert_eq!(snapshot.allocated_ccu, 2);
    assert!(!worker.is_closed());
    let mut restarted = core();
    restarted
        .restore_managed_checkpoint(checkpoint.clone())
        .unwrap();
    restarted.manage_at(patch(2), Instant::now()).unwrap();
    assert_eq!(restarted.export_managed_checkpoint().unwrap(), checkpoint);
}

#[tokio::test]
async fn baseline_provenance_survives_gated_real_mutation_delete_and_automatic_reload() {
    let mut source = core();
    source.manage_at(put(), Instant::now()).unwrap();
    let mut baseline = source.export_managed_checkpoint().unwrap();
    baseline.migration_baseline = Some(ManagedMigrationBaseline {
        version: MANAGED_MIGRATION_BASELINE_VERSION,
        migration_id: "durable-worker-baseline-test".to_owned(),
        previous_lineage: "e".repeat(48),
        closed_scope_through: 2,
        unknown_retired_verifier_history: true,
    });
    baseline.scopes[0].last_mutation = ManagedCheckpointMutation::MigrationBaseline;
    let provenance = baseline.migration_baseline.clone();
    let mut restored = core();
    restored.restore_managed_checkpoint(baseline).unwrap();
    let (worker, mut saves) = spawn(restored);
    assert_eq!(
        worker.manage(put()).await,
        Err(ManagedError::RevisionConflict)
    );
    let mutation = manage(&worker, patch(2));
    let (checkpoint, finish) = save(&mut saves).await;
    assert_eq!(checkpoint.migration_baseline, provenance);
    assert_eq!(checkpoint.scopes.len(), 1);
    assert_eq!(checkpoint.scopes[0].namespace_id, SESSION.namespace.get());
    assert_eq!(checkpoint.scopes[0].session_id, SESSION.session.get());
    assert_eq!(
        checkpoint.scopes[0].last_mutation,
        ManagedCheckpointMutation::Configuration {
            allocated_ccu: 2,
            tick_rate_hz: None,
        }
    );
    let mut observation = enqueue_managed(&worker, ManagedRequest::Get { session: SESSION }).await;
    assert_pending(&mut observation);
    finish.send(Ok(())).unwrap();
    mutation.await.unwrap().unwrap();
    observation.await.unwrap().unwrap();

    let deletion = manage(
        &worker,
        ManagedRequest::Delete {
            session: SESSION,
            revision: 2,
        },
    );
    let (checkpoint, finish) = save(&mut saves).await;
    assert_eq!(checkpoint.migration_baseline, provenance);
    assert!(checkpoint.scopes[0].retired);
    assert!(!deletion.is_finished());
    finish.send(Ok(())).unwrap();
    deletion.await.unwrap().unwrap();
    let mut restarted = core();
    restarted
        .restore_managed_checkpoint(checkpoint.clone())
        .unwrap();
    assert_eq!(restarted.session_count(), 0);
    assert_eq!(restarted.export_managed_checkpoint().unwrap(), checkpoint);
    let fenced = ManagedRequest::Put {
        session: SessionKey::new(NamespaceId::new(2), SessionId::new(3)),
        revision: 1,
        allocated_ccu: 1,
        tick_rate_hz: None,
        credentials: Credentials::new("b".repeat(64)),
    };
    assert_eq!(
        restarted.manage_at(fenced, Instant::now()),
        Err(ManagedError::ScopeRetired)
    );
    assert_eq!(
        restarted.manage_at(put(), Instant::now()),
        Err(ManagedError::ScopeRetired)
    );
}

#[tokio::test]
async fn existing_spawn_worker_stays_ephemeral() {
    let worker = spawn_worker(TransportIndependentWorker::new(core()));
    worker.manage(put()).await.unwrap();
    worker.manage(patch(2)).await.unwrap();
    assert!(!worker.is_closed());
}
