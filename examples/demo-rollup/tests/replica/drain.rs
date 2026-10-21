use sov_modules_rollup_blueprint::shutdown::ShutdownRequest;
use sov_test_utils::TEST_NORMAL_SHUTDOWN_TIMEOUT;
use tokio::signal::unix::SignalKind;

use super::*;

/// A replica has nothing to drain: it exits at once without pinning a stop height.
#[tokio::test(flavor = "multi_thread")]
async fn test_replica_drain_exits_immediately_and_leaves_leader_sequencing() {
    let postgres = match PostgresData::create_postgres().await {
        Ok(pg) => pg,
        Err(CreatePostgresError::DockerNotSupported) => return,
        Err(CreatePostgresError::DockerError(e)) => {
            panic!("Failed to create Postgres container: {e}");
        }
    };

    let key_and_address = read_private_key::<S>("tx_signer_private_key.json");
    let ExternalDa {
        shutdown: da_shutdown,
        addr,
        ..
    } = start_external_mock_da(periodic_block_producing())
        .await
        .unwrap();

    let mut replica = start_rollup(
        addr,
        Some((
            postgres.clone(),
            "replica".into(),
            ConfiguredNodeRole::Replica,
        )),
    )
    .await;
    let leader = start_rollup(
        addr,
        Some((postgres, "primary".into(), ConfiguredNodeRole::Leader)),
    )
    .await;
    leader.wait_for_sequencer_ready().await.unwrap();
    replica.wait_for_sequencer_ready().await.unwrap();

    let token_id = config_gas_token_id();
    let receiver_addr = random_address();
    let mut bank_events = replica
        .api_client()
        .subscribe_to_events_with_filter("Bank/*")
        .await
        .unwrap();
    let transfer = |nonce| {
        build_transfer_token_tx::<S>(
            &key_and_address.private_key,
            token_id,
            receiver_addr,
            AMOUNT,
            nonce,
        )
    };

    leader.send_tx_to_sequencer(&transfer(0)).await.unwrap();
    wait_for_all_events_with_timeout(Duration::from_secs(10), 1, &mut bank_events).await;

    replica
        .shutdown_requester
        .send(ShutdownRequest::Signal(SignalKind::terminate()))
        .await
        .unwrap();
    replica
        .try_wait_for_rollup_to_shutdown(TEST_NORMAL_SHUTDOWN_TIMEOUT)
        .await
        .expect("replica should shut down without waiting on anything");

    let height = leader.height().await;
    leader.send_tx_to_sequencer(&transfer(1)).await.unwrap();
    leader.wait_for_height(height.get() + 1).await;
    assert_eq!(
        leader
            .client
            .get_balance::<S>(&receiver_addr, &token_id, None)
            .await
            .unwrap()
            .0,
        2 * AMOUNT
    );

    let _ = leader.shutdown().await;
    da_shutdown.shutdown();
}
