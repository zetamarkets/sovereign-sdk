//! Shutdown policies driven through [`TestRollup::shutdown_requester`], the programmatic
//! equivalent of `SIGTERM`.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sov_db::config::PrunerConfig;
use sov_mock_da::storable::StorableMockDaService;
use sov_mock_da::BlockProducingConfig;
use sov_modules_api::capabilities::TransactionAuthenticator;
use sov_modules_api::Runtime;
use sov_modules_rollup_blueprint::shutdown::{
    DrainThenShutdown, ImmediateShutdown, ShutdownContext, ShutdownPolicy, ShutdownRequest,
};
use sov_rollup_interface::node::da::DaService;
use sov_rollup_interface::TxHash;
use sov_test_utils::test_rollup::TestRollup;
use sov_test_utils::{
    RtAgnosticBlueprintWithShutdownPolicy, TestSpec, TestUser, TEST_BLOB_PROCESSING_TIMEOUT,
    TEST_DEFAULT_MOCK_DA_BLOCK_TIME_MS, TEST_FINALIZATION_BLOCKS, TEST_MAX_BATCH_SIZE,
    TEST_NORMAL_SHUTDOWN_TIMEOUT,
};
use tokio::signal::unix::SignalKind;
use tokio::sync::mpsc;

use super::{genesis_params_with_admin, send_tx, tx_set_value, TestRuntime};
use crate::utils::{new_test_rollup_with_shutdown_policy, MAX_BATCH_EXECUTION_TIME_MILLIS};

type Blueprint<P> = RtAgnosticBlueprintWithShutdownPolicy<TestSpec, TestRuntime<TestSpec>, P>;
type Auth = <TestRuntime<TestSpec> as Runtime<TestSpec>>::Auth;

const STOP_HEIGHT_ERROR: &str = "has reached the stop height";
/// Long enough for the drain to finalize a blob while the test produces DA blocks by hand.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(60);

/// [`DrainThenShutdown`] with a 5s timeout.
#[derive(Default)]
struct ShortDrain;

#[async_trait]
impl ShutdownPolicy<TestSpec, StorableMockDaService> for ShortDrain {
    async fn run(
        self: Box<Self>,
        requests: mpsc::Receiver<ShutdownRequest>,
        ctx: ShutdownContext<TestSpec, StorableMockDaService>,
    ) {
        Box::new(DrainThenShutdown::with_timeout(Duration::from_secs(5)))
            .run(requests, ctx)
            .await;
    }
}

async fn ready_rollup<P>() -> (TestRollup<Blueprint<P>>, TestUser<TestSpec>)
where
    P: ShutdownPolicy<TestSpec, StorableMockDaService> + Default + Sync + 'static,
{
    let (genesis_params, admin) = genesis_params_with_admin();
    let rollup = new_test_rollup_with_shutdown_policy::<TestRuntime<TestSpec>, P>(
        Arc::new(tempfile::tempdir().unwrap()),
        genesis_params
            .runtime
            .sequencer_registry
            .sequencer_config
            .seq_da_address,
        genesis_params,
        0,
        true,
        TEST_MAX_BATCH_SIZE,
        BlockProducingConfig::Manual,
        None,
        TEST_BLOB_PROCESSING_TIMEOUT,
        MAX_BATCH_EXECUTION_TIME_MILLIS,
        None,
        TEST_FINALIZATION_BLOCKS,
        PrunerConfig::Off,
    )
    .await;
    rollup.produce_enough_finalized_slots().await;
    rollup.wait_for_sequencer_ready().await.unwrap();
    (rollup, admin)
}

/// Soft-confirms `count` txs. With manual DA block production and no forced batch close they
/// stay in the open batch.
async fn soft_confirm_txs<P>(
    rollup: &TestRollup<Blueprint<P>>,
    admin: &TestUser<TestSpec>,
    count: u64,
) -> BTreeSet<TxHash>
where
    P: ShutdownPolicy<TestSpec, StorableMockDaService> + Default + Sync + 'static,
{
    let mut hashes = BTreeSet::new();
    for nonce in 0..count {
        let raw_tx = tx_set_value(&admin.private_key, nonce, nonce + 1);
        let baked_tx = Auth::encode_with_standard_auth(raw_tx.clone());
        hashes.insert(Auth::compute_tx_hash(&baked_tx).unwrap());
        rollup
            .api_client()
            .send_raw_tx_to_sequencer(&raw_tx)
            .await
            .unwrap();
    }
    hashes
}

async fn request_drain<P>(rollup: &TestRollup<Blueprint<P>>, admin: &TestUser<TestSpec>, nonce: u64)
where
    P: ShutdownPolicy<TestSpec, StorableMockDaService> + Default + Sync + 'static,
{
    rollup
        .shutdown_requester
        .send(ShutdownRequest::Signal(SignalKind::terminate()))
        .await
        .unwrap();
    rollup.wait_for_sequencer_not_ready().await.unwrap();
    let err = send_tx(admin, nonce, rollup.api_client())
        .await
        .unwrap_err();
    assert!(
        err.contains(STOP_HEIGHT_ERROR),
        "unexpected rejection: {err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn drain_publishes_open_batch_and_exits_once_it_is_final() {
    let (mut rollup, admin) = ready_rollup::<DrainThenShutdown>().await;
    let tx_hashes = soft_confirm_txs(&rollup, &admin, 3).await;
    let da_height_before = rollup
        .da_service
        .get_head_block_header()
        .await
        .unwrap()
        .height;

    request_drain(&rollup, &admin, 3).await;

    // The drain itself must close and submit the open batch; the test never forces a close.
    rollup
        .wait_for_txs_on_da(&tx_hashes, da_height_before, Duration::from_secs(15))
        .await
        .unwrap();
    let included_at = batch_inclusion_height(&rollup, da_height_before).await;

    // The node only exits once the batch is final and applied; keep DA moving until then.
    let deadline = tokio::time::Instant::now() + DRAIN_TIMEOUT;
    loop {
        match rollup
            .try_wait_for_rollup_to_shutdown(Duration::from_millis(
                TEST_DEFAULT_MOCK_DA_BLOCK_TIME_MS,
            ))
            .await
        {
            Ok(()) => break,
            Err(error) if tokio::time::Instant::now() < deadline => {
                assert!(
                    error
                        .to_string()
                        .contains("Failed to join rollup task before timeout"),
                    "rollup failed during drain: {error:#}"
                );
                rollup.da_service.produce_block_now().await.unwrap();
            }
            Err(error) => panic!("rollup did not exit after draining: {error:#}"),
        }
    }

    // The node must not have exited before the DA block holding the batch was final.
    let finalized_height = rollup
        .da_service
        .get_last_finalized_block_header()
        .await
        .unwrap()
        .height;
    assert!(
        finalized_height >= included_at,
        "node exited with the drained batch (DA block {included_at}) not final (finalized head {finalized_height})"
    );
}

async fn batch_inclusion_height<P>(rollup: &TestRollup<Blueprint<P>>, from: u64) -> u64
where
    P: ShutdownPolicy<TestSpec, StorableMockDaService> + Default + Sync + 'static,
{
    let head = rollup
        .da_service
        .get_head_block_header()
        .await
        .unwrap()
        .height;
    for height in from + 1..=head {
        let block = rollup.da_service.get_block_at(height).await.unwrap();
        if !block.batch_blobs.is_empty() {
            return height;
        }
    }
    panic!("no batch blob on DA between {from} and {head}");
}

#[tokio::test(flavor = "multi_thread")]
async fn second_request_during_drain_shuts_down_immediately() {
    let (mut rollup, admin) = ready_rollup::<DrainThenShutdown>().await;
    soft_confirm_txs(&rollup, &admin, 2).await;
    request_drain(&rollup, &admin, 2).await;

    // No DA blocks are produced, so the drain cannot complete on its own.
    rollup
        .shutdown_requester
        .send(ShutdownRequest::Signal(SignalKind::terminate()))
        .await
        .unwrap();
    rollup
        .try_wait_for_rollup_to_shutdown(TEST_NORMAL_SHUTDOWN_TIMEOUT)
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn drain_timeout_shuts_down() {
    let (mut rollup, admin) = ready_rollup::<ShortDrain>().await;
    soft_confirm_txs(&rollup, &admin, 2).await;
    request_drain(&rollup, &admin, 2).await;

    // No DA blocks are produced, so only the timeout can end the drain.
    rollup
        .try_wait_for_rollup_to_shutdown(Duration::from_secs(10))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn immediate_policy_shuts_down_on_request() {
    let (mut rollup, admin) = ready_rollup::<ImmediateShutdown>().await;
    soft_confirm_txs(&rollup, &admin, 1).await;
    rollup
        .shutdown_requester
        .send(ShutdownRequest::Programmatic)
        .await
        .unwrap();
    rollup
        .try_wait_for_rollup_to_shutdown(TEST_NORMAL_SHUTDOWN_TIMEOUT)
        .await
        .unwrap();
}
