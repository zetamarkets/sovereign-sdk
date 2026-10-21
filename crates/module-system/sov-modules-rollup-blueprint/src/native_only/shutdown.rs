//! How a node reacts to a shutdown request, i.e. an OS signal or
//! [`Rollup::shutdown_requester`](super::Rollup::shutdown_requester).
//!
//! The OS signal task only forwards requests into a channel; a [`ShutdownPolicy`] chosen by
//! [`FullNodeBlueprint::shutdown_policy`](super::FullNodeBlueprint::shutdown_policy) owns the
//! channel and decides what each request means, including a repeated one.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use sov_db::ledger_db::LedgerDb;
use sov_modules_api::rest::ApiState;
use sov_modules_api::{DaSpec, Spec};
use sov_rollup_full_node_interface::{DaSyncState, StateUpdateReceiver};
use sov_rollup_interface::node::da::{DaService, SequencerRole};
use sov_rollup_interface::node::SyncStatus;
use sov_sequencer::{DrainError, SequencerControl};
use sov_shutdown::{BackgroundHandle, PrimaryShutdownController, SecondaryShutdownController};
use sov_stf_runner::RollupConfig;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::{mpsc, watch};
use tracing::{debug, info, warn};

/// What asked the node to shut down.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownRequest {
    /// `SIGINT`, `SIGTERM` (what Kubernetes sends on a rollout, scale-down or pod deletion) or
    /// `SIGQUIT`.
    Signal(SignalKind),
    /// Sent through [`Rollup::shutdown_requester`](super::Rollup::shutdown_requester).
    Programmatic,
}

/// Handles to the running node that a [`ShutdownPolicy`] may use.
pub struct ShutdownContext<S: Spec, Da: DaService> {
    /// Stops the main loop and node-level tasks. Triggering it is how every policy ends.
    pub primary_shutdown: PrimaryShutdownController,
    /// Stops shared infrastructure (DA service, metrics). [`Rollup::run`](super::Rollup::run)
    /// triggers it after the runner exits.
    pub secondary_shutdown: SecondaryShutdownController,
    /// `None` when the configured sequencer cannot be drained.
    pub sequencer_control: Option<Arc<dyn SequencerControl>>,
    /// The role the sequencer resolved at startup.
    pub sequencer_role: SequencerRole,
    /// The DA layer client.
    pub da_service: Arc<Da>,
    /// The node's ledger database.
    pub ledger_db: LedgerDb,
    /// The ledger database the REST API reads from.
    pub api_ledger_db: LedgerDb,
    /// Rollup state as exposed by the REST API.
    pub api_state: ApiState<S>,
    /// Progress of the DA sync.
    pub da_sync_state: Arc<DaSyncState>,
    /// Live sync status of the node.
    pub sync_status_receiver: watch::Receiver<SyncStatus>,
    /// Notified on every state update.
    pub state_update_receiver: StateUpdateReceiver<S::Storage>,
    /// The DA address this node publishes from.
    pub da_address: <S::Da as DaSpec>::Address,
    /// The node's configuration.
    pub rollup_config: RollupConfig<S::Address, Da>,
}

/// Decides how shutdown requests are handled.
#[async_trait]
pub trait ShutdownPolicy<S: Spec, Da: DaService>: Send {
    /// Runs until it has triggered `ctx.primary_shutdown`. The task is cancelled once the primary
    /// shutdown fires, so returning early without triggering it only means the node keeps running.
    async fn run(
        self: Box<Self>,
        requests: mpsc::Receiver<ShutdownRequest>,
        ctx: ShutdownContext<S, Da>,
    );
}

/// Triggers the primary shutdown on the first request.
#[derive(Debug, Clone, Copy, Default)]
pub struct ImmediateShutdown;

#[async_trait]
impl<S: Spec, Da: DaService> ShutdownPolicy<S, Da> for ImmediateShutdown {
    async fn run(
        self: Box<Self>,
        mut requests: mpsc::Receiver<ShutdownRequest>,
        ctx: ShutdownContext<S, Da>,
    ) {
        if let Some(request) = requests.recv().await {
            info!(?request, "Shutting down");
            ctx.primary_shutdown.shutdown();
        }
    }
}

/// Drains the sequencer before shutting down, so nothing soft-confirmed is left for the next
/// leader: closes the open batch, refuses new transactions and waits until every published batch
/// is final on DA and applied by this node.
///
/// Shuts down at once when there is nothing to drain (no drainable sequencer, or a replica),
/// on a second request, when the sequencer is unavailable, or after `timeout`.
#[derive(Debug, Clone, Copy)]
pub struct DrainThenShutdown {
    /// Upper bound on the whole drain. Must stay below the orchestrator's kill deadline
    /// (`terminationGracePeriodSeconds` on Kubernetes).
    pub timeout: Duration,
    /// How often the in-flight batch count is checked.
    pub poll_interval: Duration,
}

impl Default for DrainThenShutdown {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(120),
            poll_interval: Duration::from_millis(250),
        }
    }
}

impl DrainThenShutdown {
    /// `timeout` with the default poll interval.
    pub fn with_timeout(timeout: Duration) -> Self {
        Self {
            timeout,
            ..Self::default()
        }
    }

    async fn drain(&self, control: &dyn SequencerControl) -> Result<(), DrainError> {
        let started = Instant::now();
        let stop_height = control.begin_drain().await?;
        info!(%stop_height, "Sequencer stopped accepting transactions, waiting for unfinalized batches");
        loop {
            let unfinalized = control.unfinalized_batches().await?;
            if unfinalized == 0 {
                info!(%stop_height, elapsed = ?started.elapsed(), "Sequencer drained");
                return Ok(());
            }
            debug!(unfinalized, elapsed = ?started.elapsed(), "Waiting for unfinalized batches");
            tokio::time::sleep(self.poll_interval).await;
        }
    }
}

#[async_trait]
impl<S: Spec, Da: DaService> ShutdownPolicy<S, Da> for DrainThenShutdown {
    async fn run(
        self: Box<Self>,
        mut requests: mpsc::Receiver<ShutdownRequest>,
        ctx: ShutdownContext<S, Da>,
    ) {
        let Some(request) = requests.recv().await else {
            warn!("Shutdown request channel closed, the node can no longer be stopped by signal");
            return;
        };
        let Some(control) = ctx.sequencer_control.as_deref() else {
            info!(?request, "Shutting down: no sequencer to drain");
            ctx.primary_shutdown.shutdown();
            return;
        };

        info!(?request, timeout = ?self.timeout, "Draining the sequencer before shutting down");
        tokio::select! {
            result = tokio::time::timeout(self.timeout, self.drain(control)) => match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => warn!(%error, "Sequencer drain failed, shutting down now"),
                Err(_) => warn!(timeout = ?self.timeout, "Sequencer drain timed out, shutting down now"),
            },
            Some(request) = requests.recv() => {
                warn!(?request, "Second shutdown request during drain, shutting down now");
            }
        }
        ctx.primary_shutdown.shutdown();
    }
}

/// Forwards `SIGINT`, `SIGTERM` and `SIGQUIT` into `requests` until the primary shutdown fires.
pub(super) fn spawn_os_signal_forwarder(
    primary_shutdown: PrimaryShutdownController,
    requests: mpsc::Sender<ShutdownRequest>,
) {
    BackgroundHandle::spawn("os-signal-handler", async move {
        let mut interrupt =
            signal(SignalKind::interrupt()).expect("Failed to set up SIGINT handler");
        let mut terminate =
            signal(SignalKind::terminate()).expect("Failed to set up SIGTERM handler");
        let mut quit = signal(SignalKind::quit()).expect("Failed to set up SIGQUIT handler");

        loop {
            let request = tokio::select! {
                _ = interrupt.recv() => ShutdownRequest::Signal(SignalKind::interrupt()),
                _ = terminate.recv() => ShutdownRequest::Signal(SignalKind::terminate()),
                _ = quit.recv() => ShutdownRequest::Signal(SignalKind::quit()),
                _ = primary_shutdown.wait_for_shutdown() => {
                    debug!("Stopping OS signal handling task, as rollup has been stopped programmatically");
                    return;
                }
            };
            info!(?request, "Received OS signal");
            if requests.send(request).await.is_err() {
                return;
            }
        }
    });
}

/// Runs `policy` until it triggers the primary shutdown or the primary shutdown fires on its own.
pub(super) fn spawn_shutdown_policy<S: Spec, Da: DaService>(
    policy: Box<dyn ShutdownPolicy<S, Da>>,
    requests: mpsc::Receiver<ShutdownRequest>,
    ctx: ShutdownContext<S, Da>,
) -> BackgroundHandle<()> {
    let primary_shutdown = ctx.primary_shutdown.clone();
    BackgroundHandle::spawn("shutdown-policy", async move {
        let _ = primary_shutdown
            .future_or_shutdown(policy.run(requests, ctx))
            .await;
    })
}
