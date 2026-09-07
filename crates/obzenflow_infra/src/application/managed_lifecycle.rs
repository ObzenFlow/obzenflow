// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Application coordination over Runtime-owned stop admission and publication.

use obzenflow_runtime::pipeline::{FlowCancelCause, FlowHandle, FlowStopStatus, PipelineState};
use obzenflow_runtime::supervised_base::SupervisorHandle;
use std::time::{Duration, Instant};

use super::config::{OnTerminalArg, StartupMode};
use super::flow_application::ShutdownSignal;
use super::ApplicationError;
use crate::web::host_error::ManagedWebHostError;
use crate::web::managed_host::ManagedWebHost;

struct Signals {
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
    #[cfg(test)]
    injected: Option<tokio::sync::oneshot::Receiver<ShutdownSignal>>,
    #[cfg(test)]
    test_only: bool,
}

impl Signals {
    fn new(
        #[cfg(test)] injected: Option<tokio::sync::oneshot::Receiver<ShutdownSignal>>,
    ) -> Result<Self, ApplicationError> {
        Ok(Self {
            #[cfg(unix)]
            terminate: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?,
            #[cfg(test)]
            test_only: injected.is_some(),
            #[cfg(test)]
            injected,
        })
    }

    async fn recv(&mut self) -> ShutdownSignal {
        #[cfg(test)]
        if self.test_only {
            return match &mut self.injected {
                Some(receiver) => {
                    let signal = receiver.await.unwrap_or(ShutdownSignal::Sigint);
                    self.injected.take();
                    signal
                }
                None => std::future::pending().await,
            };
        }
        #[cfg(unix)]
        {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => ShutdownSignal::Sigint,
                _ = self.terminate.recv() => ShutdownSignal::Sigterm,
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
            ShutdownSignal::Sigint
        }
    }
}

enum Trigger {
    Host(ManagedWebHostError),
    Signal(ShutdownSignal),
    Terminal,
    Startup(ApplicationError),
}

#[derive(Clone, Copy)]
enum StopCommand {
    Graceful,
    Cancel,
    Timeout,
}

impl StopCommand {
    async fn send(self, flow: &FlowHandle, grace: Duration) -> Result<(), ApplicationError> {
        let result = match self {
            Self::Graceful => flow.stop_graceful(grace).await,
            Self::Cancel => flow.stop_cancel().await,
            Self::Timeout => flow.stop_cancel_timeout().await,
        };
        result.map_err(|error| ApplicationError::FlowExecutionFailed(error.to_string()))
    }
}

pub(super) async fn supervise(
    host: &mut ManagedWebHost,
    flow: &FlowHandle,
    startup: StartupMode,
    on_terminal: OnTerminalArg,
    grace: Duration,
    initial_failure: Option<ManagedWebHostError>,
    #[cfg(test)] injected_signal: Option<tokio::sync::oneshot::Receiver<ShutdownSignal>>,
) -> Result<(), ApplicationError> {
    let signals = Signals::new(
        #[cfg(test)]
        injected_signal,
    );
    let mut signals = match signals {
        Ok(signals) => Some(signals),
        Err(error) => {
            let trigger = if let Some(host_error) = initial_failure {
                tracing::warn!(%error, "Signal setup also failed after managed host failure");
                Trigger::Host(host_error)
            } else {
                Trigger::Startup(error)
            };
            return settle(host, flow, grace, trigger, &mut None).await;
        }
    };
    let mut trigger = initial_failure.map(Trigger::Host);
    if trigger.is_none() && startup == StartupMode::Auto {
        tracing::info!("Starting flow execution (startup_mode=auto)");
        trigger = tokio::select! {
            biased;
            error = host.failure() => Some(Trigger::Host(error)),
            signal = next_signal(&mut signals) => Some(Trigger::Signal(signal)),
            result = flow.start() => result.err().map(|error| Trigger::Startup(
                ApplicationError::FlowExecutionFailed(error.to_string())
            )),
        };
    }
    let trigger = match trigger {
        Some(trigger) => trigger,
        None => {
            if startup == StartupMode::Manual {
                tracing::info!("startup_mode=manual; waiting for Play via /api/flow/control");
            }
            let mut state = flow.state_receiver();
            tokio::select! {
                biased;
                error = host.failure() => Trigger::Host(error),
                signal = next_signal(&mut signals) => Trigger::Signal(signal),
                _ = async {
                    loop {
                        if state.borrow_and_update().is_terminal() || !flow.is_running() { break; }
                        tokio::select! {
                            _ = state.changed() => {},
                            _ = tokio::time::sleep(Duration::from_millis(20)) => {},
                        }
                    }
                }, if on_terminal == OnTerminalArg::Exit => Trigger::Terminal,
            }
        }
    };
    settle(host, flow, grace, trigger, &mut signals).await
}

async fn next_signal(signals: &mut Option<Signals>) -> ShutdownSignal {
    match signals {
        Some(signals) => signals.recv().await,
        None => std::future::pending().await,
    }
}

/// Derive the outer bound from Runtime admission, never from a repeated request.
fn completion_deadline(status: &FlowStopStatus, not_admitted: Instant, grace: Duration) -> Instant {
    match status {
        FlowStopStatus::NotRequested => not_admitted,
        FlowStopStatus::Graceful { deadline } => *deadline + grace,
        FlowStopStatus::Cancelling {
            admitted_at,
            cause,
            graceful_deadline,
        } => {
            if *cause == FlowCancelCause::GracefulTimeout {
                graceful_deadline.unwrap_or(*admitted_at) + grace
            } else {
                *admitted_at + grace
            }
        }
    }
}

async fn settle(
    host: &mut ManagedWebHost,
    flow: &FlowHandle,
    grace: Duration,
    trigger: Trigger,
    signals: &mut Option<Signals>,
) -> Result<(), ApplicationError> {
    // Subscribe before sending. A successful send only means it entered the queue.
    let mut stop = flow.stop_status_receiver();
    let (mut host_error, mut other_error, command) = match trigger {
        Trigger::Host(error) => (Some(error), None, StopCommand::Graceful),
        Trigger::Startup(error) => (None, Some(error), StopCommand::Graceful),
        Trigger::Signal(ShutdownSignal::Sigint) => (None, None, StopCommand::Cancel),
        Trigger::Signal(ShutdownSignal::Sigterm) | Trigger::Terminal => {
            (None, None, StopCommand::Graceful)
        }
    };
    let current = flow.current_state();
    let mut pending = if !flow.is_running() || current.is_terminal() {
        None
    } else if matches!(*stop.borrow(), FlowStopStatus::Graceful { .. })
        && matches!(command, StopCommand::Cancel)
    {
        Some(StopCommand::Cancel)
    } else if !matches!(*stop.borrow(), FlowStopStatus::NotRequested) {
        None
    } else if matches!(
        current,
        PipelineState::Created
            | PipelineState::Materializing
            | PipelineState::Materialized
            | PipelineState::ReadyForRun
    ) {
        Some(StopCommand::Cancel)
    } else {
        Some(command)
    };
    let not_admitted = Instant::now() + grace;
    let mut timeout_requested = false;
    let mut observation_open = true;
    let mut publication = Box::pin(flow.wait_for_termination());
    loop {
        let status = stop.borrow_and_update().clone();
        let deadline = completion_deadline(&status, not_admitted, grace);
        let graceful_deadline = match status {
            FlowStopStatus::Graceful { deadline } => deadline,
            _ => deadline,
        };
        tokio::select! {
            biased;
            error = host.failure(), if host_error.is_none() => host_error = Some(error),
            result = &mut publication => {
                if let Err(error) = result {
                    let message = match &error {
                        obzenflow_runtime::errors::FlowError::ExecutionFailed(source) => format!("{error}: {source}"),
                        _ => error.to_string(),
                    };
                    other_error.get_or_insert(ApplicationError::FlowExecutionFailed(message));
                }
                break;
            }
            _ = tokio::time::sleep_until(deadline.into()) => {
                other_error.get_or_insert_with(|| ApplicationError::FlowExecutionFailed(
                    "Pipeline did not finish terminal publication within the admitted shutdown bound".into()
                ));
                // The retained completion remains available to emergency teardown.
                break;
            }
            changed = stop.changed(), if observation_open => {
                observation_open = changed.is_ok();
            }
            signal = next_signal(signals) => {
                if matches!(signal, ShutdownSignal::Sigint | ShutdownSignal::Sigterm)
                    && !matches!(status, FlowStopStatus::Cancelling { .. }) {
                    pending = Some(StopCommand::Cancel);
                }
            }
            result = async { pending.expect("guarded stop command").send(flow, grace).await }, if pending.is_some() => {
                pending = None;
                if let Err(error) = result { other_error.get_or_insert(error); }
            }
            _ = tokio::time::sleep_until(graceful_deadline.into()),
                if matches!(status, FlowStopStatus::Graceful { .. }) && !timeout_requested => {
                timeout_requested = true;
                pending = Some(StopCommand::Timeout);
            }
        }
    }
    // Drop this observer before emergency teardown; the shared join remains retained.
    drop(publication);
    if flow.is_running() {
        if let Err(error) = flow.abort_and_wait().await {
            tracing::warn!(%error, "Pipeline abort after shutdown deadline failed");
        }
    }
    if let Some(error) = host_error {
        if let Some(secondary) = other_error {
            tracing::warn!(%secondary, "Cleanup after managed host failure also failed");
        }
        return Err(ApplicationError::Other(Box::new(error)));
    }
    if let Some(error) = other_error {
        return Err(error);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observer_deadlines_come_from_admission_even_when_observed_late() {
        let now = Instant::now();
        let grace = Duration::from_secs(5);
        let deadline = now + Duration::from_secs(3);
        let late_observer = now + Duration::from_secs(100);
        let graceful = FlowStopStatus::Graceful { deadline };
        assert_eq!(
            completion_deadline(&graceful, late_observer, grace),
            deadline + grace
        );
        let timeout = FlowStopStatus::Cancelling {
            admitted_at: deadline + Duration::from_secs(2),
            cause: FlowCancelCause::GracefulTimeout,
            graceful_deadline: Some(deadline),
        };
        assert_eq!(
            completion_deadline(&timeout, late_observer, grace),
            deadline + grace
        );
        let explicit = FlowStopStatus::Cancelling {
            admitted_at: now,
            cause: FlowCancelCause::Requested,
            graceful_deadline: None,
        };
        assert_eq!(
            completion_deadline(&explicit, late_observer, grace),
            now + grace
        );
    }
}
