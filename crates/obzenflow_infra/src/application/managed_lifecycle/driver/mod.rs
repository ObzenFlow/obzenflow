// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Private application FSM driver. Runtime owns execution; the host owns its subtree.
//! Futures and original errors live here, outside cloneable transition payloads.

#[cfg(test)]
mod tests;

use super::machine;
#[cfg(feature = "warp-server")]
use super::signals;

use futures::future::BoxFuture;
use machine::{Action, Context, Event, FlowActivity, State, StopCommand, StopInput, StopReason};
use machine::{FailureOrigin, Outcome};
use obzenflow_fsm::StateVariant;
use obzenflow_runtime::__private::lifecycle;
use obzenflow_runtime::errors::FlowError;
use obzenflow_runtime::pipeline::{FlowHandle, PipelineState};
use obzenflow_runtime::supervised_base::SupervisorHandle;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::{AbortHandle, JoinError, JoinHandle};
use tokio::time::Instant as TokioInstant;

use crate::application::config::OnTerminalArg;
#[cfg(feature = "warp-server")]
use crate::application::config::StartupMode;
use crate::application::ApplicationError;
#[cfg(feature = "warp-server")]
use crate::web::surface_metrics::HttpSurfaceMetricsEmitter;
#[cfg(feature = "warp-server")]
use crate::web::{host_error::ManagedWebHostError, managed_host::ManagedWebHost};

/// Drop requests cancellation; only an awaited join establishes termination.
pub(in crate::application) struct ApplicationTask(pub JoinHandle<()>);

impl ApplicationTask {
    async fn join(mut self) -> Result<(), JoinError> {
        (&mut self.0).await
    }
}

impl Drop for ApplicationTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn abort_and_join(tasks: Vec<ApplicationTask>) -> Vec<JoinError> {
    // Cancel the whole group before waiting, including tasks after blocking work.
    for task in &tasks {
        task.0.abort();
    }
    let mut errors = Vec::new();
    for task in tasks {
        if let Err(error) = task.join().await {
            if !error.is_cancelled() {
                errors.push(error);
            }
        }
    }
    errors
}

enum Failure {
    Application(ApplicationError),
    Execution(FlowError),
    CompletionExpired,
}

impl Failure {
    fn into_application(self) -> ApplicationError {
        match self {
            Self::Application(error) => error,
            Self::Execution(error) => {
                let message = match &error {
                    FlowError::ExecutionFailed(source) => format!("{error}: {source}"),
                    _ => error.to_string(),
                };
                ApplicationError::FlowExecutionFailed(message)
            }
            Self::CompletionExpired => ApplicationError::FlowExecutionFailed(
                "Pipeline did not finish terminal publication within the admitted shutdown bound"
                    .into(),
            ),
        }
    }
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Application(error) => error.fmt(f),
            Self::Execution(error) => error.fmt(f),
            Self::CompletionExpired => f.write_str("terminal publication deadline expired"),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum ResourceGroup {
    MetricsCollector,
    StudioHeartbeat,
    HookAndSurfaceTasks,
}

/// Consumed once before delivering the corresponding typed FSM observation.
enum Observed {
    Started(Result<(), FlowError>),
    Standalone(Result<(), FlowError>),
    Publication(Result<(), FlowError>),
    Aborted(Result<(), FlowError>),
    Joined(ResourceGroup, Vec<JoinError>),
    #[cfg(feature = "warp-server")]
    HostClosed(Result<(), ManagedWebHostError>),
    HostAbsent,
    MetricsFlushed,
}

pub(in crate::application) struct ApplicationLifecycle {
    machine: machine::Machine,
    context: Context,
    pub tasks: Vec<ApplicationTask>,
    pub metrics_collector: Option<ApplicationTask>,
    #[cfg(feature = "studio-registration")]
    pub heartbeat: Option<ApplicationTask>,
    #[cfg(feature = "warp-server")]
    pub metrics_emitter: Option<HttpSurfaceMetricsEmitter>,
    #[cfg(feature = "warp-server")]
    host: Option<ManagedWebHost>,
    host_error: Option<ApplicationError>,
    failure: Option<Failure>,
    auxiliary_errors: Vec<JoinError>,
    execution_guard: Option<lifecycle::ExecutionGuard>,
    flow: Option<Arc<FlowHandle>>,
    stop: Option<lifecycle::StopObserver>,
    operation: Option<BoxFuture<'static, Observed>>,
    command: Option<BoxFuture<'static, Result<(), FlowError>>>,
    heartbeat_abort: Option<AbortHandle>,
    operation_started: TokioInstant,
}

impl ApplicationLifecycle {
    pub fn new(grace: Duration, on_terminal: OnTerminalArg) -> Self {
        Self {
            machine: machine::new(),
            context: Context {
                grace,
                outcome: Outcome::Success,
                on_terminal,
                commands: Vec::new(),
            },
            tasks: Vec::new(),
            metrics_collector: None,
            #[cfg(feature = "studio-registration")]
            heartbeat: None,
            #[cfg(feature = "warp-server")]
            metrics_emitter: None,
            #[cfg(feature = "warp-server")]
            host: None,
            host_error: None,
            failure: None,
            auxiliary_errors: Vec::new(),
            execution_guard: None,
            flow: None,
            stop: None,
            operation: None,
            command: None,
            heartbeat_abort: None,
            operation_started: TokioInstant::now(),
        }
    }

    pub async fn fail_before_run(
        &mut self,
        flow: Arc<FlowHandle>,
        error: ApplicationError,
    ) -> Result<(), ApplicationError> {
        self.attach_flow(flow);
        self.failure = Some(Failure::Application(error));
        let event = Event::Stop(StopReason::BeforeRun, self.stop_input());
        self.drive(
            event,
            #[cfg(feature = "warp-server")]
            None,
        )
        .await;
        self.take_result()
    }

    pub async fn run_standalone(&mut self, flow: FlowHandle) -> Result<(), ApplicationError> {
        self.protect_flow(&flow);
        self.flow = Some(Arc::new(flow));
        self.drive(
            Event::Standalone,
            #[cfg(feature = "warp-server")]
            None,
        )
        .await;
        self.take_result()
    }

    /// Completed runs have already joined. Other preparation failures have no flow.
    pub async fn finish(
        &mut self,
        result: Result<(), ApplicationError>,
    ) -> Result<(), ApplicationError> {
        if matches!(self.machine.state(), State::Finished) {
            return result;
        }
        self.failure = result.err().map(Failure::Application);
        self.drive(
            Event::PreparationFailed,
            #[cfg(feature = "warp-server")]
            None,
        )
        .await;
        self.take_result()
    }

    #[cfg(feature = "warp-server")]
    pub async fn run_hosted(
        &mut self,
        host: ManagedWebHost,
        flow: Arc<FlowHandle>,
        startup: StartupMode,
        initial_failure: Option<ManagedWebHostError>,
        #[cfg(test)] injected_signal: Option<
            tokio::sync::oneshot::Receiver<crate::application::flow_application::ShutdownSignal>,
        >,
    ) -> Result<(), ApplicationError> {
        self.host = Some(host);
        self.host_error = initial_failure.map(|error| ApplicationError::Other(Box::new(error)));
        self.attach_flow(flow);
        let signals = match signals::Signals::new(
            #[cfg(test)]
            injected_signal,
        ) {
            Ok(signals) => Some(signals),
            Err(error) => {
                self.failure = Some(Failure::Application(error));
                None
            }
        };
        let event = if self.host_error.is_some() || self.failure.is_some() {
            Event::Stop(StopReason::Graceful, self.stop_input())
        } else {
            Event::HostBound(startup)
        };
        self.drive(event, signals).await;
        self.take_result()
    }

    pub(in crate::application) fn protect_flow(&mut self, flow: &FlowHandle) {
        if self.execution_guard.is_none() {
            self.execution_guard = Some(lifecycle::guard_execution(flow));
        }
    }

    fn attach_flow(&mut self, flow: Arc<FlowHandle>) {
        self.protect_flow(&flow);
        // Subscribe before sending; queue success does not supply admission timestamps.
        self.stop = Some(lifecycle::observe_stop(&flow));
        self.flow = Some(flow);
    }

    fn stop_input(&mut self) -> StopInput {
        let flow = self
            .flow
            .as_ref()
            .expect("stop observation requires a flow");
        let state = flow.current_state();
        let activity = if !flow.is_running() || state.is_terminal() {
            FlowActivity::Terminal
        } else if matches!(
            state,
            PipelineState::Created
                | PipelineState::Materializing
                | PipelineState::Materialized
                | PipelineState::ReadyForRun
        ) {
            FlowActivity::BeforeRun
        } else {
            FlowActivity::Executing
        };
        StopInput {
            activity,
            admitted: self
                .stop
                .as_mut()
                .expect("subscribed before stop")
                .snapshot(),
            at: Instant::now(),
        }
    }

    async fn dispatch(&mut self, event: Event) {
        // Replay retained failure observations before advancing. The FSM selects
        // precedence; the driver retains both original errors. Re-observation is
        // deliberate and cannot restart an operation or an absolute deadline.
        for origin in [
            self.failure.as_ref().map(|_| FailureOrigin::Application),
            self.host_error.as_ref().map(|_| FailureOrigin::Host),
        ]
        .into_iter()
        .flatten()
        {
            let actions = self
                .machine
                .handle(Event::Failure(origin), &mut self.context)
                .await
                .expect("failure observations are infallible");
            debug_assert!(actions.is_empty());
        }
        let before = self.machine.state().variant_name().to_owned();
        let actions = self
            .machine
            .handle(event, &mut self.context)
            .await
            .expect("application transition handlers are infallible");
        let after = self.machine.state().variant_name();
        if before != after {
            tracing::debug!(
                from = before,
                to = after,
                "Application lifecycle phase changed"
            );
        }
        self.machine
            .execute_actions(actions, &mut self.context)
            .await
            .expect("application actions only enqueue driver commands");
        for action in std::mem::take(&mut self.context.commands) {
            self.execute(action);
        }
    }

    fn send_stop(&mut self, command: StopCommand) {
        let flow = self.flow.as_ref().expect("stop requires a flow").clone();
        let grace = self.context.grace;
        self.command = Some(Box::pin(async move {
            match command {
                StopCommand::Graceful => flow.stop_graceful(grace).await,
                StopCommand::Cancel => flow.stop_cancel().await,
                StopCommand::Timeout => lifecycle::cancel_after_timeout(&flow).await,
            }
        }));
    }

    fn begin(&mut self, future: BoxFuture<'static, Observed>) {
        self.operation_started = TokioInstant::now();
        self.operation = Some(future);
    }

    fn execute(&mut self, action: Action) {
        match action {
            Action::StartFlow => {
                tracing::info!("Starting flow execution (startup_mode=auto)");
                let flow = self.flow.as_ref().expect("bound host has a flow").clone();
                self.begin(Box::pin(async move {
                    // A supervisor can exit without publishing another state change.
                    // Completion must win over a stale readiness observation, while
                    // the outer Starting select still gives ready host faults/signals priority.
                    tokio::select! {
                        biased;
                        result = lifecycle::wait(&flow) => Observed::Publication(result),
                        result = flow.start() => Observed::Started(result),
                    }
                }));
            }
            Action::RunStandalone => {
                let flow = self
                    .flow
                    .take()
                    .expect("standalone execution owns its flow");
                self.begin(Box::pin(async move {
                    let flow = Arc::try_unwrap(flow)
                        .ok()
                        .expect("standalone flow is uniquely owned");
                    Observed::Standalone(flow.run().await)
                }));
            }
            Action::SettleFlow(command) => {
                let flow = self
                    .flow
                    .as_ref()
                    .expect("settlement requires a flow")
                    .clone();
                self.begin(Box::pin(async move {
                    Observed::Publication(lifecycle::wait(&flow).await)
                }));
                if let Some(command) = command {
                    self.send_stop(command);
                }
            }
            Action::SendStop(command) => self.send_stop(command),
            Action::AbortFlow => {
                // Drop observers before emergency teardown; Runtime retains the shared join.
                self.operation = None;
                self.command = None;
                self.stop = None;
                let flow = self
                    .flow
                    .as_ref()
                    .expect("settlement requires a flow")
                    .clone();
                self.begin(Box::pin(async move {
                    // Even an already-exited pipeline can have surviving stage tasks.
                    Observed::Aborted(flow.abort_and_wait().await)
                }));
            }
            Action::StopMetrics => {
                let tasks = self.metrics_collector.take().into_iter().collect();
                self.begin(Box::pin(async move {
                    Observed::Joined(ResourceGroup::MetricsCollector, abort_and_join(tasks).await)
                }));
            }
            Action::CloseHost => {
                #[cfg(feature = "warp-server")]
                if let Some(host) = self.host.take() {
                    self.begin(Box::pin(
                        async move { Observed::HostClosed(host.close().await) },
                    ));
                    return;
                }
                self.begin(Box::pin(async { Observed::HostAbsent }));
            }
            Action::AwaitDeregistration => {
                #[cfg(feature = "studio-registration")]
                if let Some(heartbeat) = self.heartbeat.take() {
                    self.heartbeat_abort = Some(heartbeat.0.abort_handle());
                    self.begin(Box::pin(async move {
                        let errors = heartbeat
                            .join()
                            .await
                            .err()
                            .into_iter()
                            .filter(|error| !error.is_cancelled())
                            .collect();
                        Observed::Joined(ResourceGroup::StudioHeartbeat, errors)
                    }));
                    return;
                }
                self.begin(Box::pin(async {
                    Observed::Joined(ResourceGroup::StudioHeartbeat, Vec::new())
                }));
            }
            Action::AbortDeregistration => {
                self.diagnose("Studio deregistration deadline expired; lease expiry will remove the registration", ResourceGroup::StudioHeartbeat);
                if let Some(abort) = &self.heartbeat_abort {
                    abort.abort();
                }
                // The pending join is retained even for non-abortable blocking work.
            }
            Action::FlushMetrics => {
                #[cfg(feature = "warp-server")]
                if let Some(emitter) = self.metrics_emitter.take() {
                    self.begin(Box::pin(async move {
                        emitter.flush().await;
                        Observed::MetricsFlushed
                    }));
                    return;
                }
                self.begin(Box::pin(async { Observed::MetricsFlushed }));
            }
            Action::JoinLeftoverHeartbeat => {
                #[cfg(feature = "studio-registration")]
                let tasks = self.heartbeat.take().into_iter().collect();
                #[cfg(not(feature = "studio-registration"))]
                let tasks = Vec::new();
                self.begin(Box::pin(async move {
                    Observed::Joined(ResourceGroup::StudioHeartbeat, abort_and_join(tasks).await)
                }));
            }
            Action::JoinTasks => {
                let tasks = std::mem::take(&mut self.tasks);
                self.begin(Box::pin(async move {
                    Observed::Joined(
                        ResourceGroup::HookAndSurfaceTasks,
                        abort_and_join(tasks).await,
                    )
                }));
            }
            Action::DiagnoseJoinBudget => {
                let group = match self.machine.state() {
                    State::JoiningLeftoverHeartbeat(_) => ResourceGroup::StudioHeartbeat,
                    _ => ResourceGroup::HookAndSurfaceTasks,
                };
                self.diagnose(
                    "Auxiliary join wait exceeded its budget; awaiting termination",
                    group,
                );
            }
        }
    }

    fn primary_diagnostic(&self) -> Option<String> {
        self.host_error
            .as_ref()
            .map(ToString::to_string)
            .or_else(|| self.failure.as_ref().map(ToString::to_string))
    }

    fn diagnose(&self, message: &str, group: ResourceGroup) {
        tracing::warn!(
            phase = self.machine.state().variant_name(), resource_group = ?group,
            elapsed_ms = self.operation_started.elapsed().as_millis() as u64,
            primary_error = ?self.primary_diagnostic(), "{message}"
        );
    }

    fn observe(&mut self, observed: Observed) -> Event {
        let at = TokioInstant::now();
        match observed {
            Observed::Started(Ok(())) => Event::Started,
            Observed::Started(Err(error)) => {
                tracing::debug!(%error, "Flow start did not complete; observing Runtime execution result");
                Event::Stop(StopReason::Graceful, self.stop_input())
            }
            Observed::Standalone(result) => {
                if let Err(error) = result {
                    self.failure.get_or_insert(Failure::Execution(error));
                }
                Event::StandaloneReturned
            }
            Observed::Publication(result) => {
                if let Err(error) = result {
                    self.failure.get_or_insert(Failure::Execution(error));
                }
                Event::PublicationObserved
            }
            Observed::Aborted(result) => {
                if let Err(error) = result {
                    tracing::warn!(%error, "Pipeline abort after shutdown deadline failed");
                }
                Event::FlowAborted
            }
            Observed::Joined(group, errors) => {
                for error in &errors {
                    tracing::warn!(%error, phase = self.machine.state().variant_name(),
                        resource_group = ?group,
                        elapsed_ms = self.operation_started.elapsed().as_millis() as u64,
                        primary_error = ?self.primary_diagnostic(),
                        "Application-owned task failed before joining");
                }
                self.auxiliary_errors.extend(errors);
                match group {
                    ResourceGroup::MetricsCollector => Event::MetricsStopped,
                    ResourceGroup::HookAndSurfaceTasks => Event::TasksJoined,
                    ResourceGroup::StudioHeartbeat => {
                        self.heartbeat_abort = None;
                        if matches!(self.machine.state(), State::JoiningLeftoverHeartbeat(_)) {
                            Event::LeftoverHeartbeatJoined { at }
                        } else {
                            Event::HeartbeatJoined { at }
                        }
                    }
                }
            }
            #[cfg(feature = "warp-server")]
            Observed::HostClosed(result) => {
                if let Err(error) = result {
                    self.retain_host_error(ApplicationError::Other(Box::new(error)));
                }
                Event::HostClosed { at }
            }
            Observed::HostAbsent => Event::HostAbsent { at },
            Observed::MetricsFlushed => Event::MetricsFlushed { at },
        }
    }

    fn retain_host_error(&mut self, error: ApplicationError) {
        if self.host_error.is_some() {
            tracing::warn!(%error, "Managed host close also failed");
        } else {
            self.host_error = Some(error);
        }
    }

    fn take_result(&mut self) -> Result<(), ApplicationError> {
        assert!(matches!(self.machine.state(), State::Finished));
        if let Some(guard) = self.execution_guard.take() {
            guard.disarm();
        }
        self.flow = None;
        match self.context.outcome {
            Outcome::HostFailure => {
                if let Some(secondary) = self.failure.take() {
                    tracing::warn!(%secondary, "Cleanup after managed host failure also failed");
                }
                Err(self
                    .host_error
                    .take()
                    .expect("observed host error is retained"))
            }
            Outcome::ApplicationFailure => Err(self
                .failure
                .take()
                .expect("observed application error is retained")
                .into_application()),
            Outcome::Success => Ok(()),
        }
    }

    async fn drive(
        &mut self,
        initial: Event,
        #[cfg(feature = "warp-server")] mut signals: Option<signals::Signals>,
    ) {
        self.dispatch(initial).await;
        loop {
            if matches!(self.machine.state(), State::Finished) {
                return;
            }
            // Preserve startup arbitration: an already-ready signal can withhold Run.
            // During settlement the publication observation instead wins a signal race.
            #[cfg(feature = "warp-server")]
            if matches!(self.machine.state(), State::Starting) {
                let event = tokio::select! {
                    biased;
                    error = host_failure(&mut self.host), if self.host_error.is_none() => {
                        self.retain_host_error(error);
                        Event::Stop(StopReason::Graceful, self.stop_input())
                    }
                    reason = signals::next(&mut signals) => Event::Stop(reason, self.stop_input()),
                    observed = pending(&mut self.operation) => {
                        self.operation = None;
                        self.observe(observed)
                    }
                };
                self.dispatch(event).await;
                continue;
            }
            // Like Runtime's observer contract, read the latest admitted status
            // before testing any deadline. A delayed wake may find both the old
            // admission-wait bound and a newer admission ready at once.
            if matches!(self.machine.state(), State::SettlingFlow(_)) {
                if let Some(stop) = &mut self.stop {
                    let admitted = stop.snapshot();
                    self.dispatch(Event::Admission(admitted)).await;
                }
            }
            let completion = match self.machine.state() {
                State::SettlingFlow(settlement) => Some(settlement.completion_deadline().into()),
                _ => None,
            };
            let graceful = match self.machine.state() {
                State::SettlingFlow(settlement) => settlement.graceful_deadline().map(Into::into),
                _ => None,
            };
            let auxiliary_deadline = match self.machine.state() {
                State::Deregistering { deadline } | State::FlushingMetrics { deadline } => {
                    Some(*deadline)
                }
                State::JoiningLeftoverHeartbeat(budget) | State::JoiningTasks(budget) => {
                    budget.pending_deadline()
                }
                _ => None,
            };
            let terminal = matches!(self.machine.state(), State::Active)
                && self.context.on_terminal == OnTerminalArg::Exit;
            let settling = matches!(self.machine.state(), State::SettlingFlow(_));
            let event = tokio::select! {
                biased;
                error = async {
                    #[cfg(feature = "warp-server")]
                    { host_failure(&mut self.host).await }
                    #[cfg(not(feature = "warp-server"))]
                    { std::future::pending::<ApplicationError>().await }
                }, if self.host_error.is_none() => {
                    self.retain_host_error(error);
                    if matches!(self.machine.state(), State::Preparing | State::Starting | State::Active) {
                        Event::Stop(StopReason::Graceful, self.stop_input())
                    } else { continue; }
                }
                observed = pending(&mut self.operation) => {
                    self.operation = None;
                    self.observe(observed)
                }
                _ = sleep_until(completion) => {
                    self.failure.get_or_insert(Failure::CompletionExpired);
                    Event::CompletionExpired
                }
                changed = stop_changed(&mut self.stop), if settling => {
                    if changed.is_err() { self.stop = None; continue; }
                    Event::Admission(self.stop.as_mut().expect("open stop observer").snapshot())
                }
                reason = async {
                    #[cfg(feature = "warp-server")]
                    { signals::next(&mut signals).await }
                    #[cfg(not(feature = "warp-server"))]
                    { std::future::pending::<StopReason>().await }
                } => {
                    if matches!(self.machine.state(), State::Starting | State::Active) {
                        Event::Stop(reason, self.stop_input())
                    } else { Event::RepeatedSignal }
                }
                _ = terminal_observation(&self.flow), if terminal => {
                    Event::Stop(StopReason::Graceful, self.stop_input())
                }
                result = pending(&mut self.command) => {
                    self.command = None;
                    if let Err(error) = result { self.failure.get_or_insert(Failure::Execution(error)); }
                    Event::StopSent
                }
                _ = sleep_until(graceful) => Event::GracefulExpired,
                _ = sleep_until(auxiliary_deadline) => match self.machine.state() {
                    State::Deregistering { .. } => Event::DeregistrationExpired,
                    State::FlushingMetrics { .. } => {
                        tracing::warn!(phase = self.machine.state().variant_name(),
                            elapsed_ms = self.operation_started.elapsed().as_millis() as u64,
                            primary_error = ?self.primary_diagnostic(),
                            "Final HTTP metrics flush timed out; publication is best-effort");
                        Event::FlushExpired { at: TokioInstant::now() }
                    }
                    _ => Event::JoinBudgetExpired,
                },
            };
            self.dispatch(event).await;
        }
    }
}

async fn pending<T>(operation: &mut Option<BoxFuture<'static, T>>) -> T {
    match operation {
        Some(operation) => operation.await,
        None => std::future::pending().await,
    }
}

async fn sleep_until(deadline: Option<TokioInstant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

async fn stop_changed(
    stop: &mut Option<lifecycle::StopObserver>,
) -> Result<(), lifecycle::StopObservationClosed> {
    match stop {
        Some(stop) => stop.changed().await,
        None => std::future::pending().await,
    }
}

#[cfg(feature = "warp-server")]
async fn host_failure(host: &mut Option<ManagedWebHost>) -> ApplicationError {
    match host {
        Some(host) => ApplicationError::Other(Box::new(host.failure().await)),
        None => std::future::pending().await,
    }
}

async fn terminal_observation(flow: &Option<Arc<FlowHandle>>) {
    let flow = flow.as_ref().expect("active host has a flow");
    let mut state = flow.state_receiver();
    loop {
        if state.borrow_and_update().is_terminal() || !flow.is_running() {
            return;
        }
        tokio::select! {
            _ = state.changed() => {},
            _ = tokio::time::sleep(Duration::from_millis(20)) => {},
        }
    }
}
