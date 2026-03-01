// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Builder for transform stages

use std::collections::HashSet;
use std::sync::Arc;

use crate::metrics::instrumentation::StageInstrumentation;
use crate::stages::common::control_strategies::{ControlEventStrategy, JonestownStrategy};
use crate::stages::common::cycle_guard::CycleGuard;
use crate::stages::common::handlers::{AsyncTransformHandler, TransformHandler};
use crate::stages::resources_builder::StageResources;
use crate::supervised_base::{
    BuilderError, ChannelBuilder, HandleBuilder, HandlerSupervisedExt,
    HandlerSupervisedWithExternalEvents, SupervisorBuilder, SupervisorTaskBuilder,
};

use super::config::TransformConfig;
use super::fsm::{TransformContext, TransformState};
use super::handle::TransformHandle;
use super::supervisor::TransformSupervisor;
use crate::stages::common::handlers::transform::traits::AsyncTransformHandlerAdapter;

/// Builder for creating transform stages
pub struct TransformBuilder<H: TransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static> {
    handler: H,
    config: TransformConfig,
    resources: StageResources,
    instrumentation: Option<Arc<StageInstrumentation>>,
}

impl<H: TransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static> TransformBuilder<H> {
    /// Create a new transform builder with StageResources
    pub fn new(handler: H, config: TransformConfig, resources: StageResources) -> Self {
        Self {
            handler,
            config,
            resources,
            instrumentation: None,
        }
    }

    /// Set the instrumentation for this transform
    pub fn with_instrumentation(mut self, instrumentation: Arc<StageInstrumentation>) -> Self {
        self.instrumentation = Some(instrumentation);
        self
    }

    /// Set a custom control strategy (defaults to JonestownStrategy)
    pub fn with_control_strategy(mut self, strategy: Arc<dyn ControlEventStrategy>) -> Self {
        self.config.control_strategy = Some(strategy);
        self
    }
}

#[async_trait::async_trait]
impl<H: TransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static> SupervisorBuilder
    for TransformBuilder<H>
{
    type Handle = TransformHandle<H>;
    type Error = BuilderError;

    async fn build(self) -> Result<Self::Handle, Self::Error> {
        // Create channels for supervisor communication
        let (event_sender, event_receiver, state_watcher) =
            ChannelBuilder::new().build(TransformState::<H>::Created);

        // Use provided strategy or default to JonestownStrategy
        let control_strategy = self
            .config
            .control_strategy
            .unwrap_or_else(|| Arc::new(JonestownStrategy));

        // Create instrumentation if not provided
        let instrumentation = self
            .instrumentation
            .unwrap_or_else(|| Arc::new(StageInstrumentation::new()));

        let cycle_guard_config = self.config.cycle_guard.clone();

        // Create context with bound subscription factory from resources
        let context = TransformContext {
            handler: self.handler,
            stage_id: self.config.stage_id,
            stage_name: self.config.stage_name.clone(),
            flow_name: self.config.flow_name.clone(),
            flow_id: self.resources.flow_id,
            data_journal: self.resources.data_journal.clone(),
            error_journal: self.resources.error_journal.clone(),
            system_journal: self.resources.system_journal.clone(),
            writer_id: None,
            subscription: None,
            contract_state: Vec::new(),
            last_contract_check: None,
            control_strategy,
            buffered_eof: None,
            instrumentation,
            upstream_subscription_factory: self.resources.upstream_subscription_factory,
            backpressure_writer: self.resources.backpressure_writer.clone(),
            backpressure_readers: self.resources.backpressure_readers.clone(),
            pending_outputs: std::collections::VecDeque::new(),
            pending_parent: None,
            pending_ack_upstream: None,
            backpressure_pulse:
                crate::stages::common::backpressure_activity_pulse::BackpressureActivityPulse::new(),
            backpressure_backoff:
                crate::supervised_base::idle_backoff::IdleBackoff::exponential_with_cap(
                    std::time::Duration::from_millis(1),
                    std::time::Duration::from_millis(50),
                ),
            backpressure_registry: self.resources.backpressure_registry.clone(),
            cycle_guard_config: cycle_guard_config.clone(),
            external_eofs_received: HashSet::new(),
            drain_received: false,
            buffered_terminal_envelope: None,
        };

        // Create supervisor (private - not exposed)
        let supervisor = TransformSupervisor {
            name: format!("transform_{}", self.config.stage_name),
            data_journal: self.resources.data_journal.clone(),
            system_journal: self.resources.system_journal.clone(),
            stage_id: self.config.stage_id,
            subscription: None,
            cycle_guard: cycle_guard_config.as_ref().map(|cfg| {
                CycleGuard::new(
                    cfg.max_iterations,
                    cfg.scc_id,
                    cfg.is_entry_point,
                    self.config.stage_name.clone(),
                )
            }),
            _marker: std::marker::PhantomData,
        };

        // Clone what we need for the task
        let state_watcher_for_task = state_watcher.clone();

        // Spawn the supervisor task
        let supervisor_name = format!("transform_{}", self.config.stage_name);
        let task = SupervisorTaskBuilder::<TransformSupervisor<H>>::new(&supervisor_name).spawn(
            move || async move {
                let supervisor_with_events = HandlerSupervisedWithExternalEvents::new(
                    supervisor,
                    event_receiver,
                    state_watcher_for_task,
                );

                // Run with the wrapper
                HandlerSupervisedExt::run(
                    supervisor_with_events,
                    TransformState::<H>::Created,
                    context,
                )
                .await
            },
        );

        // Build and return handle
        HandleBuilder::new()
            .with_event_sender(event_sender)
            .with_state_watcher(state_watcher)
            .with_supervisor_task(task)
            .build_standard()
            .map_err(|e| BuilderError::Other(e.to_string()))
    }
}

/// Builder for creating async transform stages.
///
/// This is the async-aware counterpart to `TransformBuilder` that accepts an
/// `AsyncTransformHandler` and runs it through the same transform supervisor
/// semantics (error mapping, draining, contract tracking).
pub struct AsyncTransformBuilder<
    H: AsyncTransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
> {
    handler: H,
    config: TransformConfig,
    resources: StageResources,
    instrumentation: Option<Arc<StageInstrumentation>>,
}

impl<H: AsyncTransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    AsyncTransformBuilder<H>
{
    pub fn new(handler: H, config: TransformConfig, resources: StageResources) -> Self {
        Self {
            handler,
            config,
            resources,
            instrumentation: None,
        }
    }

    pub fn with_instrumentation(mut self, instrumentation: Arc<StageInstrumentation>) -> Self {
        self.instrumentation = Some(instrumentation);
        self
    }

    pub fn with_control_strategy(mut self, strategy: Arc<dyn ControlEventStrategy>) -> Self {
        self.config.control_strategy = Some(strategy);
        self
    }
}

#[async_trait::async_trait]
impl<H: AsyncTransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static> SupervisorBuilder
    for AsyncTransformBuilder<H>
{
    type Handle = TransformHandle<AsyncTransformHandlerAdapter<H>>;
    type Error = BuilderError;

    async fn build(self) -> Result<Self::Handle, Self::Error> {
        type Wrapped<H> = AsyncTransformHandlerAdapter<H>;

        // Create channels for supervisor communication
        let (event_sender, event_receiver, state_watcher) =
            ChannelBuilder::new().build(TransformState::<Wrapped<H>>::Created);

        // Use provided strategy or default to JonestownStrategy
        let control_strategy = self
            .config
            .control_strategy
            .unwrap_or_else(|| Arc::new(JonestownStrategy));

        // Create instrumentation if not provided
        let instrumentation = self
            .instrumentation
            .unwrap_or_else(|| Arc::new(StageInstrumentation::new()));

        let cycle_guard_config = self.config.cycle_guard.clone();

        // Create context with bound subscription factory from resources
        let context = TransformContext {
            handler: AsyncTransformHandlerAdapter(self.handler),
            stage_id: self.config.stage_id,
            stage_name: self.config.stage_name.clone(),
            flow_name: self.config.flow_name.clone(),
            flow_id: self.resources.flow_id,
            data_journal: self.resources.data_journal.clone(),
            error_journal: self.resources.error_journal.clone(),
            system_journal: self.resources.system_journal.clone(),
            writer_id: None,
            subscription: None,
            contract_state: Vec::new(),
            last_contract_check: None,
            control_strategy,
            buffered_eof: None,
            instrumentation,
            upstream_subscription_factory: self.resources.upstream_subscription_factory,
            backpressure_writer: self.resources.backpressure_writer.clone(),
            backpressure_readers: self.resources.backpressure_readers.clone(),
            pending_outputs: std::collections::VecDeque::new(),
            pending_parent: None,
            pending_ack_upstream: None,
            backpressure_pulse:
                crate::stages::common::backpressure_activity_pulse::BackpressureActivityPulse::new(),
            backpressure_backoff:
                crate::supervised_base::idle_backoff::IdleBackoff::exponential_with_cap(
                    std::time::Duration::from_millis(1),
                    std::time::Duration::from_millis(50),
                ),
            backpressure_registry: self.resources.backpressure_registry.clone(),
            cycle_guard_config: cycle_guard_config.clone(),
            external_eofs_received: HashSet::new(),
            drain_received: false,
            buffered_terminal_envelope: None,
        };

        // Create supervisor (private - not exposed)
        let supervisor = TransformSupervisor {
            name: format!("transform_{}", self.config.stage_name),
            data_journal: self.resources.data_journal.clone(),
            system_journal: self.resources.system_journal.clone(),
            stage_id: self.config.stage_id,
            subscription: None,
            cycle_guard: cycle_guard_config.as_ref().map(|cfg| {
                CycleGuard::new(
                    cfg.max_iterations,
                    cfg.scc_id,
                    cfg.is_entry_point,
                    self.config.stage_name.clone(),
                )
            }),
            _marker: std::marker::PhantomData,
        };

        // Clone what we need for the task
        let state_watcher_for_task = state_watcher.clone();

        // Spawn the supervisor task
        let supervisor_name = format!("transform_{}", self.config.stage_name);
        let task = SupervisorTaskBuilder::<TransformSupervisor<Wrapped<H>>>::new(&supervisor_name)
            .spawn(move || async move {
                // Create a wrapper that handles external events
                let supervisor_with_events = HandlerSupervisedWithExternalEvents::new(
                    supervisor,
                    event_receiver,
                    state_watcher_for_task,
                );

                // Run with the wrapper
                HandlerSupervisedExt::run(
                    supervisor_with_events,
                    TransformState::<Wrapped<H>>::Created,
                    context,
                )
                .await
            });

        // Build and return handle
        HandleBuilder::new()
            .with_event_sender(event_sender)
            .with_state_watcher(state_watcher)
            .with_supervisor_task(task)
            .build_standard()
            .map_err(|e| BuilderError::Other(e.to_string()))
    }
}
