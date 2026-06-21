// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Timing middleware for measuring stage processing time
//!
//! This middleware implements the wide events pattern by enriching events
//! with processing duration before they're written to the journal.

use crate::middleware::{
    validate_attachment_request, ControlMiddlewareRole, ErrorAction, Middleware, MiddlewareAction,
    MiddlewareAttachmentRequest, MiddlewareContext, MiddlewareDeclaration, MiddlewareFactory,
    MiddlewareOverrideKey, MiddlewarePlanContribution, MiddlewareSurfaceAttachment,
    MiddlewareSurfaceKind, SourceMiddlewarePhase, TopologyMiddlewareConfigSlot,
};
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::time::MetricsDuration;
use obzenflow_core::{
    HandlerMiddlewareObserver, HandlerObserverContext, JoinMiddlewareObserver, JoinObserverContext,
    ObserverCommitError, ObserverCommitResult, ObserverDeterminism, ObserverReport,
    OutputCommitObserver, OutputCommitObserverContext, SourcePollObserver,
    SourcePollObserverContext, StageId, StageObserverBundle, StatefulMiddlewareObserver,
    StatefulObserverContext,
};
use obzenflow_runtime::pipeline::config::StageConfig;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Middleware that measures processing time and adds it to events
///
/// This is a core system middleware that should be added to all stages
/// to provide automatic timing instrumentation.
#[derive(Debug, Clone)]
pub struct TimingMiddleware {
    starts: Arc<Mutex<HashMap<obzenflow_core::EventId, Instant>>>,
    output_durations: Arc<Mutex<HashMap<obzenflow_core::EventId, MetricsDuration>>>,
}

impl TimingMiddleware {
    /// Create a new timing middleware for a specific stage
    pub fn new(_stage_name: impl Into<String>) -> Self {
        Self {
            starts: Arc::new(Mutex::new(HashMap::new())),
            output_durations: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn bundle(stage_name: impl Into<String>, stage_id: StageId) -> StageObserverBundle {
        let observer = Arc::new(Self::new(stage_name));
        let mut bundle = StageObserverBundle::default();
        bundle.handler.push(observer.clone());
        bundle.stateful.push(observer.clone());
        bundle.join.push(observer.clone());
        bundle.source_poll.push(observer.clone());
        bundle.output_commit.push(observer);
        tracing::trace!(?stage_id, "created timing observer bundle");
        bundle
    }

    fn remember_start(&self, event: &ChainEvent) {
        let mut starts = self
            .starts
            .lock()
            .expect("TimingMiddleware starts lock poisoned");
        starts.insert(event.id, Instant::now());
    }

    fn duration_for_input(&self, event: &ChainEvent) -> MetricsDuration {
        let elapsed = self
            .starts
            .lock()
            .expect("TimingMiddleware starts lock poisoned")
            .remove(&event.id)
            .map(|start| start.elapsed())
            .unwrap_or(Duration::ZERO);
        MetricsDuration::from_nanos(elapsed.as_nanos().min(u64::MAX as u128) as u64)
    }

    fn stamp_outputs(&self, input: Option<&ChainEvent>, outputs: &mut [ChainEvent]) {
        let duration = input
            .map(|event| self.duration_for_input(event))
            .unwrap_or(MetricsDuration::ZERO);
        let mut output_durations = self
            .output_durations
            .lock()
            .expect("TimingMiddleware output durations lock poisoned");
        for output in outputs {
            output.processing_info.processing_time = duration;
            output_durations.insert(output.id, duration);
        }
    }

    fn stamp_source_outputs(&self, poll_duration: Duration, outputs: &mut [ChainEvent]) {
        let data_count = outputs
            .iter()
            .filter(|event| event.is_data())
            .count()
            .max(1);
        let nanos = (poll_duration.as_nanos() / data_count as u128).min(u64::MAX as u128) as u64;
        let duration = MetricsDuration::from_nanos(nanos);
        let mut output_durations = self
            .output_durations
            .lock()
            .expect("TimingMiddleware output durations lock poisoned");
        for output in outputs {
            if output.is_data() {
                output.processing_info.processing_time = duration;
                output_durations.insert(output.id, duration);
            }
        }
    }
}

impl Middleware for TimingMiddleware {
    fn label(&self) -> &'static str {
        "timing"
    }

    fn source_phase(&self) -> SourceMiddlewarePhase {
        SourceMiddlewarePhase::Ordinary
    }

    fn pre_handle(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
        let _ = ctx;

        self.remember_start(event);

        MiddlewareAction::Continue
    }

    fn post_handle(
        &self,
        _event: &ChainEvent,
        _results: &[ChainEvent],
        _ctx: &mut MiddlewareContext,
    ) {
        // Post-handle doesn't need to do anything - timing is added in pre_write
    }

    fn on_error(&self, _event: &ChainEvent, _ctx: &mut MiddlewareContext) -> ErrorAction {
        // Just propagate errors - timing middleware doesn't handle errors
        ErrorAction::Propagate
    }

    fn pre_write(&self, event: &mut ChainEvent, ctx: &MiddlewareContext) {
        let _ = (event, ctx);
    }
}

impl HandlerMiddlewareObserver for TimingMiddleware {
    fn label(&self) -> &'static str {
        "timing"
    }

    fn determinism(&self) -> ObserverDeterminism {
        ObserverDeterminism::LiveOnly
    }

    fn before_handle(&self, ctx: &HandlerObserverContext<'_>) -> ObserverReport {
        self.remember_start(ctx.input);
        ObserverReport::empty()
    }

    fn after_handle(
        &self,
        ctx: &HandlerObserverContext<'_>,
        outputs: &mut [ChainEvent],
    ) -> ObserverReport {
        self.stamp_outputs(Some(ctx.input), outputs);
        ObserverReport::empty()
    }
}

impl StatefulMiddlewareObserver for TimingMiddleware {
    fn label(&self) -> &'static str {
        "timing"
    }

    fn determinism(&self) -> ObserverDeterminism {
        ObserverDeterminism::LiveOnly
    }

    fn before_state_accumulate(&self, ctx: &StatefulObserverContext<'_>) -> ObserverReport {
        if let Some(input) = ctx.input {
            self.remember_start(input);
        }
        ObserverReport::empty()
    }

    fn after_state_emit(
        &self,
        ctx: &StatefulObserverContext<'_>,
        outputs: &mut [ChainEvent],
    ) -> ObserverReport {
        self.stamp_outputs(ctx.input, outputs);
        ObserverReport::empty()
    }
}

impl JoinMiddlewareObserver for TimingMiddleware {
    fn label(&self) -> &'static str {
        "timing"
    }

    fn determinism(&self) -> ObserverDeterminism {
        ObserverDeterminism::LiveOnly
    }

    fn before_join_input(&self, ctx: &JoinObserverContext<'_>) -> ObserverReport {
        if let Some(input) = ctx.input {
            self.remember_start(input);
        }
        ObserverReport::empty()
    }

    fn after_join_output(
        &self,
        ctx: &JoinObserverContext<'_>,
        outputs: &mut [ChainEvent],
    ) -> ObserverReport {
        self.stamp_outputs(ctx.input, outputs);
        ObserverReport::empty()
    }
}

impl SourcePollObserver for TimingMiddleware {
    fn label(&self) -> &'static str {
        "timing"
    }

    fn determinism(&self) -> ObserverDeterminism {
        ObserverDeterminism::LiveOnly
    }

    fn after_source_poll(
        &self,
        ctx: &SourcePollObserverContext<'_>,
        outputs: &mut [ChainEvent],
    ) -> ObserverReport {
        self.stamp_source_outputs(ctx.poll_duration, outputs);
        ObserverReport::empty()
    }
}

impl OutputCommitObserver for TimingMiddleware {
    fn label(&self) -> &'static str {
        "timing"
    }

    fn determinism(&self) -> ObserverDeterminism {
        ObserverDeterminism::LiveOnly
    }

    fn before_output_commit(
        &self,
        _ctx: &OutputCommitObserverContext<'_>,
        event: &mut ChainEvent,
    ) -> ObserverCommitResult {
        if let Some(duration) = self
            .output_durations
            .lock()
            .expect("TimingMiddleware output durations lock poisoned")
            .remove(&event.id)
        {
            event.processing_info.processing_time = duration;
        }
        Ok(ObserverReport::empty())
    }
}

/// Factory for creating TimingMiddleware instances with stage context
pub struct TimingFamily;

pub struct TimingMiddlewareFactory;

impl Default for TimingMiddlewareFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl TimingMiddlewareFactory {
    pub fn new() -> Self {
        Self
    }
}

impl MiddlewareFactory for TimingMiddlewareFactory {
    fn label(&self) -> &'static str {
        "timing"
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<TimingFamily>("timing")
    }

    fn control_role(&self) -> ControlMiddlewareRole {
        ControlMiddlewareRole::None
    }

    fn plan_contribution(&self) -> MiddlewarePlanContribution {
        MiddlewarePlanContribution::None
    }

    fn topology_config_slot(&self) -> Option<TopologyMiddlewareConfigSlot> {
        None
    }

    fn create(
        &self,
        config: &StageConfig,
        _control_middleware: std::sync::Arc<
            crate::middleware::control::ControlMiddlewareAggregator,
        >,
    ) -> crate::middleware::MiddlewareFactoryResult<Box<dyn Middleware>> {
        Ok(Box::new(TimingMiddleware::new(&config.name)))
    }

    fn declaration(&self) -> MiddlewareDeclaration {
        MiddlewareDeclaration::observer_with_family(
            self.label(),
            self.override_key().family_label(),
            vec![
                MiddlewareSurfaceKind::SourcePoll,
                MiddlewareSurfaceKind::Handler,
                MiddlewareSurfaceKind::Stateful,
                MiddlewareSurfaceKind::Join,
                MiddlewareSurfaceKind::OutputCommit,
            ],
        )
    }

    fn materialize(
        &self,
        request: MiddlewareAttachmentRequest<'_>,
        context: &crate::middleware::MiddlewareMaterializationContext<'_>,
    ) -> crate::middleware::MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
        let declaration = self.declaration();
        validate_attachment_request(&declaration, &request).map_err(|err| {
            crate::middleware::MiddlewareFactoryError::materialization_failed(
                self.label(),
                &context.config.name,
                err,
            )
        })?;
        let observer = Arc::new(TimingMiddleware::new(&context.config.name));
        match request.surface.kind() {
            MiddlewareSurfaceKind::SourcePoll => {
                Ok(MiddlewareSurfaceAttachment::SourcePollObserver(observer))
            }
            MiddlewareSurfaceKind::Handler => {
                Ok(MiddlewareSurfaceAttachment::HandlerObserver(observer))
            }
            MiddlewareSurfaceKind::Stateful => {
                Ok(MiddlewareSurfaceAttachment::StatefulObserver(observer))
            }
            MiddlewareSurfaceKind::Join => Ok(MiddlewareSurfaceAttachment::JoinObserver(observer)),
            MiddlewareSurfaceKind::OutputCommit => {
                Ok(MiddlewareSurfaceAttachment::OutputCommitObserver(observer))
            }
            surface => Err(
                crate::middleware::MiddlewareFactoryError::materialization_failed(
                    self.label(),
                    &context.config.name,
                    ObserverCommitError::new(format!(
                        "unsupported timing observer surface {surface:?}"
                    )),
                ),
            ),
        }
    }
}

/// Convenience function to create a timing middleware factory
pub fn timing() -> Box<dyn MiddlewareFactory> {
    Box::new(TimingMiddlewareFactory::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::{StageId, WriterId};
    use serde_json::json;
    use std::thread;

    #[test]
    fn test_timing_middleware_adds_processing_time() {
        let middleware = TimingMiddleware::new("test_stage");

        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test.event",
            json!({"data": "test"}),
        );

        middleware.remember_start(&event);
        thread::sleep(MetricsDuration::from_millis(10).to_std());

        let mut result_event = event.clone();
        middleware.stamp_outputs(Some(&event), std::slice::from_mut(&mut result_event));

        assert!(result_event.processing_info.processing_time.as_nanos() >= 9_000_000);
    }

    #[test]
    fn test_timing_middleware_handles_missing_start_time() {
        let middleware = TimingMiddleware::new("test_stage");
        let ctx = MiddlewareContext::live_handler(); // No start time set

        let mut event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test.event",
            json!({"data": "test"}),
        );

        // Pre-write should handle missing start time gracefully
        middleware.pre_write(&mut event, &ctx);

        // Processing time should remain at default (ZERO)
        assert_eq!(event.processing_info.processing_time, MetricsDuration::ZERO);
    }
}
