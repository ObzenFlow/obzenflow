// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Adapter-owned observer composition (FLOWIP-115f onion conformance).
//!
//! This module folds the resolved per-surface observer list for a stage into a
//! single composed port, exactly as `Per*PolicyBoundary` folds a control-policy
//! stack into one boundary. The runtime then holds one neutral port per surface
//! (`StageObserverBundle`) and calls it once. The list, the determinism gate,
//! and report merging live here, so the runtime never iterates an observer list
//! or evaluates observer determinism itself.

use std::sync::Arc;

use obzenflow_core::ChainEvent;
use obzenflow_runtime::{
    EffectObserver, EffectObserverContext, HandlerObserver, HandlerObserverContext,
    IngressObserver, IngressObserverContext, JoinObserver, JoinObserverContext,
    ObserverCommitResult, ObserverReport, OutputCommitObserver, OutputCommitObserverContext,
    SinkDeliveryObserver, SinkDeliveryObserverContext, SourcePollObserver,
    SourcePollObserverContext, StageLifecycleObserver, StageLifecycleObserverContext,
    StageObserverBundle, StatefulObserver, StatefulObserverContext,
};

use crate::middleware::MiddlewareSurfaceAttachment;

/// Accumulates the resolved observer attachments for one stage, then folds each
/// surface's list into one composed port via [`StageObserverSet::build`].
#[derive(Default)]
pub struct StageObserverSet {
    pub(crate) handler: Vec<Arc<dyn HandlerObserver>>,
    pub(crate) stateful: Vec<Arc<dyn StatefulObserver>>,
    pub(crate) join: Vec<Arc<dyn JoinObserver>>,
    pub(crate) source_poll: Vec<Arc<dyn SourcePollObserver>>,
    pub(crate) effect: Vec<Arc<dyn EffectObserver>>,
    pub(crate) sink_delivery: Vec<Arc<dyn SinkDeliveryObserver>>,
    pub(crate) ingress: Vec<Arc<dyn IngressObserver>>,
    pub(crate) output_commit: Vec<Arc<dyn OutputCommitObserver>>,
    pub(crate) stage_lifecycle: Vec<Arc<dyn StageLifecycleObserver>>,
}

impl StageObserverSet {
    /// Route one materialized observer attachment onto its surface list.
    /// Control attachments are rejected; the planner must not send them here.
    pub fn push_attachment(
        &mut self,
        attachment: MiddlewareSurfaceAttachment,
    ) -> Result<(), String> {
        match attachment {
            MiddlewareSurfaceAttachment::HandlerObserver(observer) => self.handler.push(observer),
            MiddlewareSurfaceAttachment::StatefulObserver(observer) => self.stateful.push(observer),
            MiddlewareSurfaceAttachment::JoinObserver(observer) => self.join.push(observer),
            MiddlewareSurfaceAttachment::SourcePollObserver(observer) => {
                self.source_poll.push(observer)
            }
            MiddlewareSurfaceAttachment::EffectObserver(observer) => self.effect.push(observer),
            MiddlewareSurfaceAttachment::SinkDeliveryObserver(observer) => {
                self.sink_delivery.push(observer)
            }
            MiddlewareSurfaceAttachment::IngressObserver(observer) => self.ingress.push(observer),
            MiddlewareSurfaceAttachment::OutputCommitObserver(observer) => {
                self.output_commit.push(observer)
            }
            MiddlewareSurfaceAttachment::StageLifecycleObserver(observer) => {
                self.stage_lifecycle.push(observer)
            }
            _ => {
                return Err(
                    "middleware materialized a control attachment while planning observers".into(),
                )
            }
        }
        Ok(())
    }

    /// Merge another set's lists into this one, preserving declared order.
    pub fn extend(&mut self, other: StageObserverSet) {
        self.handler.extend(other.handler);
        self.stateful.extend(other.stateful);
        self.join.extend(other.join);
        self.source_poll.extend(other.source_poll);
        self.effect.extend(other.effect);
        self.sink_delivery.extend(other.sink_delivery);
        self.ingress.extend(other.ingress);
        self.output_commit.extend(other.output_commit);
        self.stage_lifecycle.extend(other.stage_lifecycle);
    }

    /// Fold each surface's list into one composed runtime port. An empty list
    /// yields `None` so the runtime skips that surface entirely.
    pub fn build(self) -> StageObserverBundle {
        StageObserverBundle {
            handler: compose(self.handler, |list| Arc::new(HandlerObserverChain(list))),
            stateful: compose(self.stateful, |list| Arc::new(StatefulObserverChain(list))),
            join: compose(self.join, |list| Arc::new(JoinObserverChain(list))),
            source_poll: compose(self.source_poll, |list| {
                Arc::new(SourcePollObserverChain(list))
            }),
            effect: compose(self.effect, |list| Arc::new(EffectObserverChain(list))),
            sink_delivery: compose(self.sink_delivery, |list| {
                Arc::new(SinkDeliveryObserverChain(list))
            }),
            ingress: compose(self.ingress, |list| Arc::new(IngressObserverChain(list))),
            output_commit: compose(self.output_commit, |list| {
                Arc::new(OutputCommitObserverChain(list))
            }),
            stage_lifecycle: compose(self.stage_lifecycle, |list| {
                Arc::new(StageLifecycleObserverChain(list))
            }),
        }
    }
}

/// Wrap a non-empty list in its composed port, or return `None` when empty.
/// `make` returns the trait-object type so the concrete chain unsizes at the
/// closure return.
fn compose<T: ?Sized>(
    list: Vec<Arc<T>>,
    make: impl FnOnce(Vec<Arc<T>>) -> Arc<T>,
) -> Option<Arc<T>> {
    if list.is_empty() {
        None
    } else {
        Some(make(list))
    }
}

const CHAIN_LABEL: &str = "observer-chain";

struct HandlerObserverChain(Vec<Arc<dyn HandlerObserver>>);

impl HandlerObserver for HandlerObserverChain {
    fn label(&self) -> &'static str {
        CHAIN_LABEL
    }

    fn before_handle(&self, ctx: &HandlerObserverContext<'_>) -> ObserverReport {
        let mut merged = ObserverReport::empty();
        for observer in &self.0 {
            if observer.determinism().should_run(ctx.scope) {
                merged
                    .diagnostics
                    .extend(observer.before_handle(ctx).diagnostics);
            }
        }
        merged
    }

    fn after_handle(
        &self,
        ctx: &HandlerObserverContext<'_>,
        outputs: &mut [ChainEvent],
    ) -> ObserverReport {
        let mut merged = ObserverReport::empty();
        for observer in &self.0 {
            if observer.determinism().should_run(ctx.scope) {
                merged
                    .diagnostics
                    .extend(observer.after_handle(ctx, outputs).diagnostics);
            }
        }
        merged
    }
}

struct StatefulObserverChain(Vec<Arc<dyn StatefulObserver>>);

impl StatefulObserver for StatefulObserverChain {
    fn label(&self) -> &'static str {
        CHAIN_LABEL
    }

    fn before_state_accumulate(&self, ctx: &StatefulObserverContext<'_>) -> ObserverReport {
        let mut merged = ObserverReport::empty();
        for observer in &self.0 {
            if observer.determinism().should_run(ctx.scope) {
                merged
                    .diagnostics
                    .extend(observer.before_state_accumulate(ctx).diagnostics);
            }
        }
        merged
    }

    fn after_state_accumulate(&self, ctx: &StatefulObserverContext<'_>) -> ObserverReport {
        let mut merged = ObserverReport::empty();
        for observer in &self.0 {
            if observer.determinism().should_run(ctx.scope) {
                merged
                    .diagnostics
                    .extend(observer.after_state_accumulate(ctx).diagnostics);
            }
        }
        merged
    }

    fn after_state_emit(
        &self,
        ctx: &StatefulObserverContext<'_>,
        outputs: &mut [ChainEvent],
    ) -> ObserverReport {
        let mut merged = ObserverReport::empty();
        for observer in &self.0 {
            if observer.determinism().should_run(ctx.scope) {
                merged
                    .diagnostics
                    .extend(observer.after_state_emit(ctx, outputs).diagnostics);
            }
        }
        merged
    }
}

struct JoinObserverChain(Vec<Arc<dyn JoinObserver>>);

impl JoinObserver for JoinObserverChain {
    fn label(&self) -> &'static str {
        CHAIN_LABEL
    }

    fn before_join_input(&self, ctx: &JoinObserverContext<'_>) -> ObserverReport {
        let mut merged = ObserverReport::empty();
        for observer in &self.0 {
            if observer.determinism().should_run(ctx.scope) {
                merged
                    .diagnostics
                    .extend(observer.before_join_input(ctx).diagnostics);
            }
        }
        merged
    }

    fn after_join_output(
        &self,
        ctx: &JoinObserverContext<'_>,
        outputs: &mut [ChainEvent],
    ) -> ObserverReport {
        let mut merged = ObserverReport::empty();
        for observer in &self.0 {
            if observer.determinism().should_run(ctx.scope) {
                merged
                    .diagnostics
                    .extend(observer.after_join_output(ctx, outputs).diagnostics);
            }
        }
        merged
    }
}

struct SourcePollObserverChain(Vec<Arc<dyn SourcePollObserver>>);

impl SourcePollObserver for SourcePollObserverChain {
    fn label(&self) -> &'static str {
        CHAIN_LABEL
    }

    fn after_source_poll(
        &self,
        ctx: &SourcePollObserverContext<'_>,
        outputs: &mut [ChainEvent],
    ) -> ObserverReport {
        let mut merged = ObserverReport::empty();
        for observer in &self.0 {
            if observer.determinism().should_run(ctx.scope) {
                merged
                    .diagnostics
                    .extend(observer.after_source_poll(ctx, outputs).diagnostics);
            }
        }
        merged
    }
}

struct EffectObserverChain(Vec<Arc<dyn EffectObserver>>);

impl EffectObserver for EffectObserverChain {
    fn label(&self) -> &'static str {
        CHAIN_LABEL
    }

    fn after_effect(&self, ctx: &EffectObserverContext<'_>) -> ObserverReport {
        let mut merged = ObserverReport::empty();
        for observer in &self.0 {
            if observer.determinism().should_run(ctx.scope) {
                merged
                    .diagnostics
                    .extend(observer.after_effect(ctx).diagnostics);
            }
        }
        merged
    }
}

struct SinkDeliveryObserverChain(Vec<Arc<dyn SinkDeliveryObserver>>);

impl SinkDeliveryObserver for SinkDeliveryObserverChain {
    fn label(&self) -> &'static str {
        CHAIN_LABEL
    }

    fn after_sink_delivery(&self, ctx: &SinkDeliveryObserverContext<'_>) -> ObserverReport {
        let mut merged = ObserverReport::empty();
        for observer in &self.0 {
            if observer.determinism().should_run(ctx.scope) {
                merged
                    .diagnostics
                    .extend(observer.after_sink_delivery(ctx).diagnostics);
            }
        }
        merged
    }
}

struct IngressObserverChain(Vec<Arc<dyn IngressObserver>>);

impl IngressObserver for IngressObserverChain {
    fn label(&self) -> &'static str {
        CHAIN_LABEL
    }

    fn after_ingress(&self, ctx: &IngressObserverContext<'_>) -> ObserverReport {
        // Ingress observation runs at the listener boundary, which replay never
        // enters, so there is no handler-shell determinism gate here.
        let mut merged = ObserverReport::empty();
        for observer in &self.0 {
            merged
                .diagnostics
                .extend(observer.after_ingress(ctx).diagnostics);
        }
        merged
    }
}

struct OutputCommitObserverChain(Vec<Arc<dyn OutputCommitObserver>>);

impl OutputCommitObserver for OutputCommitObserverChain {
    fn label(&self) -> &'static str {
        CHAIN_LABEL
    }

    fn before_output_commit(
        &self,
        ctx: &OutputCommitObserverContext<'_>,
        event: &mut ChainEvent,
    ) -> ObserverCommitResult {
        let mut merged = ObserverReport::empty();
        for observer in &self.0 {
            if observer.determinism().should_run(ctx.scope) {
                let report = observer.before_output_commit(ctx, event)?;
                merged.diagnostics.extend(report.diagnostics);
            }
        }
        Ok(merged)
    }
}

struct StageLifecycleObserverChain(Vec<Arc<dyn StageLifecycleObserver>>);

impl StageLifecycleObserver for StageLifecycleObserverChain {
    fn label(&self) -> &'static str {
        CHAIN_LABEL
    }

    fn on_stage_lifecycle(&self, ctx: &StageLifecycleObserverContext<'_>) -> ObserverReport {
        let mut merged = ObserverReport::empty();
        for observer in &self.0 {
            if observer.determinism().should_run(ctx.scope) {
                merged
                    .diagnostics
                    .extend(observer.on_stage_lifecycle(ctx).diagnostics);
            }
        }
        merged
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::context::{FlowContext, MiddlewareExecutionScope, StageType};
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::{StageId, WriterId};
    use obzenflow_runtime::ObserverDeterminism;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A handler observer that counts its invocations, so a test can prove the
    /// composed port gated it (or not) without constructing diagnostic events.
    struct CountingHandlerObserver {
        calls: AtomicUsize,
        determinism: ObserverDeterminism,
    }

    impl CountingHandlerObserver {
        fn new(determinism: ObserverDeterminism) -> Arc<Self> {
            Arc::new(Self {
                calls: AtomicUsize::new(0),
                determinism,
            })
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl HandlerObserver for CountingHandlerObserver {
        fn label(&self) -> &'static str {
            "counting"
        }

        fn determinism(&self) -> ObserverDeterminism {
            self.determinism
        }

        fn before_handle(&self, _ctx: &HandlerObserverContext<'_>) -> ObserverReport {
            self.calls.fetch_add(1, Ordering::SeqCst);
            ObserverReport::empty()
        }
    }

    fn invoke(port: &Arc<dyn HandlerObserver>, scope: MiddlewareExecutionScope) {
        let flow_context = FlowContext {
            flow_name: "f".to_string(),
            flow_id: "f1".to_string(),
            stage_name: "s".to_string(),
            stage_id: StageId::new(),
            stage_type: StageType::Transform,
        };
        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test.event",
            serde_json::json!({}),
        );
        let ctx = HandlerObserverContext {
            stage_id: flow_context.stage_id,
            stage_name: &flow_context.stage_name,
            flow_context: &flow_context,
            scope,
            input: &event,
            stage_input_position: Some(1),
        };
        port.before_handle(&ctx);
    }

    #[test]
    fn live_only_observer_is_suppressed_under_replay_by_the_composite() {
        let observer = CountingHandlerObserver::new(ObserverDeterminism::LiveOnly);
        let mut set = StageObserverSet::default();
        set.handler.push(observer.clone());
        let port = set.build().handler.expect("handler port composed");

        invoke(&port, MiddlewareExecutionScope::LiveHandler);
        assert_eq!(observer.calls(), 1, "live-only observer runs live");

        invoke(&port, MiddlewareExecutionScope::StrictReplayHandler);
        invoke(&port, MiddlewareExecutionScope::ResumeHandler);
        assert_eq!(
            observer.calls(),
            1,
            "live-only observer is suppressed under strict replay and resume"
        );
    }

    #[test]
    fn deterministic_observer_runs_under_replay() {
        let observer = CountingHandlerObserver::new(ObserverDeterminism::Deterministic);
        let mut set = StageObserverSet::default();
        set.handler.push(observer.clone());
        let port = set.build().handler.expect("handler port composed");

        invoke(&port, MiddlewareExecutionScope::StrictReplayHandler);
        assert_eq!(
            observer.calls(),
            1,
            "deterministic observer runs under replay"
        );
    }

    #[test]
    fn composite_invokes_every_child() {
        let a = CountingHandlerObserver::new(ObserverDeterminism::Deterministic);
        let b = CountingHandlerObserver::new(ObserverDeterminism::Deterministic);
        let mut set = StageObserverSet::default();
        set.handler.push(a.clone());
        set.handler.push(b.clone());
        let port = set.build().handler.expect("handler port composed");

        invoke(&port, MiddlewareExecutionScope::LiveHandler);
        assert_eq!(a.calls(), 1);
        assert_eq!(b.calls(), 1);
    }

    #[test]
    fn empty_surface_composes_to_none() {
        let bundle = StageObserverSet::default().build();
        assert!(bundle.handler.is_none());
        assert!(bundle.is_empty());
    }
}
