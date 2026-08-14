// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Hardened composition for ordinary observer ports.

use std::fmt;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use obzenflow_core::ChainEvent;

use super::ports::{
    EffectObserver, EffectObserverContext, HandlerObserver, HandlerObserverContext, JoinObserver,
    JoinObserverContext, SinkDeliveryObserver, SinkDeliveryObserverContext, SourcePollObserver,
    SourcePollObserverContext, StageLifecycleObserver, StageLifecycleObserverContext,
    StatefulObserver, StatefulObserverContext,
};

struct ObserverChild<T: ?Sized> {
    label: &'static str,
    observer: Arc<T>,
    quarantined: AtomicBool,
}

impl<T: ?Sized> ObserverChild<T> {
    fn new(label: &'static str, observer: Arc<T>) -> Self {
        Self {
            label,
            observer,
            quarantined: AtomicBool::new(false),
        }
    }

    fn invoke<F>(&self, stage: &str, surface: &'static str, phase: &'static str, invoke: F)
    where
        F: FnOnce(&T),
    {
        if self.quarantined.load(Ordering::Acquire) {
            return;
        }

        if catch_unwind(AssertUnwindSafe(|| invoke(self.observer.as_ref()))).is_err()
            && self
                .quarantined
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            warn_quarantined(self.label, stage, surface, phase);
        }
    }
}

pub(crate) struct HardenedObserverPort<T: ?Sized> {
    observer: Arc<T>,
    quarantined: Arc<AtomicBool>,
}

impl<T: ?Sized> Clone for HardenedObserverPort<T> {
    fn clone(&self) -> Self {
        Self {
            observer: Arc::clone(&self.observer),
            quarantined: Arc::clone(&self.quarantined),
        }
    }
}

impl<T: ?Sized> HardenedObserverPort<T> {
    fn new(observer: Arc<T>) -> Self {
        Self {
            observer,
            quarantined: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn invoke<F>(
        &self,
        stage: &str,
        surface: &'static str,
        phase: &'static str,
        invoke: F,
    ) where
        F: FnOnce(&T),
    {
        if self.quarantined.load(Ordering::Acquire) {
            return;
        }

        if catch_unwind(AssertUnwindSafe(|| invoke(self.observer.as_ref()))).is_err()
            && self
                .quarantined
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            warn_quarantined("observer-composite", stage, surface, phase);
        }
    }
}

fn warn_quarantined(label: &'static str, stage: &str, surface: &'static str, phase: &'static str) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        tracing::warn!(
            observer = label,
            stage,
            surface,
            phase,
            "observer panicked and was quarantined for the remainder of the stage run"
        );
    }));
}

struct HandlerObserverChain(Vec<ObserverChild<dyn HandlerObserver>>);

impl HandlerObserver for HandlerObserverChain {
    fn before_handle(&self, ctx: &HandlerObserverContext<'_>) {
        for child in &self.0 {
            child.invoke(ctx.stage_name(), "handler", "before_handle", |observer| {
                observer.before_handle(ctx);
            });
        }
    }

    fn after_handle(&self, ctx: &HandlerObserverContext<'_>, outputs: &[ChainEvent]) {
        for child in &self.0 {
            child.invoke(ctx.stage_name(), "handler", "after_handle", |observer| {
                observer.after_handle(ctx, outputs);
            });
        }
    }
}

struct StatefulObserverChain(Vec<ObserverChild<dyn StatefulObserver>>);

impl StatefulObserver for StatefulObserverChain {
    fn before_state_accumulate(&self, ctx: &StatefulObserverContext<'_>) {
        for child in &self.0 {
            child.invoke(
                ctx.stage_name(),
                "stateful",
                "before_state_accumulate",
                |observer| observer.before_state_accumulate(ctx),
            );
        }
    }

    fn after_state_accumulate(&self, ctx: &StatefulObserverContext<'_>) {
        for child in &self.0 {
            child.invoke(
                ctx.stage_name(),
                "stateful",
                "after_state_accumulate",
                |observer| observer.after_state_accumulate(ctx),
            );
        }
    }

    fn after_state_emit(&self, ctx: &StatefulObserverContext<'_>, outputs: &[ChainEvent]) {
        for child in &self.0 {
            child.invoke(
                ctx.stage_name(),
                "stateful",
                "after_state_emit",
                |observer| observer.after_state_emit(ctx, outputs),
            );
        }
    }
}

struct JoinObserverChain(Vec<ObserverChild<dyn JoinObserver>>);

impl JoinObserver for JoinObserverChain {
    fn before_join_input(&self, ctx: &JoinObserverContext<'_>) {
        for child in &self.0 {
            child.invoke(ctx.stage_name(), "join", "before_join_input", |observer| {
                observer.before_join_input(ctx)
            });
        }
    }

    fn after_join_output(&self, ctx: &JoinObserverContext<'_>, outputs: &[ChainEvent]) {
        for child in &self.0 {
            child.invoke(ctx.stage_name(), "join", "after_join_output", |observer| {
                observer.after_join_output(ctx, outputs)
            });
        }
    }
}

struct SourcePollObserverChain(Vec<ObserverChild<dyn SourcePollObserver>>);

impl SourcePollObserver for SourcePollObserverChain {
    fn after_source_poll(&self, ctx: &SourcePollObserverContext<'_>, outputs: &[ChainEvent]) {
        for child in &self.0 {
            child.invoke(
                ctx.stage_name(),
                "source_poll",
                "after_source_poll",
                |observer| observer.after_source_poll(ctx, outputs),
            );
        }
    }
}

struct EffectObserverChain(Vec<ObserverChild<dyn EffectObserver>>);

impl EffectObserver for EffectObserverChain {
    fn after_effect(&self, ctx: &EffectObserverContext<'_>) {
        for child in &self.0 {
            child.invoke(ctx.stage_name(), "effect", "after_effect", |observer| {
                observer.after_effect(ctx);
            });
        }
    }
}

struct SinkDeliveryObserverChain(Vec<ObserverChild<dyn SinkDeliveryObserver>>);

impl SinkDeliveryObserver for SinkDeliveryObserverChain {
    fn after_sink_delivery(&self, ctx: &SinkDeliveryObserverContext<'_>) {
        for child in &self.0 {
            child.invoke(
                ctx.stage_name(),
                "sink_delivery",
                "after_sink_delivery",
                |observer| observer.after_sink_delivery(ctx),
            );
        }
    }
}

struct StageLifecycleObserverChain(Vec<ObserverChild<dyn StageLifecycleObserver>>);

impl StageLifecycleObserver for StageLifecycleObserverChain {
    fn on_stage_lifecycle(&self, ctx: &StageLifecycleObserverContext<'_>) {
        for child in &self.0 {
            child.invoke(
                ctx.stage_name(),
                "stage_lifecycle",
                "on_stage_lifecycle",
                |observer| observer.on_stage_lifecycle(ctx),
            );
        }
    }
}

/// Safe, ordered input to the runtime-owned observer compositor.
///
/// The adapter binder supplies one declaration label with every child. The
/// fields stay private so callers cannot install a pre-composed port that
/// bypasses per-child quarantine.
#[doc(hidden)]
#[derive(Default)]
pub struct StageObserverBundleBuilder {
    handler: Vec<ObserverChild<dyn HandlerObserver>>,
    stateful: Vec<ObserverChild<dyn StatefulObserver>>,
    join: Vec<ObserverChild<dyn JoinObserver>>,
    source_poll: Vec<ObserverChild<dyn SourcePollObserver>>,
    effect: Vec<ObserverChild<dyn EffectObserver>>,
    sink_delivery: Vec<ObserverChild<dyn SinkDeliveryObserver>>,
    stage_lifecycle: Vec<ObserverChild<dyn StageLifecycleObserver>>,
}

impl StageObserverBundleBuilder {
    pub fn push_handler(&mut self, label: &'static str, observer: Arc<dyn HandlerObserver>) {
        self.handler.push(ObserverChild::new(label, observer));
    }

    pub fn push_stateful(&mut self, label: &'static str, observer: Arc<dyn StatefulObserver>) {
        self.stateful.push(ObserverChild::new(label, observer));
    }

    pub fn push_join(&mut self, label: &'static str, observer: Arc<dyn JoinObserver>) {
        self.join.push(ObserverChild::new(label, observer));
    }

    pub fn push_source_poll(&mut self, label: &'static str, observer: Arc<dyn SourcePollObserver>) {
        self.source_poll.push(ObserverChild::new(label, observer));
    }

    pub fn push_effect(&mut self, label: &'static str, observer: Arc<dyn EffectObserver>) {
        self.effect.push(ObserverChild::new(label, observer));
    }

    pub fn push_sink_delivery(
        &mut self,
        label: &'static str,
        observer: Arc<dyn SinkDeliveryObserver>,
    ) {
        self.sink_delivery.push(ObserverChild::new(label, observer));
    }

    pub fn push_stage_lifecycle(
        &mut self,
        label: &'static str,
        observer: Arc<dyn StageLifecycleObserver>,
    ) {
        self.stage_lifecycle
            .push(ObserverChild::new(label, observer));
    }

    pub fn extend(&mut self, other: Self) {
        self.handler.extend(other.handler);
        self.stateful.extend(other.stateful);
        self.join.extend(other.join);
        self.source_poll.extend(other.source_poll);
        self.effect.extend(other.effect);
        self.sink_delivery.extend(other.sink_delivery);
        self.stage_lifecycle.extend(other.stage_lifecycle);
    }

    pub fn is_empty(&self) -> bool {
        self.handler.is_empty()
            && self.stateful.is_empty()
            && self.join.is_empty()
            && self.source_poll.is_empty()
            && self.effect.is_empty()
            && self.sink_delivery.is_empty()
            && self.stage_lifecycle.is_empty()
    }

    pub fn build(self) -> StageObserverBundle {
        StageObserverBundle {
            handler: compose(self.handler, HandlerObserverChain),
            stateful: compose(self.stateful, StatefulObserverChain),
            join: compose(self.join, JoinObserverChain),
            source_poll: compose(self.source_poll, SourcePollObserverChain),
            effect: compose(self.effect, EffectObserverChain),
            sink_delivery: compose(self.sink_delivery, SinkDeliveryObserverChain),
            stage_lifecycle: compose(self.stage_lifecycle, StageLifecycleObserverChain),
        }
    }
}

fn compose<T: ?Sized, C>(
    children: Vec<ObserverChild<T>>,
    chain: fn(Vec<ObserverChild<T>>) -> C,
) -> Option<HardenedObserverPort<T>>
where
    C: IntoObserverArc<T>,
{
    if children.is_empty() {
        None
    } else {
        Some(HardenedObserverPort::new(
            chain(children).into_observer_arc(),
        ))
    }
}

trait IntoObserverArc<T: ?Sized> {
    fn into_observer_arc(self) -> Arc<T>;
}

macro_rules! impl_into_observer_arc {
    ($chain:ty, $port:ty) => {
        impl IntoObserverArc<$port> for $chain {
            fn into_observer_arc(self) -> Arc<$port> {
                Arc::new(self)
            }
        }
    };
}

impl_into_observer_arc!(HandlerObserverChain, dyn HandlerObserver);
impl_into_observer_arc!(StatefulObserverChain, dyn StatefulObserver);
impl_into_observer_arc!(JoinObserverChain, dyn JoinObserver);
impl_into_observer_arc!(SourcePollObserverChain, dyn SourcePollObserver);
impl_into_observer_arc!(EffectObserverChain, dyn EffectObserver);
impl_into_observer_arc!(SinkDeliveryObserverChain, dyn SinkDeliveryObserver);
impl_into_observer_arc!(StageLifecycleObserverChain, dyn StageLifecycleObserver);

/// Opaque, per-stage composed observer ports.
///
/// Only the empty/default value is directly constructible. Non-empty values
/// come from [`StageObserverBundleBuilder`], which always applies labelled
/// per-child quarantine.
#[derive(Clone, Default)]
pub struct StageObserverBundle {
    handler: Option<HardenedObserverPort<dyn HandlerObserver>>,
    stateful: Option<HardenedObserverPort<dyn StatefulObserver>>,
    join: Option<HardenedObserverPort<dyn JoinObserver>>,
    source_poll: Option<HardenedObserverPort<dyn SourcePollObserver>>,
    effect: Option<HardenedObserverPort<dyn EffectObserver>>,
    sink_delivery: Option<HardenedObserverPort<dyn SinkDeliveryObserver>>,
    stage_lifecycle: Option<HardenedObserverPort<dyn StageLifecycleObserver>>,
}

impl StageObserverBundle {
    pub fn is_empty(&self) -> bool {
        self.handler.is_none()
            && self.stateful.is_none()
            && self.join.is_none()
            && self.source_poll.is_none()
            && self.effect.is_none()
            && self.sink_delivery.is_none()
            && self.stage_lifecycle.is_none()
    }

    pub(crate) fn has_join(&self) -> bool {
        self.join.is_some()
    }

    pub(crate) fn has_source_poll(&self) -> bool {
        self.source_poll.is_some()
    }

    pub(crate) fn has_sink_delivery(&self) -> bool {
        self.sink_delivery.is_some()
    }

    pub(crate) fn handler(&self) -> Option<&HardenedObserverPort<dyn HandlerObserver>> {
        self.handler.as_ref()
    }

    pub(crate) fn stateful(&self) -> Option<&HardenedObserverPort<dyn StatefulObserver>> {
        self.stateful.as_ref()
    }

    pub(crate) fn join(&self) -> Option<&HardenedObserverPort<dyn JoinObserver>> {
        self.join.as_ref()
    }

    pub(crate) fn source_poll(&self) -> Option<&HardenedObserverPort<dyn SourcePollObserver>> {
        self.source_poll.as_ref()
    }

    pub(crate) fn effect(&self) -> Option<&HardenedObserverPort<dyn EffectObserver>> {
        self.effect.as_ref()
    }

    pub(crate) fn sink_delivery(&self) -> Option<&HardenedObserverPort<dyn SinkDeliveryObserver>> {
        self.sink_delivery.as_ref()
    }

    pub(crate) fn stage_lifecycle(
        &self,
    ) -> Option<&HardenedObserverPort<dyn StageLifecycleObserver>> {
        self.stage_lifecycle.as_ref()
    }
}

impl fmt::Debug for StageObserverBundle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StageObserverBundle")
            .field("has_observers", &!self.is_empty())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::context::{FlowContext, StageType};
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::{StageId, WriterId};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Barrier, Mutex};
    use tracing::span::{Attributes, Id, Record};
    use tracing::{Event, Metadata, Subscriber};

    struct PanicsOnce {
        calls: AtomicUsize,
    }

    impl HandlerObserver for PanicsOnce {
        fn before_handle(&self, _ctx: &HandlerObserverContext<'_>) {
            self.calls.fetch_add(1, Ordering::SeqCst);
            panic!("observer panic");
        }
    }

    struct Counts(AtomicUsize);

    impl HandlerObserver for Counts {
        fn before_handle(&self, _ctx: &HandlerObserverContext<'_>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct Records {
        id: usize,
        calls: Arc<Mutex<Vec<usize>>>,
    }

    struct PanickingWarningSubscriber {
        warnings: Arc<AtomicUsize>,
    }

    struct CountingWarningSubscriber {
        warnings: Arc<AtomicUsize>,
    }

    impl Subscriber for PanickingWarningSubscriber {
        fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _span: &Attributes<'_>) -> Id {
            Id::from_u64(1)
        }

        fn record(&self, _span: &Id, _values: &Record<'_>) {}

        fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

        fn event(&self, _event: &Event<'_>) {
            self.warnings.fetch_add(1, Ordering::SeqCst);
            panic!("warning subscriber panic");
        }

        fn enter(&self, _span: &Id) {}

        fn exit(&self, _span: &Id) {}
    }

    impl Subscriber for CountingWarningSubscriber {
        fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _span: &Attributes<'_>) -> Id {
            Id::from_u64(1)
        }

        fn record(&self, _span: &Id, _values: &Record<'_>) {}

        fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

        fn event(&self, _event: &Event<'_>) {
            self.warnings.fetch_add(1, Ordering::SeqCst);
        }

        fn enter(&self, _span: &Id) {}

        fn exit(&self, _span: &Id) {}
    }

    impl HandlerObserver for Records {
        fn before_handle(&self, _ctx: &HandlerObserverContext<'_>) {
            self.calls
                .lock()
                .expect("recording observer lock")
                .push(self.id);
        }
    }

    fn context() -> (FlowContext, ChainEvent) {
        let flow_context = FlowContext {
            flow_name: "flow".to_string(),
            flow_id: "run".to_string(),
            stage_name: "stage".to_string(),
            stage_id: StageId::new(),
            stage_type: StageType::Transform,
        };
        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test.event",
            serde_json::json!({}),
        );
        (flow_context, event)
    }

    #[test]
    fn panicking_child_is_quarantined_and_siblings_continue() {
        let panics = Arc::new(PanicsOnce {
            calls: AtomicUsize::new(0),
        });
        let counts = Arc::new(Counts(AtomicUsize::new(0)));
        let mut builder = StageObserverBundleBuilder::default();
        builder.push_handler("panics", panics.clone());
        builder.push_handler("counts", counts.clone());
        let bundle = builder.build();
        let (flow_context, event) = context();
        let ctx = HandlerObserverContext::new(&flow_context, &event, Some(1));

        for _ in 0..3 {
            bundle.handler().expect("handler port").invoke(
                "stage",
                "handler",
                "before_handle",
                |observer| {
                    observer.before_handle(&ctx);
                },
            );
        }

        assert_eq!(panics.calls.load(Ordering::SeqCst), 1);
        assert_eq!(counts.0.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn composed_port_preserves_declaration_order() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut builder = StageObserverBundleBuilder::default();
        for id in [1, 2, 3] {
            builder.push_handler(
                "ordered",
                Arc::new(Records {
                    id,
                    calls: calls.clone(),
                }),
            );
        }
        let bundle = builder.build();
        let (flow_context, event) = context();
        let ctx = HandlerObserverContext::new(&flow_context, &event, Some(1));

        bundle.handler().expect("one composed handler port").invoke(
            "stage",
            "handler",
            "before_handle",
            |observer| {
                observer.before_handle(&ctx);
            },
        );

        assert_eq!(*calls.lock().expect("recording observer lock"), [1, 2, 3]);
    }

    #[test]
    fn panicking_warning_subscriber_cannot_skip_siblings_or_repeat_warning() {
        let panics = Arc::new(PanicsOnce {
            calls: AtomicUsize::new(0),
        });
        let counts = Arc::new(Counts(AtomicUsize::new(0)));
        let warnings = Arc::new(AtomicUsize::new(0));
        let mut builder = StageObserverBundleBuilder::default();
        builder.push_handler("panics", panics.clone());
        builder.push_handler("counts", counts.clone());
        let bundle = builder.build();
        let (flow_context, event) = context();
        let ctx = HandlerObserverContext::new(&flow_context, &event, Some(1));

        tracing::subscriber::with_default(
            PanickingWarningSubscriber {
                warnings: warnings.clone(),
            },
            || {
                for _ in 0..2 {
                    bundle.handler().expect("handler port").invoke(
                        "stage",
                        "handler",
                        "before_handle",
                        |observer| {
                            observer.before_handle(&ctx);
                        },
                    );
                }
            },
        );

        assert_eq!(panics.calls.load(Ordering::SeqCst), 1);
        assert_eq!(warnings.load(Ordering::SeqCst), 1);
        assert_eq!(counts.0.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn concurrent_panics_make_one_quarantine_transition_and_later_calls_skip_child() {
        const WORKERS: usize = 8;

        let panics = Arc::new(PanicsOnce {
            calls: AtomicUsize::new(0),
        });
        let counts = Arc::new(Counts(AtomicUsize::new(0)));
        let warnings = Arc::new(AtomicUsize::new(0));
        let mut builder = StageObserverBundleBuilder::default();
        builder.push_handler("panics", panics.clone());
        builder.push_handler("counts", counts.clone());
        let bundle = Arc::new(builder.build());
        let (flow_context, event) = context();
        let flow_context = Arc::new(flow_context);
        let event = Arc::new(event);
        let barrier = Arc::new(Barrier::new(WORKERS));
        let dispatch = tracing::Dispatch::new(CountingWarningSubscriber {
            warnings: warnings.clone(),
        });

        let handles = (0..WORKERS)
            .map(|_| {
                let bundle = bundle.clone();
                let flow_context = flow_context.clone();
                let event = event.clone();
                let barrier = barrier.clone();
                let dispatch = dispatch.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    tracing::dispatcher::with_default(&dispatch, || {
                        let ctx = HandlerObserverContext::new(&flow_context, &event, Some(1));
                        bundle.handler().expect("handler port").invoke(
                            "stage",
                            "handler",
                            "before_handle",
                            |observer| observer.before_handle(&ctx),
                        );
                    });
                })
            })
            .collect::<Vec<_>>();

        for handle in handles {
            handle.join().expect("observer dispatch thread");
        }

        let calls_after_race = panics.calls.load(Ordering::SeqCst);
        assert!((1..=WORKERS).contains(&calls_after_race));
        assert_eq!(warnings.load(Ordering::SeqCst), 1);
        assert_eq!(counts.0.load(Ordering::SeqCst), WORKERS);

        tracing::dispatcher::with_default(&dispatch, || {
            let ctx = HandlerObserverContext::new(&flow_context, &event, Some(2));
            bundle.handler().expect("handler port").invoke(
                "stage",
                "handler",
                "before_handle",
                |observer| observer.before_handle(&ctx),
            );
        });

        assert_eq!(panics.calls.load(Ordering::SeqCst), calls_after_race);
        assert_eq!(warnings.load(Ordering::SeqCst), 1);
        assert_eq!(counts.0.load(Ordering::SeqCst), WORKERS + 1);
    }
}
