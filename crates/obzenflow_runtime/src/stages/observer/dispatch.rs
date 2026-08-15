// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Runtime-owned ordinary observer occurrence dispatch.
//!
//! Dispatch is synchronous, live-only, and consumes no observer-produced
//! value. Each helper checks the relevant composed port before constructing a
//! context, preserving the empty-bundle fast path.

use obzenflow_core::event::context::{FlowContext, MiddlewareExecutionScope};
use obzenflow_core::{ChainEvent, FlowId, StageId};

use super::{
    EffectObserverContext, EffectObserverOutcome, HandlerObserverContext, JoinObserverContext,
    SinkDeliveryObserverContext, SinkDeliveryObserverOutcome, SourcePollObserverContext,
    StageInputPosition, StageLifecycleObserverContext, StageLifecyclePhase, StageObserverBundle,
    StatefulObserverContext,
};

fn is_live(scope: MiddlewareExecutionScope) -> bool {
    !scope.is_deterministic_replay()
}

pub(crate) fn run_before_handler_observers(
    observers: &StageObserverBundle,
    flow_id: FlowId,
    flow_context: &FlowContext,
    scope: MiddlewareExecutionScope,
    input: &ChainEvent,
    stage_input_position: StageInputPosition,
) {
    let Some(observer) = observers.handler().filter(|_| is_live(scope)) else {
        return;
    };
    let ctx = HandlerObserverContext::new(flow_id, flow_context, input, stage_input_position);
    observer.invoke(ctx.stage_name(), "handler", "before_handle", |port| {
        port.before_handle(&ctx)
    });
}

pub(crate) fn run_after_handler_observers(
    observers: &StageObserverBundle,
    flow_id: FlowId,
    flow_context: &FlowContext,
    scope: MiddlewareExecutionScope,
    input: &ChainEvent,
    stage_input_position: StageInputPosition,
    outputs: &[ChainEvent],
) {
    let Some(observer) = observers.handler().filter(|_| is_live(scope)) else {
        return;
    };
    let ctx = HandlerObserverContext::new(flow_id, flow_context, input, stage_input_position);
    observer.invoke(ctx.stage_name(), "handler", "after_handle", |port| {
        port.after_handle(&ctx, outputs);
    });
}

pub(crate) fn run_stateful_after_emit_observers(
    observers: &StageObserverBundle,
    scope: MiddlewareExecutionScope,
    ctx: &StatefulObserverContext<'_>,
    outputs: &[ChainEvent],
) {
    let Some(observer) = observers.stateful().filter(|_| is_live(scope)) else {
        return;
    };
    observer.invoke(ctx.stage_name(), "stateful", "after_state_emit", |port| {
        port.after_state_emit(ctx, outputs)
    });
}

pub(crate) fn run_stateful_before_accumulate_observers(
    observers: &StageObserverBundle,
    scope: MiddlewareExecutionScope,
    ctx: &StatefulObserverContext<'_>,
) {
    let Some(observer) = observers.stateful().filter(|_| is_live(scope)) else {
        return;
    };
    observer.invoke(
        ctx.stage_name(),
        "stateful",
        "before_state_accumulate",
        |port| port.before_state_accumulate(ctx),
    );
}

pub(crate) fn run_stateful_after_accumulate_observers(
    observers: &StageObserverBundle,
    scope: MiddlewareExecutionScope,
    ctx: &StatefulObserverContext<'_>,
) {
    let Some(observer) = observers.stateful().filter(|_| is_live(scope)) else {
        return;
    };
    observer.invoke(
        ctx.stage_name(),
        "stateful",
        "after_state_accumulate",
        |port| port.after_state_accumulate(ctx),
    );
}

pub(crate) fn run_source_poll_observers(
    observers: &StageObserverBundle,
    scope: MiddlewareExecutionScope,
    ctx: &SourcePollObserverContext<'_>,
    outputs: &[ChainEvent],
) {
    let Some(observer) = observers.source_poll().filter(|_| is_live(scope)) else {
        return;
    };
    observer.invoke(
        ctx.stage_name(),
        "source_poll",
        "after_source_poll",
        |port| port.after_source_poll(ctx, outputs),
    );
}

pub(crate) fn run_sink_delivery_observers(
    observers: &StageObserverBundle,
    flow_id: FlowId,
    flow_context: &FlowContext,
    scope: MiddlewareExecutionScope,
    input: &ChainEvent,
    stage_input_position: StageInputPosition,
    outcome: SinkDeliveryObserverOutcome,
) {
    let Some(observer) = observers.sink_delivery().filter(|_| is_live(scope)) else {
        return;
    };
    let ctx = SinkDeliveryObserverContext::new(
        flow_id,
        flow_context,
        input,
        stage_input_position,
        outcome,
    );
    observer.invoke(
        ctx.stage_name(),
        "sink_delivery",
        "after_sink_delivery",
        |port| port.after_sink_delivery(&ctx),
    );
}

pub(crate) fn run_effect_observers(
    observers: &StageObserverBundle,
    flow_id: FlowId,
    stage_id: StageId,
    stage_name: &str,
    scope: MiddlewareExecutionScope,
    effect_type: &str,
    outcome: EffectObserverOutcome,
) {
    let Some(observer) = observers.effect().filter(|_| is_live(scope)) else {
        return;
    };
    let ctx = EffectObserverContext::new(flow_id, stage_id, stage_name, effect_type, outcome);
    observer.invoke(stage_name, "effect", "after_effect", |port| {
        port.after_effect(&ctx);
    });
}

pub(crate) fn run_stage_lifecycle_observers(
    observers: &StageObserverBundle,
    scope: MiddlewareExecutionScope,
    phase: StageLifecyclePhase,
    context: impl FnOnce() -> (FlowId, FlowContext),
) {
    let Some(observer) = observers.stage_lifecycle().filter(|_| is_live(scope)) else {
        return;
    };
    let (flow_id, flow_context) = context();
    let ctx = StageLifecycleObserverContext::new(flow_id, &flow_context, phase);
    observer.invoke(
        ctx.stage_name(),
        "stage_lifecycle",
        "on_stage_lifecycle",
        |port| port.on_stage_lifecycle(&ctx),
    );
}

pub(crate) fn run_join_after_output_observers(
    observers: &StageObserverBundle,
    scope: MiddlewareExecutionScope,
    ctx: &JoinObserverContext<'_>,
    outputs: &[ChainEvent],
) {
    let Some(observer) = observers.join().filter(|_| is_live(scope)) else {
        return;
    };
    observer.invoke(ctx.stage_name(), "join", "after_join_output", |port| {
        port.after_join_output(ctx, outputs);
    });
}

pub(crate) fn run_join_before_input_observers(
    observers: &StageObserverBundle,
    scope: MiddlewareExecutionScope,
    ctx: &JoinObserverContext<'_>,
) {
    let Some(observer) = observers.join().filter(|_| is_live(scope)) else {
        return;
    };
    observer.invoke(ctx.stage_name(), "join", "before_join_input", |port| {
        port.before_join_input(ctx);
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stages::observer::composition::StageObserverBundleBuilder;
    use crate::stages::observer::{
        HandlerObserver, HandlerObserverContext, StageLifecycleObserver,
        StageLifecycleObserverContext,
    };
    use obzenflow_core::event::context::StageType;
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::WriterId;
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    thread_local! {
        static THREAD_ALLOCATION_COUNT: Cell<Option<usize>> = const { Cell::new(None) };
    }

    struct CountingTestAllocator;

    #[global_allocator]
    static TEST_ALLOCATOR: CountingTestAllocator = CountingTestAllocator;

    fn record_test_allocation() {
        let _ = THREAD_ALLOCATION_COUNT.try_with(|counter| {
            if let Some(count) = counter.get() {
                counter.set(Some(count + 1));
            }
        });
    }

    // SAFETY: every operation delegates unchanged to the system allocator. The
    // wrapper only increments a thread-local counter before allocation calls.
    unsafe impl GlobalAlloc for CountingTestAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            record_test_allocation();
            unsafe { System.alloc(layout) }
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            record_test_allocation();
            unsafe { System.alloc_zeroed(layout) }
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            unsafe { System.dealloc(ptr, layout) }
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            record_test_allocation();
            unsafe { System.realloc(ptr, layout, new_size) }
        }
    }

    fn count_thread_allocations(run: impl FnOnce()) -> usize {
        THREAD_ALLOCATION_COUNT.with(|counter| {
            assert!(counter.get().is_none(), "allocation counter is not nested");
            counter.set(Some(0));
        });
        run();
        THREAD_ALLOCATION_COUNT.with(|counter| {
            let count = counter.get().expect("allocation counter is enabled");
            counter.set(None);
            count
        })
    }

    struct Counts(Arc<AtomicUsize>);

    impl HandlerObserver for Counts {
        fn before_handle(&self, _ctx: &HandlerObserverContext<'_>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl StageLifecycleObserver for Counts {
        fn on_stage_lifecycle(&self, _ctx: &StageLifecycleObserverContext<'_>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn reconstruction_scopes_dispatch_zero_callbacks_and_live_dispatches_once() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut builder = StageObserverBundleBuilder::default();
        builder.push_handler("counts", Arc::new(Counts(calls.clone())));
        let observers = builder.build();
        let flow_id = FlowId::new();
        let stage_id = StageId::new();
        let flow_context = FlowContext {
            flow_name: "flow".to_string(),
            flow_id: flow_id.to_string(),
            stage_name: "stage".to_string(),
            stage_id,
            stage_type: StageType::Transform,
        };
        let input = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test.input",
            serde_json::json!({}),
        );

        for scope in [
            MiddlewareExecutionScope::StrictReplayHandler,
            MiddlewareExecutionScope::ResumeHandler,
        ] {
            run_before_handler_observers(
                &observers,
                flow_id,
                &flow_context,
                scope,
                &input,
                StageInputPosition(1),
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        run_before_handler_observers(
            &observers,
            flow_id,
            &flow_context,
            MiddlewareExecutionScope::LiveHandler,
            &input,
            StageInputPosition(1),
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn lifecycle_context_is_not_constructed_without_a_live_observer() {
        let empty_observers = StageObserverBundleBuilder::default().build();
        let context_constructions = AtomicUsize::new(0);

        let empty_allocations = count_thread_allocations(|| {
            run_stage_lifecycle_observers(
                &empty_observers,
                MiddlewareExecutionScope::LiveHandler,
                StageLifecyclePhase::Running,
                || {
                    context_constructions.fetch_add(1, Ordering::SeqCst);
                    panic!("an empty bundle must not construct lifecycle context")
                },
            );
        });

        let calls = Arc::new(AtomicUsize::new(0));
        let mut builder = StageObserverBundleBuilder::default();
        builder.push_stage_lifecycle("counts", Arc::new(Counts(calls.clone())));
        let observers = builder.build();
        let replay_allocations = count_thread_allocations(|| {
            run_stage_lifecycle_observers(
                &observers,
                MiddlewareExecutionScope::StrictReplayHandler,
                StageLifecyclePhase::Running,
                || {
                    context_constructions.fetch_add(1, Ordering::SeqCst);
                    panic!("replay must not construct lifecycle context")
                },
            );
        });

        assert_eq!(context_constructions.load(Ordering::SeqCst), 0);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(empty_allocations, 0);
        assert_eq!(replay_allocations, 0);
    }
}
