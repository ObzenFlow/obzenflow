// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-120c phase 6 (test-hygiene cleanup): the effectful results path
//! builds its `pre_write` context with the stage execution scope, not the
//! `LiveHandler` default (the G6 regression), and the scope is a per-call
//! decision (H3).
//!
//! These live in the integration suite rather than as `obzenflow_adapters`
//! unit tests because the effectful case needs a full `EffectInvocationContext`,
//! whose `data_journal` is an `Arc<dyn Journal<ChainEvent>>`. The adapters
//! crate cannot depend on `obzenflow_infra` (correctly), so a unit test there
//! would have to fake the journal. Here we inject the real
//! `obzenflow_infra::journal::MemoryJournal`, the same pattern as
//! `middleware_factory_context_injection_test.rs`.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::json;

use obzenflow_adapters::middleware::{
    Middleware, MiddlewareContext, SourceMiddlewarePhase, UnifiedMiddlewareTransform,
};
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::EventEnvelope;
use obzenflow_core::journal::Journal;
use obzenflow_core::{
    FlowId, JournalOwner, JournalWriterId, MiddlewareExecutionScope, StageId, WriterId,
};
use obzenflow_infra::journal::MemoryJournal;
use obzenflow_runtime::backpressure::BackpressureWriter;
use obzenflow_runtime::effects::{EffectInvocationContext, EffectPortRegistry, EffectRuntimeMode};
use obzenflow_runtime::feed_plan::StageOutputContract;
use obzenflow_runtime::messaging::upstream_subscription::StageInputPosition;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::transform::traits::UnifiedTransformHandler;

/// Records the execution scope its `pre_write` hook observes.
struct ScopeProbe {
    seen: Arc<Mutex<Vec<MiddlewareExecutionScope>>>,
}

impl Middleware for ScopeProbe {
    fn label(&self) -> &'static str {
        "test.scope_probe"
    }

    fn source_phase(&self) -> SourceMiddlewarePhase {
        SourceMiddlewarePhase::Ordinary
    }

    fn pre_write(&self, _event: &mut ChainEvent, ctx: &MiddlewareContext) {
        self.seen
            .lock()
            .expect("seen lock poisoned")
            .push(ctx.execution_scope());
    }
}

#[derive(Clone, Debug)]
struct UnifiedEcho;

#[async_trait]
impl UnifiedTransformHandler for UnifiedEcho {
    async fn process(
        &self,
        event: ChainEvent,
        _effect_context: Option<EffectInvocationContext>,
        _scope: MiddlewareExecutionScope,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(vec![event])
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

/// Build an `EffectInvocationContext` backed by the real in-memory journal.
fn effect_context(journal: Arc<dyn Journal<ChainEvent>>) -> EffectInvocationContext {
    let stage_id = StageId::new();
    let writer_id = WriterId::from(stage_id);
    let parent = EventEnvelope::new(
        JournalWriterId::from(*journal.id()),
        ChainEventFactory::data_event(writer_id, "test.parent", json!({})),
    );
    EffectInvocationContext {
        flow_id: FlowId::new(),
        stage_id,
        stage_key: "effect_stage".to_string(),
        writer_id,
        input_seq: StageInputPosition(1),
        stage_logic_version: "test-v1".to_string(),
        data_journal: journal,
        flow_context: None,
        system_journal: None,
        instrumentation: None,
        heartbeat_state: None,
        parent,
        effect_history: None,
        effect_runtime_mode: EffectRuntimeMode::ReplayStrict,
        effect_ports: EffectPortRegistry::new(),
        effect_declarations: Vec::new(),
        synthesized_outcomes: Vec::new(),
        output_contract: StageOutputContract::empty(),
        backpressure_writer: BackpressureWriter::disabled(),
        emit_enabled: false,
        effect_boundary: None,
        boundary_control_events: Arc::new(Mutex::new(Vec::new())),
    }
}

/// FLOWIP-120c H3: the scope is a per-call decision, so consecutive events
/// through one wrapper can carry different scopes. This is the property
/// FLOWIP-120n's resume phase predicate relies on.
#[tokio::test]
async fn consecutive_events_carry_independent_scopes() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let wrapped = UnifiedMiddlewareTransform::new(UnifiedEcho)
        .with_middleware(Box::new(ScopeProbe { seen: seen.clone() }));

    let make_event =
        || ChainEventFactory::data_event(WriterId::from(StageId::new()), "test", json!({}));

    wrapped
        .process(
            make_event(),
            None,
            MiddlewareExecutionScope::StrictReplayHandler,
        )
        .await
        .expect("echo handler should not fail");
    wrapped
        .process(make_event(), None, MiddlewareExecutionScope::LiveHandler)
        .await
        .expect("echo handler should not fail");

    let scopes = seen.lock().expect("seen lock poisoned").clone();
    assert_eq!(
        scopes,
        vec![
            MiddlewareExecutionScope::StrictReplayHandler,
            MiddlewareExecutionScope::LiveHandler,
        ],
        "each dispatch scopes its context independently"
    );
}

/// FLOWIP-120c G6: the effectful results path must build its `pre_write`
/// context with the stage execution scope, not the `LiveHandler` default.
#[tokio::test]
async fn effectful_results_pre_write_carries_stage_scope() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let wrapped = UnifiedMiddlewareTransform::new(UnifiedEcho)
        .with_middleware(Box::new(ScopeProbe { seen: seen.clone() }));

    let journal: Arc<dyn Journal<ChainEvent>> = Arc::new(MemoryJournal::<ChainEvent>::with_owner(
        JournalOwner::stage(StageId::new()),
    ));
    let event = ChainEventFactory::data_event(WriterId::from(StageId::new()), "test", json!({}));

    wrapped
        .process(
            event,
            Some(effect_context(journal)),
            MiddlewareExecutionScope::StrictReplayHandler,
        )
        .await
        .expect("echo handler should not fail");

    let scopes = seen.lock().expect("seen lock poisoned").clone();
    assert!(
        !scopes.is_empty(),
        "pre_write probe should observe at least one result event"
    );
    assert!(
        scopes
            .iter()
            .all(|s| *s == MiddlewareExecutionScope::StrictReplayHandler),
        "effectful pre_write context must carry the stage scope, got {scopes:?}"
    );
}
