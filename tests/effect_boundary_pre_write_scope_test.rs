// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-120c phase 6 (test-hygiene cleanup): the effectful results path
//! builds its `pre_write` context with the stage execution scope, not the
//! `LiveHandler` default (the G6 regression), and the scope is a per-call
//! decision (H3).
//!
//! These live in the integration suite rather than as `obzenflow_adapters`
//! unit tests because the effectful case needs a full `EffectInvocationContext`.
//! The root `effect_testkit` helper builds that context with the real
//! `obzenflow_infra::journal::MemoryJournal`, keeping infra dependencies out
//! of lower crates.

mod effect_testkit;

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::json;

use obzenflow_adapters::middleware::{
    Middleware, MiddlewareContext, SourceMiddlewarePhase, UnifiedMiddlewareTransform,
};
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::{MiddlewareExecutionScope, StageId, WriterId};
use obzenflow_runtime::effects::EffectInvocationContext;
use obzenflow_runtime::execution::{RuntimeExecution, RuntimeMode};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::transform::traits::UnifiedTransformHandler;

use effect_testkit::EffectInvocationContextBuilder;

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

    let event = ChainEventFactory::data_event(WriterId::from(StageId::new()), "test", json!({}));
    let effect_context = EffectInvocationContextBuilder::new()
        .with_runtime_execution(RuntimeExecution::new(RuntimeMode::Replay, None))
        .build();

    wrapped
        .process(
            event,
            Some(effect_context),
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
