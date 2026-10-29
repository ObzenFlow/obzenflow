// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Sealed erased join interface used only by the stage supervisor.

use crate::stages::common::handler_error::{HandlerError, StageFatal};
use crate::stages::join::config::{
    JoinCatalogKind, JoinReferenceMode, DEFAULT_REFERENCE_BATCH_CAP,
};
use async_trait::async_trait;
use obzenflow_core::event::payloads::flow_control_payload::EofKind;
use obzenflow_core::{ChainEvent, OneFactStageOutput, StageId, TypedPayload, WriterId};

pub(crate) mod sealed {
    pub trait Sealed {}
}

/// Sealed runtime result for one erased stream-side join invocation.
///
/// Domain authors return facts through `TypedJoinHandler`. Framework strategies
/// may additionally request terminal control, but the intent remains separate
/// from the facts until the supervisor can seal it at the durable commit
/// frontier.
#[doc(hidden)]
#[derive(Debug)]
pub struct ErasedJoinInvocation {
    events: Vec<ChainEvent>,
    framework_eof: Option<EofKind>,
}

impl ErasedJoinInvocation {
    pub(crate) fn new(events: Vec<ChainEvent>, framework_eof: Option<EofKind>) -> Self {
        Self {
            events,
            framework_eof,
        }
    }

    #[doc(hidden)]
    pub fn into_parts(self) -> (Vec<ChainEvent>, Option<EofKind>) {
        (self.events, self.framework_eof)
    }
}

/// Unified join surface used by the join stage supervisor.
///
/// `process_event` takes the per-delivery middleware execution scope computed
/// by the supervisor at dispatch (FLOWIP-120n); handlers without middleware
/// ignore it.
#[doc(hidden)]
#[async_trait]
pub trait UnifiedJoinHandler: sealed::Sealed + Send + Sync {
    type State: Clone + Send + Sync;

    /// FLOWIP-010 §7: forwarded to the wrapped handler at stage build.
    fn install_lineage_policy(&mut self, _policy: obzenflow_core::config::LineagePolicy) {}

    /// Installed once by the runtime before the handler is shared.
    fn install_writer_id(&mut self, _writer_id: WriterId) {}

    /// Installed once by the flow build with the join's and its catalog's stage names.
    fn install_stage_names(&mut self, _join: &str, _catalog: &str) {}

    fn initial_state(&self) -> Self::State;

    fn process_reference(
        &self,
        state: &mut Self::State,
        event: ChainEvent,
        source_id: StageId,
        writer_id: WriterId,
        scope: obzenflow_core::MiddlewareExecutionScope,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError>;

    fn process_stream(
        &self,
        state: &mut Self::State,
        event: ChainEvent,
        source_id: StageId,
        writer_id: WriterId,
        scope: obzenflow_core::MiddlewareExecutionScope,
    ) -> std::result::Result<ErasedJoinInvocation, HandlerError>;

    fn reference_mode(&self) -> JoinReferenceMode {
        JoinReferenceMode::FiniteEof
    }

    fn catalog_kind(&self) -> JoinCatalogKind {
        JoinCatalogKind::LatestValue
    }

    #[doc(hidden)]
    fn strategy_label(&self) -> Option<&'static str> {
        None
    }

    fn reference_batch_cap(&self) -> Option<usize> {
        Some(DEFAULT_REFERENCE_BATCH_CAP)
    }

    fn on_stream_eof(
        &self,
        state: &mut Self::State,
        event: ChainEvent,
        source_id: StageId,
        writer_id: WriterId,
    ) -> std::result::Result<Vec<ChainEvent>, StageFatal>;

    async fn drain(
        &self,
        state: &Self::State,
        parent: Option<&ChainEvent>,
    ) -> std::result::Result<Vec<ChainEvent>, StageFatal>;
}

/// A handler that `join!` accepts, with the facts its arrow must witness.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this join handler does not witness its arrow contract",
    label = "this handler implements neither join contract",
    note = "implement TypedJoinHandler, or implement TypedAsOfJoinHandler and bind it with AsOfJoin::new"
)]
pub trait JoinBinding: Clone + std::fmt::Debug + Send + Sync + 'static {
    type Reference: TypedPayload + Clone + Send + Sync + 'static;
    type Stream: TypedPayload + Send + Sync + 'static;
    type Output: OneFactStageOutput + Send + Sync + 'static;
    type Adapter: UnifiedJoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static;

    fn into_adapter(self) -> Self::Adapter;
}
