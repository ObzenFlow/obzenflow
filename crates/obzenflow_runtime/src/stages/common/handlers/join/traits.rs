// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Sealed erased join interface used only by the stage supervisor.

use crate::stages::common::handler_error::HandlerError;
use crate::stages::join::config::{JoinReferenceMode, DEFAULT_REFERENCE_BATCH_CAP};
use async_trait::async_trait;
use obzenflow_core::event::payloads::flow_control_payload::EofKind;
use obzenflow_core::{ChainEvent, StageId, WriterId};

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

    fn reference_batch_cap(&self) -> Option<usize> {
        Some(DEFAULT_REFERENCE_BATCH_CAP)
    }

    fn on_stream_eof(
        &self,
        state: &mut Self::State,
        event: ChainEvent,
        source_id: StageId,
        writer_id: WriterId,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError>;

    async fn drain(
        &self,
        state: &Self::State,
        parent: Option<&ChainEvent>,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError>;
}
