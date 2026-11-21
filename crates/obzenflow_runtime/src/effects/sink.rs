// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::command::Command;
use super::{CallRequest, DeclaredEffectSet, Effects, RecordedReply, StageCompletion};
use crate::stages::common::handler_error::HandlerError;
use crate::stages::common::handlers::EffectfulTransformHandler;
use async_trait::async_trait;
use obzenflow_core::event::schema::{EmptySet, FactsOf, WithMember};
use obzenflow_core::TypedPayload;
use std::marker::PhantomData;

/// A request a sink sends: one whose port method records a reply.
#[diagnostic::on_unimplemented(
    message = "`{Self}` records facts, so a sink can't send it",
    label = "a sink sends a request whose port method is marked `records = reply`",
    note = "a command whose outcome names facts belongs in `effectful_transform!`, and a sink delivers those facts"
)]
pub trait SinkRequest: CallRequest<Records = RecordedReply> {}

impl<R: CallRequest<Records = RecordedReply>> SinkRequest for R {}

/// A destination that commits an ordered group in one physical call.
/// A successful call returns one reply for each request in input order.
#[doc(hidden)]
#[async_trait]
pub trait SinkGroupSystem<R: SinkRequest>: Send + Sync + 'static {
    async fn write_group(
        &self,
        requests: std::sync::Arc<[R]>,
        context: &super::EffectContext,
    ) -> Result<Vec<R::Outcome>, super::EffectError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SinkBatchDefaults {
    pub size: Option<std::num::NonZeroUsize>,
    pub max_wait: Option<std::time::Duration>,
}

/// The command a sink row takes. `prepare` builds the one request the sink
/// sends for each input, from the input and the command's own fields, with no
/// I/O. The runtime sends it through the command's port, records the call and
/// settles the input with that record. A `prepare` error sends nothing, and
/// the input goes to the stage's error journal.
#[diagnostic::on_unimplemented(
    message = "`{Self}` can't be a sink row's command for `{In}`",
    label = "a sink row takes `perform(...)`, a ready-made helper or a `SinkRole`",
    note = "a command that calls ports itself belongs in `effectful_transform!`"
)]
pub trait SinkRole<In>: Command + Send + Sync + 'static {
    /// The request the sink sends for each input.
    type Request: SinkRequest;

    /// Consumes the input so its fields can move into the request.
    fn prepare(&self, input: In) -> Result<Self::Request, HandlerError>;

    #[doc(hidden)]
    fn batch_defaults(&self) -> SinkBatchDefaults {
        SinkBatchDefaults::default()
    }

    /// The role's logic version, which changes only when its logic changes.
    fn stage_logic_version(&self) -> &str {
        "1"
    }
}

/// The handler a sink stage runs for its command.
#[doc(hidden)]
pub struct SinkHandler<C, In> {
    role: std::sync::Arc<C>,
    input: PhantomData<fn(In)>,
}

impl<C, In> SinkHandler<C, In> {
    pub fn new(role: C) -> Self {
        Self {
            role: std::sync::Arc::new(role),
            input: PhantomData,
        }
    }
}

impl<C: Clone, In> Clone for SinkHandler<C, In> {
    fn clone(&self) -> Self {
        Self {
            role: self.role.clone(),
            input: PhantomData,
        }
    }
}

impl<C: std::fmt::Debug, In> std::fmt::Debug for SinkHandler<C, In> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SinkHandler")
            .field("role", &self.role)
            .finish()
    }
}

impl<C: SinkRole<In>, In> Command for SinkHandler<C, In> {
    type Ports = C::Ports;
    type AllowedEffects = DeclaredEffectSet<WithMember<C::Request, EmptySet>>;

    fn ports(&self) -> Self::Ports {
        self.role.ports()
    }
}

pub(super) mod private {
    pub trait Sealed {}
}

#[doc(hidden)]
#[async_trait]
pub trait OpenSinkBatch: private::Sealed + Send + Sync {
    fn admission_wait(&self) -> std::time::Duration;
    async fn admit(&mut self, input: super::EffectInvocationContext) -> Result<bool, HandlerError>;
    async fn finish(
        self: Box<Self>,
    ) -> Result<Vec<Result<(), HandlerError>>, (usize, HandlerError)>;
}

#[async_trait]
impl<C, In> EffectfulTransformHandler for SinkHandler<C, In>
where
    C: SinkRole<In>,
    In: TypedPayload + Send + Sync + 'static,
{
    type Input = In;
    type Output = FactsOf<EmptySet>;

    fn open_sink_batch(
        &self,
        boundary: std::sync::Arc<dyn super::EffectBoundary>,
    ) -> Option<Box<dyn OpenSinkBatch>> {
        Some(Box::new(super::runtime::SinkGroup::new(
            self.role.clone(),
            boundary,
        )))
    }

    async fn process(
        &self,
        input: In,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        let request = self.role.prepare(input)?;
        match fx.send(request).await {
            Err(error) if !error.is_replay_rejection() => return Err(error.into()),
            Ok(_) | Err(_) => {}
        }
        Ok(fx.complete_empty()?)
    }

    fn stage_logic_version(&self) -> &str {
        self.role.stage_logic_version()
    }
}
