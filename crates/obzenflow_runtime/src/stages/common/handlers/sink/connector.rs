// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Configured sink connectors and stage-local writer construction.

use super::typed::{SinkWriteContext, SinkWriteReport, SinkWriter, SinkWriterLifecycleReport};
use crate::effects::SinkRedeliverySafety;
use crate::stages::common::handler_error::HandlerError;
use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::{StageId, TypedPayload};

/// Stable, pre-erasure facts about one configured sink connector.
///
/// This is a description of the configured connector, not a second execution
/// protocol. The runtime snapshots it once before opening the stage-local
/// writer. Per-attempt counts, confirmations, and partial failures remain
/// writer outcomes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SinkDescription {
    destination: Option<String>,
    default_method: Option<DeliveryMethod>,
    redelivery_safety: Option<SinkRedeliverySafety>,
}

impl SinkDescription {
    /// Describe a connector with no archive-safety or receipt defaults.
    pub fn unspecified() -> Self {
        Self {
            destination: None,
            default_method: None,
            redelivery_safety: None,
        }
    }

    /// Describe a sink whose receipts use `method` and whose destination
    /// identity falls back to the stage name.
    pub fn method(method: DeliveryMethod) -> Self {
        Self {
            destination: None,
            default_method: Some(method),
            redelivery_safety: None,
        }
    }

    /// Describe a configured destination and its normal receipt method.
    /// Redelivery safety is an independent fact added with
    /// [`with_redelivery_safety`](Self::with_redelivery_safety) when known.
    pub fn destination(destination: impl Into<String>, method: DeliveryMethod) -> Self {
        Self {
            destination: Some(destination.into()),
            default_method: Some(method),
            redelivery_safety: None,
        }
    }

    /// Set or replace the archive redelivery classification.
    pub fn with_redelivery_safety(mut self, safety: SinkRedeliverySafety) -> Self {
        self.redelivery_safety = Some(safety);
        self
    }

    #[doc(hidden)]
    pub fn destination_name(&self) -> Option<&str> {
        self.destination.as_deref()
    }

    #[doc(hidden)]
    pub fn default_method(&self) -> Option<&DeliveryMethod> {
        self.default_method.as_ref()
    }

    #[doc(hidden)]
    pub fn redelivery_safety(&self) -> Option<SinkRedeliverySafety> {
        self.redelivery_safety
    }
}

/// Narrow runtime context supplied when a configured connector opens its
/// stage-local writer.
///
/// Connectors deliberately do not receive `StageResources`: journals,
/// middleware, replay archives, and runtime coordination remain framework
/// concerns rather than adapter side channels.
#[derive(Clone, Debug)]
pub struct SinkWriterInitContext {
    stage_id: StageId,
    stage_name: String,
    flow_name: String,
}

impl SinkWriterInitContext {
    #[doc(hidden)]
    pub fn new(stage_id: StageId, stage_name: String, flow_name: String) -> Self {
        Self {
            stage_id,
            stage_name,
            flow_name,
        }
    }

    pub fn stage_id(&self) -> StageId {
        self.stage_id
    }

    pub fn stage_name(&self) -> &str {
        &self.stage_name
    }

    pub fn flow_name(&self) -> &str {
        &self.flow_name
    }
}

/// Configured sink factory admitted by `sink!`.
///
/// A connector is reusable immutable configuration. Each open creates a fresh,
/// mutable writer owned by one materialised sink stage.
#[async_trait]
#[diagnostic::on_unimplemented(
    message = "this sink connector does not witness its declared input",
    note = "implement SinkConnector with Input matching the sink! arrow (FLOWIP-134h)"
)]
pub trait SinkConnector: Send + Sync + Sized + 'static {
    type Input: TypedPayload + Send + Sync + 'static;
    type Writer: SinkWriter<Input = Self::Input>;

    /// Return the configured connector description captured before erasure.
    fn describe(&self) -> SinkDescription;

    /// Open a fresh stage-local mutable writer.
    async fn open(&self, context: SinkWriterInitContext) -> Result<Self::Writer, HandlerError>;
}

/// Small, already-configured sink tier for in-process integrations.
///
/// `InlineSink` deliberately combines configuration and execution when there
/// is no meaningful resource-opening lifecycle to separate. The blanket
/// implementations below still lower it through the canonical connector and
/// writer roles. Resource-owning integrations should implement
/// [`SinkConnector`] on a separate configuration type and return their writer
/// from `open`.
///
/// `Clone` is the opening boundary for this tier. A clone must not share
/// transient buffer, transaction, or pending-input state with its source.
/// Shared handles may represent the external destination itself, as in a test
/// probe backed by an `Arc`.
#[async_trait]
pub trait InlineSink: Clone + Send + Sync + 'static {
    type Input: TypedPayload + Send + Sync + 'static;

    /// Describe any fixed receipt defaults and redelivery classification.
    fn describe(&self) -> SinkDescription {
        SinkDescription::unspecified()
    }

    async fn write(
        &mut self,
        input: Self::Input,
        context: SinkWriteContext,
    ) -> Result<SinkWriteReport, HandlerError>;

    async fn flush(&mut self) -> Result<SinkWriterLifecycleReport, HandlerError> {
        Ok(SinkWriterLifecycleReport::default())
    }

    async fn drain(&mut self) -> Result<SinkWriterLifecycleReport, HandlerError> {
        self.flush().await
    }
}

#[async_trait]
impl<I> SinkWriter for I
where
    I: InlineSink,
{
    type Input = I::Input;

    async fn write(
        &mut self,
        input: Self::Input,
        context: SinkWriteContext,
    ) -> Result<SinkWriteReport, HandlerError> {
        InlineSink::write(self, input, context).await
    }

    async fn flush(&mut self) -> Result<SinkWriterLifecycleReport, HandlerError> {
        InlineSink::flush(self).await
    }

    async fn drain(&mut self) -> Result<SinkWriterLifecycleReport, HandlerError> {
        InlineSink::drain(self).await
    }
}

#[async_trait]
impl<I> SinkConnector for I
where
    I: InlineSink,
{
    type Input = I::Input;
    type Writer = I;

    fn describe(&self) -> SinkDescription {
        InlineSink::describe(self)
    }

    async fn open(&self, _context: SinkWriterInitContext) -> Result<Self::Writer, HandlerError> {
        Ok(self.clone())
    }
}

/// Connector wrapper used by the `sink!` site-level redelivery clause.
#[doc(hidden)]
pub struct WithRedeliverySafety<C> {
    connector: C,
    safety: SinkRedeliverySafety,
}

impl<C> WithRedeliverySafety<C> {
    pub fn new(connector: C, safety: SinkRedeliverySafety) -> Self {
        Self { connector, safety }
    }
}

impl<C: std::fmt::Debug> std::fmt::Debug for WithRedeliverySafety<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WithRedeliverySafety")
            .field("connector", &self.connector)
            .field("safety", &self.safety)
            .finish()
    }
}

#[async_trait]
impl<C> SinkConnector for WithRedeliverySafety<C>
where
    C: SinkConnector,
{
    type Input = C::Input;
    type Writer = C::Writer;

    fn describe(&self) -> SinkDescription {
        self.connector
            .describe()
            .with_redelivery_safety(self.safety)
    }

    async fn open(&self, context: SinkWriterInitContext) -> Result<Self::Writer, HandlerError> {
        self.connector.open(context).await
    }
}
