// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Asynchronous infinite source admission and supervised acquisition.

use super::{sealed, ConnectorSource, DirectSource, ReaderState};
use crate::stages::common::handlers::source::erased::{
    ErasedSourceInvocation, SealAsyncInfinite, UnifiedAsyncInfiniteSourceHandler,
};
use crate::stages::common::handlers::source::typed::{
    TypedAsyncInfiniteSourceHandler, TypedAsyncInfiniteSourceHandlerAdapter,
};
use crate::stages::source::{AsyncInfiniteSourceConnector, SourceError, SourceReaderInitContext};
use async_trait::async_trait;
use futures::future::BoxFuture;
use obzenflow_core::event::observability::ObservationRecorder;
use obzenflow_core::ingress::HostedIngressBindingSlot;
use obzenflow_core::{OneFactStageOutput, WriterId};
use std::sync::Arc;
use std::time::Duration;

type OpenFuture =
    BoxFuture<'static, Result<Box<dyn UnifiedAsyncInfiniteSourceHandler>, SourceError>>;
type OpenReader = Box<dyn FnOnce(SourceReaderInitContext) -> OpenFuture + Send + Sync>;

#[doc(hidden)]
pub trait AdmitAsyncInfiniteSource<Kind>: sealed::Admitted<Kind> + Send + Sync + 'static {
    type Output: OneFactStageOutput + Send + Sync + 'static;

    fn prepare(self) -> PreparedAsyncInfiniteSource;
}

impl<H: TypedAsyncInfiniteSourceHandler + 'static>
    sealed::Admitted<(DirectSource, PreparedAsyncInfiniteSource)> for H
{
}

impl<C: AsyncInfiniteSourceConnector>
    sealed::Admitted<(ConnectorSource, PreparedAsyncInfiniteSource)> for C
{
}

impl<H: TypedAsyncInfiniteSourceHandler + 'static>
    AdmitAsyncInfiniteSource<(DirectSource, PreparedAsyncInfiniteSource)> for H
{
    type Output = H::Output;

    fn prepare(self) -> PreparedAsyncInfiniteSource {
        // Register the cold hosted-ingress binding without claiming its receiver.
        let hosted_slot = self
            .runtime_registration()
            .map(|registration| registration.hosted_ingress_slot);
        PreparedAsyncInfiniteSource::new(
            Box::new(move |_| {
                Box::pin(async move {
                    let reader: Box<dyn UnifiedAsyncInfiniteSourceHandler> =
                        Box::new(TypedAsyncInfiniteSourceHandlerAdapter::new(self));
                    Ok(reader)
                })
            }),
            hosted_slot,
        )
    }
}

impl<C: AsyncInfiniteSourceConnector>
    AdmitAsyncInfiniteSource<(ConnectorSource, PreparedAsyncInfiniteSource)> for C
{
    type Output = C::Output;

    fn prepare(self) -> PreparedAsyncInfiniteSource {
        PreparedAsyncInfiniteSource::new(
            Box::new(move |context| {
                Box::pin(async move {
                    let reader = self.open(context).await?;
                    let reader: Box<dyn UnifiedAsyncInfiniteSourceHandler> =
                        Box::new(TypedAsyncInfiniteSourceHandlerAdapter::new(reader));
                    Ok(reader)
                })
            }),
            None,
        )
    }
}

#[doc(hidden)]
pub struct PreparedAsyncInfiniteSource {
    state: ReaderState<dyn UnifiedAsyncInfiniteSourceHandler, OpenReader>,
    writer_id: Option<WriterId>,
    recorder: Option<Arc<dyn ObservationRecorder>>,
    hosted_slot: Option<HostedIngressBindingSlot>,
}

impl PreparedAsyncInfiniteSource {
    fn new(open: OpenReader, hosted_slot: Option<HostedIngressBindingSlot>) -> Self {
        Self {
            state: ReaderState::Cold(open),
            writer_id: None,
            recorder: None,
            hosted_slot,
        }
    }
}

impl SealAsyncInfinite for PreparedAsyncInfiniteSource {}

#[async_trait]
impl UnifiedAsyncInfiniteSourceHandler for PreparedAsyncInfiniteSource {
    fn install_writer_id(&mut self, writer_id: WriterId) {
        self.writer_id = Some(writer_id);
    }

    fn install_observation_recorder(&mut self, recorder: Arc<dyn ObservationRecorder>) {
        self.recorder = Some(recorder);
    }

    async fn acquire(&mut self, context: SourceReaderInitContext) -> Result<(), SourceError> {
        let Some(open) = self.state.take_opener()? else {
            return Ok(());
        };
        let reader = self.state.store_reader(open(context).await?);
        if let Some(writer_id) = self.writer_id {
            reader.install_writer_id(writer_id);
        }
        if let Some(recorder) = &self.recorder {
            reader.install_observation_recorder(recorder.clone());
        }
        Ok(())
    }

    fn poll_timeout(&self) -> Option<Duration> {
        self.state.reader().and_then(|reader| reader.poll_timeout())
    }

    fn hosted_ingress_slot(&self) -> Option<HostedIngressBindingSlot> {
        self.hosted_slot.clone()
    }

    async fn next_invocation(&mut self) -> ErasedSourceInvocation {
        self.state
            .reader_mut()
            .expect("the source supervisor must acquire before polling")
            .next_invocation()
            .await
    }

    async fn drain(&mut self) -> Result<(), SourceError> {
        match self.state.reader_mut() {
            Some(reader) => reader.drain().await,
            None => Ok(()),
        }
    }
}
