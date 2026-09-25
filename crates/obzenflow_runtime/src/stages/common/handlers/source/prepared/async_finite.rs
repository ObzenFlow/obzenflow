// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Asynchronous finite source admission and supervised acquisition.

use super::{sealed, ConnectorSource, DirectSource, ReaderState};
use crate::stages::common::handlers::source::erased::{
    ErasedSourceInvocation, SealAsyncFinite, UnifiedAsyncFiniteSourceHandler,
};
use crate::stages::common::handlers::source::typed::{
    TypedAsyncFiniteSourceHandler, TypedAsyncFiniteSourceHandlerAdapter,
};
use crate::stages::source::{AsyncFiniteSourceConnector, SourceError, SourceReaderInitContext};
use async_trait::async_trait;
use futures::future::BoxFuture;
use obzenflow_core::event::observability::ObservationRecorder;
use obzenflow_core::{OneFactStageOutput, WriterId};
use std::sync::Arc;
use std::time::Duration;

type OpenFuture = BoxFuture<'static, Result<Box<dyn UnifiedAsyncFiniteSourceHandler>, SourceError>>;
type OpenReader = Box<dyn FnOnce(SourceReaderInitContext) -> OpenFuture + Send + Sync>;

#[doc(hidden)]
pub trait AdmitAsyncFiniteSource<Kind>: sealed::Admitted<Kind> + Send + Sync + 'static {
    type Output: OneFactStageOutput + Send + Sync + 'static;

    fn prepare(self) -> PreparedAsyncFiniteSource;
}

impl<H: TypedAsyncFiniteSourceHandler + 'static>
    sealed::Admitted<(DirectSource, PreparedAsyncFiniteSource)> for H
{
}

impl<C: AsyncFiniteSourceConnector> sealed::Admitted<(ConnectorSource, PreparedAsyncFiniteSource)>
    for C
{
}

impl<H: TypedAsyncFiniteSourceHandler + 'static>
    AdmitAsyncFiniteSource<(DirectSource, PreparedAsyncFiniteSource)> for H
{
    type Output = H::Output;

    fn prepare(self) -> PreparedAsyncFiniteSource {
        PreparedAsyncFiniteSource::new(Box::new(move |_| {
            Box::pin(async move {
                let reader: Box<dyn UnifiedAsyncFiniteSourceHandler> =
                    Box::new(TypedAsyncFiniteSourceHandlerAdapter::new(self));
                Ok(reader)
            })
        }))
    }
}

impl<C: AsyncFiniteSourceConnector>
    AdmitAsyncFiniteSource<(ConnectorSource, PreparedAsyncFiniteSource)> for C
{
    type Output = C::Output;

    fn prepare(self) -> PreparedAsyncFiniteSource {
        PreparedAsyncFiniteSource::new(Box::new(move |context| {
            Box::pin(async move {
                let reader = self.open(context).await?;
                let reader: Box<dyn UnifiedAsyncFiniteSourceHandler> =
                    Box::new(TypedAsyncFiniteSourceHandlerAdapter::new(reader));
                Ok(reader)
            })
        }))
    }
}

#[doc(hidden)]
pub struct PreparedAsyncFiniteSource {
    state: ReaderState<dyn UnifiedAsyncFiniteSourceHandler, OpenReader>,
    writer_id: Option<WriterId>,
    recorder: Option<Arc<dyn ObservationRecorder>>,
}

impl PreparedAsyncFiniteSource {
    fn new(open: OpenReader) -> Self {
        Self {
            state: ReaderState::Cold(open),
            writer_id: None,
            recorder: None,
        }
    }
}

impl SealAsyncFinite for PreparedAsyncFiniteSource {}

#[async_trait]
impl UnifiedAsyncFiniteSourceHandler for PreparedAsyncFiniteSource {
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
        match self.state.reader() {
            Some(reader) => reader.poll_timeout(),
            None => Some(Duration::from_secs(30)),
        }
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
