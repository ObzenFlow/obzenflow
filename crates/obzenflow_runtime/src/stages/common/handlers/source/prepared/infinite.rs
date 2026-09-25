// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Synchronous infinite source admission and supervised acquisition.

use super::{sealed, ConnectorSource, DirectSource, ReaderState};
use crate::stages::common::handlers::source::erased::{
    ErasedSourceInvocation, SealInfinite, UnifiedInfiniteSourceHandler,
};
use crate::stages::common::handlers::source::typed::{
    TypedInfiniteSourceHandler, TypedInfiniteSourceHandlerAdapter,
};
use crate::stages::source::{InfiniteSourceConnector, SourceError, SourceReaderInitContext};
use obzenflow_core::event::observability::ObservationRecorder;
use obzenflow_core::{OneFactStageOutput, WriterId};
use std::sync::Arc;

type OpenResult = Result<Box<dyn UnifiedInfiniteSourceHandler>, SourceError>;
type OpenReader = Box<dyn FnOnce(SourceReaderInitContext) -> OpenResult + Send + Sync>;

#[doc(hidden)]
pub trait AdmitInfiniteSource<Kind>: sealed::Admitted<Kind> + Send + Sync + 'static {
    type Output: OneFactStageOutput + Send + Sync + 'static;

    fn prepare(self) -> PreparedInfiniteSource;
}

impl<H: TypedInfiniteSourceHandler + 'static>
    sealed::Admitted<(DirectSource, PreparedInfiniteSource)> for H
{
}

impl<C: InfiniteSourceConnector> sealed::Admitted<(ConnectorSource, PreparedInfiniteSource)> for C {}

impl<H: TypedInfiniteSourceHandler + 'static>
    AdmitInfiniteSource<(DirectSource, PreparedInfiniteSource)> for H
{
    type Output = H::Output;

    fn prepare(self) -> PreparedInfiniteSource {
        PreparedInfiniteSource::new(Box::new(move |_| {
            Ok(Box::new(TypedInfiniteSourceHandlerAdapter::new(self)))
        }))
    }
}

impl<C: InfiniteSourceConnector> AdmitInfiniteSource<(ConnectorSource, PreparedInfiniteSource)>
    for C
{
    type Output = C::Output;

    fn prepare(self) -> PreparedInfiniteSource {
        PreparedInfiniteSource::new(Box::new(move |context| {
            let reader = self.open(context)?;
            Ok(Box::new(TypedInfiniteSourceHandlerAdapter::new(reader)))
        }))
    }
}

#[doc(hidden)]
pub struct PreparedInfiniteSource {
    state: ReaderState<dyn UnifiedInfiniteSourceHandler, OpenReader>,
    writer_id: Option<WriterId>,
    recorder: Option<Arc<dyn ObservationRecorder>>,
}

impl PreparedInfiniteSource {
    fn new(open: OpenReader) -> Self {
        Self {
            state: ReaderState::Cold(open),
            writer_id: None,
            recorder: None,
        }
    }
}

impl SealInfinite for PreparedInfiniteSource {}

impl UnifiedInfiniteSourceHandler for PreparedInfiniteSource {
    fn install_writer_id(&mut self, writer_id: WriterId) {
        self.writer_id = Some(writer_id);
    }

    fn install_observation_recorder(&mut self, recorder: Arc<dyn ObservationRecorder>) {
        self.recorder = Some(recorder);
    }

    fn acquire(&mut self, context: SourceReaderInitContext) -> Result<(), SourceError> {
        let Some(open) = self.state.take_opener()? else {
            return Ok(());
        };
        let reader = self.state.store_reader(open(context)?);
        if let Some(writer_id) = self.writer_id {
            reader.install_writer_id(writer_id);
        }
        if let Some(recorder) = &self.recorder {
            reader.install_observation_recorder(recorder.clone());
        }
        Ok(())
    }

    fn next_invocation(&mut self) -> ErasedSourceInvocation {
        self.state
            .reader_mut()
            .expect("the source supervisor must acquire before polling")
            .next_invocation()
    }
}
