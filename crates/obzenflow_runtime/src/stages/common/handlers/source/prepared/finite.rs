// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Synchronous finite source admission and supervised acquisition.

use super::{sealed, ConnectorSource, DirectSource, ReaderState};
use crate::stages::common::handlers::source::erased::{
    ErasedSourceInvocation, SealFinite, UnifiedFiniteSourceHandler,
};
use crate::stages::common::handlers::source::typed::{
    TypedFiniteSourceHandler, TypedFiniteSourceHandlerAdapter,
};
use crate::stages::source::{FiniteSourceConnector, SourceError, SourceReaderInitContext};
use obzenflow_core::event::observability::ObservationRecorder;
use obzenflow_core::{OneFactStageOutput, WriterId};
use std::sync::Arc;

type OpenResult = Result<Box<dyn UnifiedFiniteSourceHandler>, SourceError>;
type OpenReader = Box<dyn FnOnce(SourceReaderInitContext) -> OpenResult + Send + Sync>;

#[doc(hidden)]
pub trait AdmitFiniteSource<Kind>: sealed::Admitted<Kind> + Send + Sync + 'static {
    type Output: OneFactStageOutput + Send + Sync + 'static;

    fn prepare(self) -> PreparedFiniteSource;
}

impl<H: TypedFiniteSourceHandler + 'static> sealed::Admitted<(DirectSource, PreparedFiniteSource)>
    for H
{
}

impl<C: FiniteSourceConnector> sealed::Admitted<(ConnectorSource, PreparedFiniteSource)> for C {}

impl<H: TypedFiniteSourceHandler + 'static> AdmitFiniteSource<(DirectSource, PreparedFiniteSource)>
    for H
{
    type Output = H::Output;

    fn prepare(self) -> PreparedFiniteSource {
        PreparedFiniteSource::new(Box::new(move |_| {
            Ok(Box::new(TypedFiniteSourceHandlerAdapter::new(self)))
        }))
    }
}

impl<C: FiniteSourceConnector> AdmitFiniteSource<(ConnectorSource, PreparedFiniteSource)> for C {
    type Output = C::Output;

    fn prepare(self) -> PreparedFiniteSource {
        PreparedFiniteSource::new(Box::new(move |context| {
            let reader = self.open(context)?;
            Ok(Box::new(TypedFiniteSourceHandlerAdapter::new(reader)))
        }))
    }
}

#[doc(hidden)]
pub struct PreparedFiniteSource {
    state: ReaderState<dyn UnifiedFiniteSourceHandler, OpenReader>,
    writer_id: Option<WriterId>,
    recorder: Option<Arc<dyn ObservationRecorder>>,
}

impl PreparedFiniteSource {
    fn new(open: OpenReader) -> Self {
        Self {
            state: ReaderState::Cold(open),
            writer_id: None,
            recorder: None,
        }
    }
}

impl SealFinite for PreparedFiniteSource {}

impl UnifiedFiniteSourceHandler for PreparedFiniteSource {
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
