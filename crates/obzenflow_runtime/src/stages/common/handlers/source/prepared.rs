// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Shared source admission and single-owner reader acquisition.
//!
//! Admission selects an existing typed adapter. Its reader stays cold until the
//! supervisor acquires it; this module has no polling or envelope-lowering path.

use super::continuation::{AsyncReaderOpener, SourceContinuation, SyncReaderOpener};
use super::SourceError;
use crate::stages::source::SourceReaderInitContext;
use obzenflow_core::OneFactStageOutput;

pub(super) mod sealed {
    pub trait Admitted<Family: ?Sized, Kind> {}
}

/// Select the existing runtime adapter for a direct handler, cold connector or
/// Runtime-owned hosted ingress.
///
/// `Family` is the requested erased source contract. `Kind` distinguishes the
/// three without requiring either authoring trait to change.
#[doc(hidden)]
pub trait AdmitSource<Family: ?Sized, Kind>:
    sealed::Admitted<Family, Kind> + Send + Sync + 'static
{
    type Output: OneFactStageOutput + Send + Sync + 'static;
    type Handler: Send + Sync + 'static;
    /// The continuation captured with the source, before any open (FLOWIP-134i B8).
    const CONTINUATION: SourceContinuation;

    fn prepare(self) -> Self::Handler;
}

#[doc(hidden)]
pub enum DirectSource {}
#[doc(hidden)]
pub enum ConnectorSource {}
/// Hosted ingress, admitted by Runtime because only its single writer
/// acknowledges after commit (FLOWIP-122i B3).
#[doc(hidden)]
pub enum HostedSource {}

pub(super) type SyncSourceReader<H> = SourceReader<H, SyncReaderOpener<H>>;
pub(super) type AsyncSourceReader<H> = SourceReader<H, AsyncReaderOpener<H>>;

/// Reader ownership shared by all typed adapters, independent of poll semantics.
pub(super) struct SourceReader<H, F> {
    state: ReaderState<H, F>,
}

enum ReaderState<H, F> {
    Cold(F),
    Acquired(H),
    Consumed,
}

impl<H, F> SourceReader<H, F> {
    pub(super) fn cold(open: F) -> Self {
        Self {
            state: ReaderState::Cold(open),
        }
    }

    /// Wrap a live reader supplied by framework code; admission uses `cold`.
    pub(super) fn acquired(reader: H) -> Self {
        Self {
            state: ReaderState::Acquired(reader),
        }
    }

    // Consume admission before calling or awaiting the opener. In particular,
    // dropping an opening future must never make its configuration reusable.
    fn take_opener(&mut self) -> Result<Option<F>, SourceError> {
        if matches!(self.state, ReaderState::Acquired(_)) {
            return Ok(None);
        }
        match std::mem::replace(&mut self.state, ReaderState::Consumed) {
            ReaderState::Cold(open) => Ok(Some(open)),
            ReaderState::Consumed => Err(SourceError::Other(
                obzenflow_core::event::SourceDiagnosticReason::Unclassified.into(),
            )),
            ReaderState::Acquired(_) => unreachable!("acquired readers were handled above"),
        }
    }

    // Take ownership before the adapter installs any capabilities on the reader.
    fn store_reader(&mut self, reader: H) -> &mut H {
        self.state = ReaderState::Acquired(reader);
        self.get_mut().expect("the reader was just acquired")
    }

    pub(super) fn get(&self) -> Option<&H> {
        match &self.state {
            ReaderState::Acquired(reader) => Some(reader),
            ReaderState::Cold(_) | ReaderState::Consumed => None,
        }
    }

    pub(super) fn get_mut(&mut self) -> Option<&mut H> {
        match &mut self.state {
            ReaderState::Acquired(reader) => Some(reader),
            ReaderState::Cold(_) | ReaderState::Consumed => None,
        }
    }
}

impl<H> SyncSourceReader<H> {
    /// Return the newly owned reader for capability installation, once only.
    pub(super) fn acquire(
        &mut self,
        context: SourceReaderInitContext,
    ) -> Result<Option<&mut H>, SourceError> {
        let Some(open) = self.take_opener()? else {
            return Ok(None);
        };
        let reader = open(context)?;
        Ok(Some(self.store_reader(reader)))
    }
}

impl<H> AsyncSourceReader<H> {
    /// The opening future owns partial resources until it returns the reader.
    pub(super) async fn acquire(
        &mut self,
        context: SourceReaderInitContext,
    ) -> Result<Option<&mut H>, SourceError> {
        let Some(open) = self.take_opener()? else {
            return Ok(None);
        };
        let reader = open(context).await?;
        Ok(Some(self.store_reader(reader)))
    }
}
