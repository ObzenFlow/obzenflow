// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Shared source admission and single-owner reader acquisition.
//!
//! Admission selects an existing typed adapter. Its reader stays cold until the
//! supervisor acquires it; this module has no polling or envelope-lowering path.

use super::continuation::{AsyncReaderOpener, SourceContinuation, SyncReaderOpener};
use super::SourceError;
use crate::effects::SourceSystem;
use crate::stages::source::SourceReaderInitContext;
use obzenflow_core::OneFactOutcome;

pub(super) mod sealed {
    pub trait Admitted<Family: ?Sized> {}
}

#[doc(hidden)]
pub struct AdmittedSource<H> {
    pub(crate) handler: H,
    pub(crate) continuation: SourceContinuation,
    pub(crate) system: SourceSystem,
}

impl<H> AdmittedSource<H> {
    pub fn continuation(&self) -> SourceContinuation {
        self.continuation
    }

    pub fn system(&self) -> &SourceSystem {
        &self.system
    }
}

impl<H: super::UnifiedAsyncInfiniteSourceHandler> AdmittedSource<H> {
    pub fn hosted_ingress_slot(&self) -> Option<obzenflow_core::ingress::HostedIngressBindingSlot> {
        self.handler.hosted_ingress_slot()
    }
}

#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this source stage does not take `{Self}` as its port",
    note = "`source!` takes `Port<dyn FiniteSource<_, _>>`, `async_source!` takes `Port<dyn AsyncFiniteSource<_, _>>`, `infinite_source!` takes `Port<dyn InfiniteSource<_, _>>`, and `async_infinite_source!` takes `Port<dyn AsyncInfiniteSource<_, _>>` or the `Port<dyn HostedIngress<_>>` from ingress registration"
)]
pub trait AdmitSource<Family: ?Sized>: sealed::Admitted<Family> + Send + Sync + 'static {
    type Output: OneFactOutcome + Send + Sync + 'static;
    type Handler: Send + Sync + 'static;

    fn admit(self) -> AdmittedSource<Self::Handler>;
}

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
