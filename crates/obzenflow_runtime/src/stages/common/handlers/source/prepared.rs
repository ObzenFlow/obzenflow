// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Sealed, single-owner source admission. No opening or producer calls happen
//! while preparing a descriptor. The supervisor alone acquires its live reader.
//! Each source family has an explicit implementation; only reader ownership is shared.

use super::SourceError;

mod async_finite;
mod async_infinite;
mod finite;
mod infinite;

pub use async_finite::AdmitAsyncFiniteSource;
pub use async_infinite::AdmitAsyncInfiniteSource;
pub use finite::AdmitFiniteSource;
pub use infinite::AdmitInfiniteSource;

#[cfg(test)]
pub(crate) use async_finite::PreparedAsyncFiniteSource;
#[cfg(test)]
pub(crate) use async_infinite::PreparedAsyncInfiniteSource;

mod sealed {
    pub trait Admitted<Kind> {}
}

#[doc(hidden)]
pub enum DirectSource {}
#[doc(hidden)]
pub enum ConnectorSource {}

// An opening future owns its configuration. Cancellation consumes admission,
// so an interrupted acquisition can never silently reopen or reuse a producer.
enum ReaderState<R: ?Sized, F> {
    Cold(F),
    Acquired(Box<R>),
    Consumed,
}

impl<R: ?Sized, F> ReaderState<R, F> {
    /// Consume admission before calling or awaiting the opener. An acquired
    /// reader needs no further opening; a failed or cancelled open cannot retry.
    fn take_opener(&mut self) -> Result<Option<F>, SourceError> {
        if matches!(self, Self::Acquired(_)) {
            return Ok(None);
        }
        match std::mem::replace(self, Self::Consumed) {
            Self::Cold(open) => Ok(Some(open)),
            Self::Consumed => Err(SourceError::Other(
                "source acquisition already consumed".into(),
            )),
            Self::Acquired(_) => unreachable!("acquired readers were handled above"),
        }
    }

    /// Take ownership before installing capabilities or doing any further
    /// fallible or awaited work with the reader.
    fn store_reader(&mut self, reader: Box<R>) -> &mut R {
        *self = Self::Acquired(reader);
        self.reader_mut().expect("the reader was just acquired")
    }

    fn reader(&self) -> Option<&R> {
        match self {
            Self::Acquired(reader) => Some(reader.as_ref()),
            Self::Cold(_) | Self::Consumed => None,
        }
    }

    fn reader_mut(&mut self) -> Option<&mut R> {
        match self {
            Self::Acquired(reader) => Some(reader.as_mut()),
            Self::Cold(_) | Self::Consumed => None,
        }
    }
}
