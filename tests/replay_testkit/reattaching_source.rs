// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_runtime::stages::source::{
    AtMostOnce, InfiniteSourceConnector, Reattach, SourceReaderInitContext,
    TypedInfiniteSourceHandler,
};
use obzenflow_runtime::stages::SourceError;

/// A resume fixture's infinite reader as a connector. Direct handlers refuse
/// interrupted input, so a fixture that resumes declares what an interruption
/// costs it: `AtMostOnce`, which understates a reader that resumes from the
/// commit point.
#[derive(Clone, Debug)]
pub struct ReattachingSource<R>(pub R);

impl<R> InfiniteSourceConnector for ReattachingSource<R>
where
    R: TypedInfiniteSourceHandler + Clone + 'static,
{
    type Output = R::Output;
    type Reader = R;
    type Continuation = Reattach<AtMostOnce>;

    fn open(&self, _context: SourceReaderInitContext) -> Result<R, SourceError> {
        Ok(self.0.clone())
    }
}
