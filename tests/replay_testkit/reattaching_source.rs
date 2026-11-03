// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

/// A resume fixture's infinite reader as a connector. Direct handlers refuse
/// interrupted input, so a fixture that resumes declares what an interruption
/// costs it: `AtMostOnce`, which understates a reader that resumes from the
/// commit point (FLOWIP-122i).
#[derive(Clone, Debug)]
pub struct ReattachingSource<R>(pub R);

impl<R> obzenflow_runtime::stages::source::InfiniteSourceConnector for ReattachingSource<R>
where
    R: obzenflow_runtime::stages::source::TypedInfiniteSourceHandler + Clone + 'static,
{
    type Output = R::Output;
    type Reader = R;
    type Continuation =
        obzenflow_runtime::stages::source::Reattach<obzenflow_runtime::stages::source::AtMostOnce>;

    fn open(
        &self,
        _context: obzenflow_runtime::stages::source::SourceReaderInitContext,
    ) -> Result<R, obzenflow_runtime::stages::SourceError> {
        Ok(self.0.clone())
    }
}
