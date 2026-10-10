// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Positioned source readers (FLOWIP-134i B1, B14).
//!
//! A `Seek<P>` connector's reader names its input once, reports where that
//! input begins, seeks strictly after a committed position, and returns each
//! record with the position immediately after it. The runtime commits the
//! input pin before the first record and each position atomically with its
//! record; readers never see envelopes, writers or journals.

use super::SourceError;
use obzenflow_core::source::{SourceInputIdentity, SourcePosition};
use obzenflow_core::OneFactStageOutput;

/// A reader that a committed position resumes.
pub trait SeekableSource: Send + Sync {
    type Position: SourcePosition;

    /// What this reader's positions index, validated against the recorded
    /// pin before any seek.
    fn input_identity(&self) -> &SourceInputIdentity;

    /// The position before the first record.
    fn beginning(&self) -> Self::Position;

    /// Continue strictly after `position`, which this input produced.
    fn seek(&mut self, position: Self::Position) -> Result<(), SourceError>;
}

/// One finite poll: records with their positions, `None` once exhausted.
type FinitePoll<T, P> = Result<Option<Vec<SourceRecord<T, P>>>, SourceError>;

/// A synchronous finite reader that reports the position after each record.
pub trait PositionedFiniteReader: SeekableSource {
    type Output: OneFactStageOutput + Send + Sync + 'static;

    /// The next batch, `None` once the input is exhausted.
    fn next(&mut self) -> FinitePoll<Self::Output, Self::Position>;
}

/// One record and the position immediately after it.
#[derive(Debug, Clone, PartialEq)]
pub struct SourceRecord<T, P> {
    pub value: T,
    pub after: P,
}
