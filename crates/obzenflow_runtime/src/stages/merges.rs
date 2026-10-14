// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Framework merge strategies (FLOWIP-084q B7, B9).
//!
//! A merge stage combines several producers of one type and journals the merged
//! stream. Its strategy decides the order, and the merge supervisor alone
//! interprets it. This is a closed set: a merge runs no author code, and each
//! further strategy arrives in its own FLOWIP.

mod sealed {
    pub trait Sealed {}
}

/// The strategy a merge stage orders its producers under.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MergeStrategyKind {
    /// Every batch producer's rows, up to each one's natural EOF, before any
    /// live producer's (B8).
    ThenLive,
}

/// A framework merge strategy, named in `merge!`'s `=>` slot.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a merge strategy",
    note = "use a strategy from `obzenflow::stages::merges`, for example `merges::then_live`.\nA merge runs no author code."
)]
pub trait MergeStrategy: sealed::Sealed + Copy + Send + Sync + 'static {
    /// The strategy the merge supervisor interprets.
    fn kind(self) -> MergeStrategyKind;
}

/// Pass on every batch producer's rows, up to each one's natural EOF, before
/// any live producer's (FLOWIP-084q B8).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ThenLive;

impl sealed::Sealed for ThenLive {}

impl MergeStrategy for ThenLive {
    fn kind(self) -> MergeStrategyKind {
        MergeStrategyKind::ThenLive
    }
}

/// `merges::then_live`, the value named in `merge!(T => merges::then_live)`.
pub use ThenLive as then_live;

#[cfg(test)]
mod tests {
    use super::*;

    fn kind_of<S: MergeStrategy>(strategy: S) -> MergeStrategyKind {
        strategy.kind()
    }

    #[test]
    fn then_live_names_the_then_live_strategy() {
        assert_eq!(kind_of(then_live), MergeStrategyKind::ThenLive);
    }
}
