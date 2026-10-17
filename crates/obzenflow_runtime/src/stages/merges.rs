// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Framework merge strategies, a closed set that only the merge supervisor
//! interprets. A strategy decides the order in which a merge journals its producers.

mod sealed {
    pub trait Sealed {}
}

/// The strategy a merge stage orders its producers under.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MergeStrategyKind {
    /// Every batch producer's rows, up to each one's natural EOF, before any
    /// live producer's.
    ThenLive,
    /// Every row of the producer the merge names `first`, up to its natural
    /// EOF, before any row of its other producer.
    Concat,
}

/// A framework merge strategy, named in `merge!`'s `=>` slot.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a merge strategy",
    note = "use a strategy from `obzenflow::stages::merges`, for example `merges::then_live`.\nA merge runs no author code."
)]
pub trait MergeStrategy: sealed::Sealed + Copy + Send + Sync + 'static {
    /// Whether a merge under this strategy names its first producer, either
    /// [`FirstUnnamed`] or [`FirstNamed`].
    type FirstClause;

    /// The strategy the merge supervisor interprets.
    fn kind(self) -> MergeStrategyKind;
}

/// The first clause of a strategy that orders its producers by their sources.
pub struct FirstUnnamed;

/// The first clause of a strategy that reads the producer its merge names
/// `first` before the other.
pub struct FirstNamed;

impl sealed::Sealed for FirstUnnamed {}
impl sealed::Sealed for FirstNamed {}

/// Satisfied by strategies a merge declares without a `first` clause.
#[diagnostic::on_unimplemented(
    message = "merges::concat reads one producer first; name it, for example `merge!(first score: AlertRaised => merges::concat)`."
)]
pub trait WithoutFirstClause: sealed::Sealed {}

impl WithoutFirstClause for FirstUnnamed {}

/// Satisfied by strategies a merge declares with a `first` clause.
#[diagnostic::on_unimplemented(
    message = "merges::then_live orders producers by their sources and takes no first producer."
)]
pub trait WithFirstClause: sealed::Sealed {}

impl WithFirstClause for FirstNamed {}

/// Pass on every batch producer's rows, up to each one's natural EOF, before
/// any live producer's.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ThenLive;

impl sealed::Sealed for ThenLive {}

impl MergeStrategy for ThenLive {
    type FirstClause = FirstUnnamed;

    fn kind(self) -> MergeStrategyKind {
        MergeStrategyKind::ThenLive
    }
}

/// `merges::then_live`, the value named in `merge!(T => merges::then_live)`.
pub use ThenLive as then_live;

/// Pass on every row of the producer the merge names `first`, up to its
/// natural EOF, before any row of its other producer. Both producers are
/// finite.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Concat;

impl sealed::Sealed for Concat {}

impl MergeStrategy for Concat {
    type FirstClause = FirstNamed;

    fn kind(self) -> MergeStrategyKind {
        MergeStrategyKind::Concat
    }
}

/// `merges::concat`, the value named in
/// `merge!(first score: AlertRaised => merges::concat)`.
pub use Concat as concat;

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

    #[test]
    fn concat_names_the_concat_strategy() {
        assert_eq!(kind_of(concat), MergeStrategyKind::Concat);
    }
}
