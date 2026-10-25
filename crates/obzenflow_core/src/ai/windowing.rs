// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fmt;
use std::num::NonZeroU64;

/// Zero-based position of a group among its key's groups, in the stage's
/// input order.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct GroupOrdinal(u64);

impl GroupOrdinal {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl fmt::Display for GroupOrdinal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Zero-based firing ordinal of a reduce window among its key's windows.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct WindowOrdinal(u64);

impl WindowOrdinal {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for WindowOrdinal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// When a map-reduce stage reduces each key's partials.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ReduceTrigger {
    /// Once per key, at a natural end of input.
    OnEof,
    /// Once per `chunks` of a key's groups, and for the remainder at a
    /// natural end of input.
    EveryNChunks { chunks: NonZeroU64 },
}

impl ReduceTrigger {
    /// The window a group belongs to and its slot within that window.
    pub fn place(self, group: GroupOrdinal) -> (WindowOrdinal, u64) {
        match self {
            Self::OnEof => (WindowOrdinal::new(0), group.get()),
            Self::EveryNChunks { chunks } => (
                WindowOrdinal::new(group.get() / chunks.get()),
                group.get() % chunks.get(),
            ),
        }
    }

    /// The number of slots a full window holds, or `None` when only the end
    /// of input closes it.
    pub fn slots(self) -> Option<NonZeroU64> {
        match self {
            Self::OnEof => None,
            Self::EveryNChunks { chunks } => Some(chunks),
        }
    }

    /// The first group ordinal of a window.
    pub fn first_group(self, window: WindowOrdinal) -> GroupOrdinal {
        match self {
            Self::OnEof => GroupOrdinal::new(0),
            Self::EveryNChunks { chunks } => {
                GroupOrdinal::new(window.get().saturating_mul(chunks.get()))
            }
        }
    }
}

/// What closed a group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupClosedBy {
    /// The group reached its chunk size.
    ChunkSize,
    /// A natural end of input flushed the group.
    EndOfInput,
}

/// A key that groups an `ai::map` stage's items. Each key forms, closes and
/// reduces its own groups.
pub trait AiKey: Ord + Clone + Serialize + DeserializeOwned + Send + Sync + 'static {}

impl<T> AiKey for T where T: Ord + Clone + Serialize + DeserializeOwned + Send + Sync + 'static {}

/// The items of one group, which a map role turns into one request.
#[derive(Debug, Clone, PartialEq)]
pub struct Chunk<T> {
    group: GroupOrdinal,
    first_number: u64,
    items: Vec<T>,
}

impl<T> Chunk<T> {
    /// A chunk whose first item carries citation number `first_number`.
    pub fn new(group: GroupOrdinal, first_number: u64, items: Vec<T>) -> Self {
        Self {
            group,
            first_number,
            items,
        }
    }

    pub fn group(&self) -> GroupOrdinal {
        self.group
    }

    pub fn items(&self) -> &[T] {
        &self.items
    }

    pub fn into_items(self) -> Vec<T> {
        self.items
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        self.items.iter()
    }

    /// Each item with its citation number. Numbers start at 1 in each reduce
    /// window and continue across its chunks, matching [`Partials::numbered`].
    pub fn numbered(&self) -> impl Iterator<Item = (u64, &T)> + '_ {
        self.items
            .iter()
            .enumerate()
            .map(|(index, item)| (self.first_number + index as u64, item))
    }
}

impl<'a, T> IntoIterator for &'a Chunk<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.iter()
    }
}

/// One reduce window, with its items in input order and each group's partial
/// in group order.
#[derive(Debug, Clone, PartialEq)]
pub struct Partials<T, P> {
    window: WindowOrdinal,
    items: Vec<T>,
    partials: Vec<P>,
}

impl<T, P> Partials<T, P> {
    pub fn new(window: WindowOrdinal, items: Vec<T>, partials: Vec<P>) -> Self {
        Self {
            window,
            items,
            partials,
        }
    }

    pub fn window(&self) -> WindowOrdinal {
        self.window
    }

    pub fn items(&self) -> &[T] {
        &self.items
    }

    pub fn partials(&self) -> &[P] {
        &self.partials
    }

    /// Each item with the citation number its chunk gave it.
    pub fn numbered(&self) -> impl Iterator<Item = (u64, &T)> + '_ {
        self.items
            .iter()
            .enumerate()
            .map(|(index, item)| (index as u64 + 1, item))
    }

    pub fn into_parts(self) -> (Vec<T>, Vec<P>) {
        (self.items, self.partials)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn every(chunks: u64) -> ReduceTrigger {
        ReduceTrigger::EveryNChunks {
            chunks: NonZeroU64::new(chunks).unwrap(),
        }
    }

    #[test]
    fn rolling_trigger_places_groups_by_division() {
        assert_eq!(
            every(2).place(GroupOrdinal::new(0)),
            (WindowOrdinal::new(0), 0)
        );
        assert_eq!(
            every(2).place(GroupOrdinal::new(3)),
            (WindowOrdinal::new(1), 1)
        );
        assert_eq!(
            every(2).first_group(WindowOrdinal::new(2)),
            GroupOrdinal::new(4)
        );
        assert_eq!(
            ReduceTrigger::OnEof.place(GroupOrdinal::new(7)),
            (WindowOrdinal::new(0), 7)
        );
    }

    #[test]
    fn chunk_numbers_continue_from_the_window_offset() {
        let chunk = Chunk::new(GroupOrdinal::new(1), 4, vec!["d", "e"]);
        let numbered: Vec<_> = chunk.numbered().collect();
        assert_eq!(numbered, vec![(4, &"d"), (5, &"e")]);

        let partials = Partials::new(WindowOrdinal::new(0), vec!["a", "b"], vec![1_u8]);
        assert_eq!(
            partials.numbered().collect::<Vec<_>>(),
            vec![(1, &"a"), (2, &"b")]
        );
    }

    #[test]
    fn trigger_wire_shape_names_its_unit() {
        assert_eq!(
            serde_json::to_value(every(6)).unwrap(),
            serde_json::json!({"kind": "every_n_chunks", "chunks": 6})
        );
        assert_eq!(
            serde_json::to_value(ReduceTrigger::OnEof).unwrap(),
            serde_json::json!({"kind": "on_eof"})
        );
    }
}
