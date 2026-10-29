// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::vector_clock::{CausalOrderingService, VectorClock};
use obzenflow_core::id::JournalId;
use obzenflow_core::EventId;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

struct Held<T> {
    rank: u128,
    event: EventId,
    sequence: u64,
    clock: VectorClock,
    item: T,
}

pub(super) struct CausalMerge<T> {
    run_journals: BTreeSet<JournalId>,
    held: BTreeMap<JournalId, VecDeque<Held<T>>>,
    printed: BTreeMap<JournalId, u64>,
}

impl<T> CausalMerge<T> {
    pub(super) fn new(run_journals: impl IntoIterator<Item = JournalId>) -> Self {
        Self {
            run_journals: run_journals.into_iter().collect(),
            held: BTreeMap::new(),
            printed: BTreeMap::new(),
        }
    }

    pub(super) fn holds(&self, journal: &JournalId) -> bool {
        self.held.get(journal).is_some_and(|held| !held.is_empty())
    }

    pub(super) fn is_empty(&self) -> bool {
        self.held.values().all(VecDeque::is_empty)
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.held.values().map(VecDeque::len).sum()
    }

    pub(super) fn push(
        &mut self,
        journal: JournalId,
        sequence: u64,
        clock: VectorClock,
        event: EventId,
        item: T,
    ) {
        let rank = CausalOrderingService::causal_rank(&clock);
        self.held.entry(journal).or_default().push_back(Held {
            rank,
            event,
            sequence,
            clock,
            item,
        });
    }

    pub(super) fn lowest(&mut self) -> Option<T> {
        self.take(false)
    }

    pub(super) fn lowest_ready(&mut self) -> Option<T> {
        self.take(true)
    }

    fn take(&mut self, ready_only: bool) -> Option<T> {
        let journal = self
            .held
            .iter()
            .filter_map(|(journal, held)| held.front().map(|front| (journal, front)))
            .filter(|(journal, front)| !ready_only || self.ready(journal, front))
            .min_by(|(_, a), (_, b)| a.rank.cmp(&b.rank).then_with(|| a.event.cmp(&b.event)))
            .map(|(journal, _)| *journal)?;
        let held = self.held.get_mut(&journal)?.pop_front()?;
        self.printed.insert(journal, held.sequence);
        Some(held.item)
    }

    fn ready(&self, own: &JournalId, held: &Held<T>) -> bool {
        held.clock.clocks.iter().all(|(coordinate, count)| {
            let journal = coordinate.journal_writer_id.as_journal_id();
            journal == own
                || !self.run_journals.contains(journal)
                || self
                    .printed
                    .get(journal)
                    .is_some_and(|printed| printed >= count)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::CausalCoordinate;
    use obzenflow_core::JournalWriterId;

    struct Rng(u64);

    impl Rng {
        fn below(&mut self, bound: usize) -> usize {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            (self.0 % bound as u64) as usize
        }
    }

    struct Record {
        journal: usize,
        sequence: u64,
        inputs: Vec<usize>,
        clock: VectorClock,
        event: EventId,
    }

    fn coordinate(journal: JournalId) -> CausalCoordinate {
        CausalCoordinate::new(JournalWriterId::from(journal))
    }

    fn causal_graph(rng: &mut Rng, journals: &[JournalId], records: usize) -> Vec<Record> {
        let mut graph: Vec<Record> = Vec::new();
        let mut last: Vec<Option<usize>> = vec![None; journals.len()];
        for _ in 0..records {
            let journal = rng.below(journals.len());
            let inputs: Vec<usize> = (0..rng.below(3))
                .filter(|_| !graph.is_empty())
                .map(|_| rng.below(graph.len()))
                .filter(|input| graph[*input].journal != journal)
                .collect();
            let mut clock = last[journal]
                .map_or_else(VectorClock::new, |previous| graph[previous].clock.clone());
            for input in &inputs {
                CausalOrderingService::update_with_parent(&mut clock, &graph[*input].clock);
            }
            let sequence = last[journal].map_or(0, |previous| graph[previous].sequence) + 1;
            clock.clocks.insert(coordinate(journals[journal]), sequence);
            last[journal] = Some(graph.len());
            graph.push(Record {
                journal,
                sequence,
                inputs,
                clock,
                event: EventId::new(),
            });
        }
        graph
    }

    fn per_journal(graph: &[Record], journals: usize) -> Vec<VecDeque<usize>> {
        let mut queues = vec![VecDeque::new(); journals];
        for (index, record) in graph.iter().enumerate() {
            queues[record.journal].push_back(index);
        }
        queues
    }

    fn assert_causal(graph: &[Record], order: &[usize]) {
        assert_eq!(order.len(), graph.len());
        let position: BTreeMap<usize, usize> = order
            .iter()
            .enumerate()
            .map(|(at, record)| (*record, at))
            .collect();
        for (index, record) in graph.iter().enumerate() {
            for input in &record.inputs {
                assert!(position[input] < position[&index], "input after its output");
            }
            if let Some(previous) = graph[..index]
                .iter()
                .rposition(|earlier| earlier.journal == record.journal)
            {
                assert!(
                    position[&previous] < position[&index],
                    "journal order broken"
                );
            }
        }
    }

    fn push(
        merge: &mut CausalMerge<usize>,
        journals: &[JournalId],
        graph: &[Record],
        index: usize,
    ) {
        let record = &graph[index];
        merge.push(
            journals[record.journal],
            record.sequence,
            record.clock.clone(),
            record.event,
            index,
        );
    }

    #[test]
    fn an_archive_lists_in_rank_order_after_everything_each_clock_counts() {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        for _ in 0..200 {
            let journals: Vec<_> = (0..2 + rng.below(5)).map(|_| JournalId::new()).collect();
            let records = 1 + rng.below(60);
            let graph = causal_graph(&mut rng, &journals, records);
            let mut queues = per_journal(&graph, journals.len());
            let mut merge = CausalMerge::new(journals.iter().copied());
            for queue in &mut queues {
                if let Some(first) = queue.pop_front() {
                    push(&mut merge, &journals, &graph, first);
                }
            }
            let mut order = Vec::new();
            while let Some(index) = merge.lowest() {
                order.push(index);
                if let Some(next) = queues[graph[index].journal].pop_front() {
                    push(&mut merge, &journals, &graph, next);
                }
            }
            assert_causal(&graph, &order);
            let mut ranked: Vec<usize> = (0..graph.len()).collect();
            ranked.sort_by(|a, b| {
                CausalOrderingService::total_compare_by_event_id(
                    &graph[*a].clock,
                    &graph[*a].event,
                    &graph[*b].clock,
                    &graph[*b].event,
                )
            });
            assert_eq!(order, ranked);
        }
    }

    #[test]
    fn a_live_listing_stays_causal_for_any_arrival_order() {
        let mut rng = Rng(0x2545_F491_4F6C_DD1D);
        for _ in 0..200 {
            let journals: Vec<_> = (0..2 + rng.below(5)).map(|_| JournalId::new()).collect();
            let records = 1 + rng.below(60);
            let graph = causal_graph(&mut rng, &journals, records);
            let mut queues = per_journal(&graph, journals.len());
            let mut merge = CausalMerge::new(journals.iter().copied());
            let mut order = Vec::new();
            while queues.iter().any(|queue| !queue.is_empty()) {
                let journal = rng.below(journals.len());
                if !merge.holds(&journals[journal]) {
                    if let Some(next) = queues[journal].pop_front() {
                        push(&mut merge, &journals, &graph, next);
                    }
                }
                order.extend(merge.lowest_ready());
            }
            order.extend(std::iter::from_fn(|| merge.lowest()));
            assert_causal(&graph, &order);
            assert!(merge.is_empty());
        }
    }

    #[test]
    fn a_held_record_waits_only_for_journals_of_this_run() {
        let (shown, earlier) = (JournalId::new(), JournalId::new());
        let mut clock = VectorClock::new();
        clock.clocks.insert(coordinate(shown), 3);
        clock.clocks.insert(coordinate(earlier), 9);
        let mut merge = CausalMerge::new([shown]);
        merge.push(JournalId::new(), 1, clock, EventId::new(), "waits");
        assert_eq!(merge.lowest_ready(), None);
        let mut own = VectorClock::new();
        own.clocks.insert(coordinate(shown), 3);
        merge.push(shown, 3, own, EventId::new(), "shown");
        assert_eq!(merge.lowest_ready(), Some("shown"));
        assert_eq!(merge.lowest_ready(), Some("waits"));
        assert!(merge.is_empty());
    }
}
