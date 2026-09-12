// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Execution-local snapshot selection and tail-search knowledge (FLOWIP-130b).
//! Neither is evidence that the sequential collector has covered a journal.

use super::fsm::MetricsJournalKind;
use obzenflow_core::event::context::RuntimeContext;
use obzenflow_core::journal::JournalError;
use obzenflow_core::{ChainEvent, EventEnvelope, EventId, Journal, JournalId, StageId, WriterId};

pub(super) const SEARCH_WINDOWS: [usize; 8] = [1, 5, 20, 100, 500, 2_000, 10_000, 50_000];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct JournalBinding {
    pub(crate) journal: JournalId,
    pub(crate) stage: StageId,
    pub(crate) kind: MetricsJournalKind,
}

/// Equality within one physical journal only. Mixed writers have no total
/// vector-clock order, and forwarding/replay can repeat an EventId.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RecordIdentity {
    event: EventId,
    writer: WriterId,
    sequence: u64,
}

impl RecordIdentity {
    fn of(row: &EventEnvelope<ChainEvent>) -> Result<Self, JournalError> {
        let writer = row.event.writer_id;
        let sequence = row
            .vector_clock
            .clocks
            .get(&writer.to_string())
            .copied()
            .ok_or_else(|| JournalError::Implementation {
                message: "Metrics observation lacks its journal writer component".into(),
                source: Box::new(std::io::Error::other("missing writer sequence")),
            })?;
        Ok(Self {
            event: row.event.id,
            writer,
            sequence,
        })
    }
}

struct SnapshotFact {
    record: RecordIdentity,
    context: RuntimeContext,
}

#[derive(Default)]
enum SnapshotSelection {
    #[default]
    Unseen,
    Folded(Box<SnapshotFact>),
    AheadOfFold(Box<SnapshotFact>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LookupOutcome {
    Snapshot,
    Absent,
    Capped,
}

struct TailSearch {
    // None is observed empty, distinct from no TailSearch (unchecked).
    head: Option<RecordIdentity>,
    outcome: LookupOutcome,
}

#[derive(Default)]
pub(crate) struct SnapshotObservation {
    search: Option<TailSearch>,
    selection: SnapshotSelection,
}

impl SnapshotObservation {
    pub(crate) fn selected(&self) -> Option<&RuntimeContext> {
        match &self.selection {
            SnapshotSelection::Unseen => None,
            SnapshotSelection::Folded(fact) | SnapshotSelection::AheadOfFold(fact) => {
                Some(&fact.context)
            }
        }
    }

    pub(crate) fn is_ahead_of_fold(&self) -> bool {
        matches!(self.selection, SnapshotSelection::AheadOfFold(_))
    }

    pub(crate) fn fold(
        &mut self,
        row: &EventEnvelope<ChainEvent>,
        stage: StageId,
    ) -> Result<(), JournalError> {
        if row.event.flow_context.stage_id != stage {
            return Ok(());
        }
        let Some(context) = &row.event.runtime_context else {
            return Ok(());
        };
        let record = RecordIdentity::of(row)?;
        match &mut self.selection {
            SnapshotSelection::AheadOfFold(fact) if fact.record != record => {}
            SnapshotSelection::AheadOfFold(_) => {
                let SnapshotSelection::AheadOfFold(fact) = std::mem::take(&mut self.selection)
                else {
                    unreachable!()
                };
                self.selection = SnapshotSelection::Folded(fact);
            }
            _ => {
                self.selection = SnapshotSelection::Folded(Box::new(SnapshotFact {
                    record,
                    context: context.clone(),
                }));
            }
        }
        Ok(())
    }

    /// Search results are committed only after a successful examined window.
    /// A cached boundary or fallback never reselects an older snapshot.
    pub(crate) async fn refresh(
        &mut self,
        journal: &dyn Journal<ChainEvent>,
        stage: StageId,
    ) -> Result<(), JournalError> {
        let mut rows = journal.read_last_n(1).await?;
        let head = rows.first().map(RecordIdentity::of).transpose()?;
        if self
            .search
            .as_ref()
            .is_some_and(|search| search.head == head)
        {
            return Ok(());
        }
        for (index, count) in SEARCH_WINDOWS.into_iter().enumerate() {
            if index != 0 {
                rows = journal.read_last_n(count).await?;
            }
            let head = rows.first().map(RecordIdentity::of).transpose()?;
            for row in &rows {
                let record = RecordIdentity::of(row)?;
                if self
                    .search
                    .as_ref()
                    .is_some_and(|search| search.head == Some(record))
                {
                    let outcome = self
                        .search
                        .as_ref()
                        .expect("matched search boundary")
                        .outcome;
                    self.search = Some(TailSearch { head, outcome });
                    return Ok(());
                }
                if row.event.flow_context.stage_id == stage {
                    if let Some(context) = &row.event.runtime_context {
                        // With serial folding and append-order journals, this fresh
                        // newest snapshot is either the latest fold or ahead of it.
                        let folded = matches!(&self.selection, SnapshotSelection::Folded(fact) if fact.record == record);
                        let fact = Box::new(SnapshotFact {
                            record,
                            context: context.clone(),
                        });
                        self.selection = if folded {
                            SnapshotSelection::Folded(fact)
                        } else {
                            SnapshotSelection::AheadOfFold(fact)
                        };
                        self.search = Some(TailSearch {
                            head,
                            outcome: LookupOutcome::Snapshot,
                        });
                        return Ok(());
                    }
                }
            }
            if rows.len() < count {
                self.search = Some(TailSearch {
                    head,
                    outcome: LookupOutcome::Absent,
                });
                return Ok(());
            }
            if index == SEARCH_WINDOWS.len() - 1 {
                self.search = Some(TailSearch {
                    head,
                    outcome: LookupOutcome::Capped,
                });
            }
        }
        Ok(())
    }

    #[cfg(feature = "test-support")]
    pub(crate) fn lookup_outcome(&self) -> Option<LookupOutcome> {
        self.search.as_ref().map(|search| search.outcome)
    }
}
