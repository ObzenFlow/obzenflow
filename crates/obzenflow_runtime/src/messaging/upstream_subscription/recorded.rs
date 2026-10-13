// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::types::MergeWaitState;
use super::{PollResult, ReaderProgress, UpstreamSubscription};
use obzenflow_core::event::payloads::flow_control_payload::{FlowControlPayload, SelectedInput};
use obzenflow_core::event::{ChainEventFactory, ChainPayload, JournalEvent};
use obzenflow_core::journal::archive::{ReplayArchive, ReplayError};
use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_core::StageId;
use std::sync::Arc;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// One archived decision row as replay reads it back.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArchivedSelection {
    pub input: SelectedInput,
    pub delivered_count: u64,
    pub input_delivered_count: u64,
}

impl std::fmt::Display for ArchivedSelection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "decision row at delivered_count {} naming '{}' at input_delivered_count {}",
            self.delivered_count, self.input.stage_key, self.input_delivered_count
        )
    }
}

/// What a recorded fan-in does once it has followed every archived row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AfterRecord {
    /// Finish with the canonical merge, as a replay does.
    Canonical,
    /// Select by availability and record each selection, as a live run and a
    /// resume do.
    Record,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RecordedStep {
    Open { row: usize },
    Continue { row: usize },
    Canonical,
    Record,
}

#[derive(Clone, Copy, Debug)]
struct OpenSelection {
    row: usize,
    remaining: Option<u64>,
}

/// A fan-in's recorded-order state, the archived rows it still follows and
/// the input of its open selection.
#[derive(Clone, Debug)]
pub struct RecordedOrder {
    rows: Vec<ArchivedSelection>,
    next_row: usize,
    open: Option<OpenSelection>,
    after: AfterRecord,
    live_input: Option<SelectedInput>,
}

impl RecordedOrder {
    /// A live run's order, with no archived rows to follow.
    pub fn live() -> Self {
        Self::following(Vec::new(), AfterRecord::Record)
    }

    /// Follow `rows`, then continue as `after` says.
    pub fn following(rows: Vec<ArchivedSelection>, after: AfterRecord) -> Self {
        Self {
            rows,
            next_row: 0,
            open: None,
            after,
            live_input: None,
        }
    }

    pub(crate) fn row(&self, row: usize) -> &ArchivedSelection {
        &self.rows[row]
    }

    pub(crate) fn step(&mut self) -> RecordedStep {
        if self.open.is_some_and(|open| open.remaining == Some(0)) {
            self.open = None;
        }
        if let Some(open) = self.open {
            return RecordedStep::Continue { row: open.row };
        }
        if self.next_row < self.rows.len() {
            return RecordedStep::Open { row: self.next_row };
        }
        match self.after {
            AfterRecord::Canonical => RecordedStep::Canonical,
            AfterRecord::Record => RecordedStep::Record,
        }
    }

    pub(crate) fn opened(&mut self) {
        let row = self.next_row;
        let remaining = self
            .rows
            .get(row + 1)
            .map(|next| next.delivered_count - self.rows[row].delivered_count);
        self.open = Some(OpenSelection { row, remaining });
        self.next_row += 1;
    }

    pub(crate) fn followed(&mut self, ends_input: bool) {
        let Some(open) = self.open.as_mut() else {
            return;
        };
        match open.remaining.as_mut() {
            Some(remaining) => *remaining = remaining.saturating_sub(1),
            None if ends_input => self.open = None,
            None => {}
        }
    }

    pub(crate) fn opens_selection(&self, input: &SelectedInput) -> bool {
        self.live_input.as_ref() != Some(input)
    }

    pub(crate) fn selected(&mut self, input: SelectedInput) {
        self.live_input = Some(input);
    }
}

/// Read a fan-in's archived decision rows, oldest first. A torn resume's
/// lineage keeps the longest record and fails closed unless every shorter
/// record is its prefix.
pub async fn load_selection_record(
    archive: &Arc<dyn ReplayArchive>,
    stage_key: &str,
) -> Result<Vec<ArchivedSelection>, String> {
    let mut longest = read_selection_record(archive, stage_key).await?;
    for ancestor in archive.ancestors() {
        if matches!(
            ancestor.archived_stage_id(stage_key),
            Err(ReplayError::StageNotInManifest { .. })
        ) {
            continue;
        }
        let rows = read_selection_record(&ancestor, stage_key).await?;
        let shorter = rows.len().min(longest.len());
        if rows[..shorter] != longest[..shorter] {
            return Err(format!(
                "recorded fan-in '{stage_key}': the decision rows in {} disagree with \
                 those in {} at row {}; a resumed lineage must extend one record \
                 (FLOWIP-095q)",
                ancestor.archive_path().display(),
                archive.archive_path().display(),
                rows.iter()
                    .zip(&longest)
                    .position(|(left, right)| left != right)
                    .unwrap_or(shorter)
            ));
        }
        if rows.len() > longest.len() {
            longest = rows;
        }
    }
    Ok(longest)
}

async fn read_selection_record(
    archive: &Arc<dyn ReplayArchive>,
    stage_key: &str,
) -> Result<Vec<ArchivedSelection>, String> {
    let mut reader = archive
        .open_effect_history(stage_key)
        .await
        .map_err(|error| {
            format!("recorded fan-in '{stage_key}': cannot open its archived journal: {error}")
        })?;
    let mut rows: Vec<ArchivedSelection> = Vec::new();
    while let Some(record) = reader.next().await.map_err(|error| {
        format!("recorded fan-in '{stage_key}': cannot read its archived journal: {error}")
    })? {
        let ChainPayload::FlowControl(FlowControlPayload::InputSelection {
            input,
            delivered_count,
            input_delivered_count,
        }) = &record.payload
        else {
            continue;
        };
        let row = ArchivedSelection {
            input: input.clone(),
            delivered_count: delivered_count.0,
            input_delivered_count: input_delivered_count.0,
        };
        if let Some(previous) = rows.last() {
            if row.delivered_count <= previous.delivered_count {
                return Err(format!(
                    "recorded fan-in '{stage_key}': {row} does not follow {previous}; each \
                     selection covers at least one delivery (FLOWIP-095q)"
                ));
            }
        }
        rows.push(row);
    }
    Ok(rows)
}

pub(crate) fn selection_failure(owner: &str, row: &ArchivedSelection, problem: &str) -> BoxError {
    Box::new(JournalError::Implementation {
        message: format!(
            "recorded fan-in '{owner}': {row} {problem}; replay and resume follow the \
             recorded order and never fall back to availability (FLOWIP-095q)"
        ),
        source: "unfollowable decision row".into(),
    })
}

impl<T> UpstreamSubscription<T>
where
    T: JournalEvent + 'static,
{
    pub(super) async fn poll_recorded(
        &mut self,
        fsm_state: &str,
        mut reader_progress: Option<&mut [ReaderProgress]>,
    ) -> PollResult<T> {
        let step = self
            .recorded
            .as_mut()
            .expect("recorded subscription")
            .step();
        let (row, opening) = match step {
            RecordedStep::Open { row } => (row, true),
            RecordedStep::Continue { row } => (row, false),
            RecordedStep::Canonical => {
                return self.poll_kahn_merge(fsm_state, reader_progress).await;
            }
            RecordedStep::Record => return self.poll_record_live(fsm_state, reader_progress).await,
        };
        let archived = self
            .recorded
            .as_ref()
            .expect("recorded subscription")
            .row(row)
            .clone();
        let Some(index) = self.reader_for_input(&archived.input) else {
            return PollResult::Error(selection_failure(
                &self.owner_label,
                &archived,
                "names an input this stage does not read",
            ));
        };
        match self
            .ensure_named_head(index, fsm_state, &mut reader_progress)
            .await
        {
            Err(error) => return PollResult::Error(error),
            Ok(NamedHead::CursorAdvanced {
                upstream,
                completed_data_rows,
            }) => {
                return PollResult::CursorAdvanced {
                    upstream,
                    completed_data_rows,
                };
            }
            Ok(NamedHead::Exhausted) => {
                return PollResult::Error(selection_failure(
                    &self.owner_label,
                    &archived,
                    "names an input that is exhausted",
                ));
            }
            Ok(NamedHead::Waiting) => return PollResult::NoEvents,
            Ok(NamedHead::Held) => {}
        }
        if opening {
            if let Err(error) = self.check_archived_counts(index, &archived) {
                return PollResult::Error(error);
            }
            if let Err(error) = self
                .append_input_selection(
                    archived.input.clone(),
                    archived.delivered_count,
                    archived.input_delivered_count,
                )
                .await
            {
                return PollResult::Error(error);
            }
            self.recorded
                .as_mut()
                .expect("recorded subscription")
                .opened();
        }
        let ends_input = self.held_heads[index]
            .as_ref()
            .is_some_and(|head| head.ends_recorded_rows());
        let result = self.take_held_head(index, fsm_state, reader_progress);
        self.recorded
            .as_mut()
            .expect("recorded subscription")
            .followed(ends_input);
        result
    }

    async fn poll_record_live(
        &mut self,
        fsm_state: &str,
        mut reader_progress: Option<&mut [ReaderProgress]>,
    ) -> PollResult<T> {
        match self
            .acquire_available_heads(fsm_state, &mut reader_progress)
            .await
        {
            Err(error) => return PollResult::Error(error),
            Ok(Some((upstream, completed_data_rows))) => {
                return PollResult::CursorAdvanced {
                    upstream,
                    completed_data_rows,
                };
            }
            Ok(None) => {}
        }
        let waiting = self.pre_crossing_quiet_inputs();
        if !waiting.is_empty() {
            self.last_merge_wait = Some(MergeWaitState {
                quiet_inputs: waiting,
            });
            return PollResult::NoEvents;
        }
        self.last_merge_wait = None;
        let candidates = self.held_indices();
        if candidates.is_empty() {
            return PollResult::NoEvents;
        }
        let Some(index) = self.select_merge_winner(&candidates) else {
            return PollResult::NoEvents;
        };
        let input = self.selected_input(index);
        let opens = self
            .recorded
            .as_ref()
            .expect("recorded subscription")
            .opens_selection(&input);
        if opens {
            let delivered = self.delivered_total();
            let input_delivered = self.delivered_count_by_reader[index].0;
            if let Err(error) = self
                .append_input_selection(input.clone(), delivered, input_delivered)
                .await
            {
                return PollResult::Error(error);
            }
            self.recorded
                .as_mut()
                .expect("recorded subscription")
                .selected(input);
        }
        self.take_held_head(index, fsm_state, reader_progress)
    }

    pub(crate) async fn acquire_available_heads(
        &mut self,
        fsm_state: &str,
        reader_progress: &mut Option<&mut [ReaderProgress]>,
    ) -> Result<Option<(StageId, u64)>, BoxError> {
        loop {
            let held_before = self.held_head_count();
            for index in 0..self.readers.len() {
                if self.state.is_reader_eof(index) || self.held_heads[index].is_some() {
                    continue;
                }
                if let Some(advanced) = self.acquire_head(index, fsm_state, reader_progress).await?
                {
                    self.merge_candidate_index = None;
                    return Ok(Some(advanced));
                }
            }
            if self.held_head_count() == held_before {
                return Ok(None);
            }
        }
    }

    pub(crate) async fn ensure_named_head(
        &mut self,
        index: usize,
        fsm_state: &str,
        reader_progress: &mut Option<&mut [ReaderProgress]>,
    ) -> Result<NamedHead, BoxError> {
        if self.held_heads[index].is_none() {
            if self.state.is_reader_eof(index) {
                return Ok(NamedHead::Exhausted);
            }
            if let Some((upstream, completed_data_rows)) =
                self.acquire_head(index, fsm_state, reader_progress).await?
            {
                return Ok(NamedHead::CursorAdvanced {
                    upstream,
                    completed_data_rows,
                });
            }
        }
        if self.held_heads[index].is_none() {
            let slot = &self.readers[index];
            self.last_merge_wait = Some(MergeWaitState {
                quiet_inputs: vec![(slot.stage_id, slot.stage_key.to_string())],
            });
            return Ok(NamedHead::Waiting);
        }
        self.last_merge_wait = None;
        Ok(NamedHead::Held)
    }

    pub(crate) fn pre_crossing_quiet_inputs(&self) -> Vec<(StageId, String)> {
        (0..self.readers.len())
            .filter(|&index| {
                !self.state.is_reader_eof(index)
                    && self.held_heads[index].is_none()
                    && self.generation_by_reader[index] < self.entered_generation
            })
            .map(|index| {
                let slot = &self.readers[index];
                (slot.stage_id, slot.stage_key.to_string())
            })
            .collect()
    }

    pub(crate) fn held_indices(&self) -> Vec<usize> {
        (0..self.held_heads.len())
            .filter(|&index| self.held_heads[index].is_some())
            .collect()
    }

    pub(crate) fn held_head_ends_recorded_rows(&self, index: usize) -> bool {
        self.held_heads
            .get(index)
            .and_then(Option::as_ref)
            .is_some_and(|head| head.ends_recorded_rows())
    }

    pub(crate) fn take_held_head(
        &mut self,
        index: usize,
        fsm_state: &str,
        reader_progress: Option<&mut [ReaderProgress]>,
    ) -> PollResult<T> {
        self.merge_candidate_index = None;
        let Some(head) = self.held_heads.get_mut(index).and_then(Option::take) else {
            return PollResult::NoEvents;
        };
        self.deliver_from_reader(index, head, fsm_state, reader_progress, false)
    }

    pub(crate) fn reader_for_input(&self, input: &SelectedInput) -> Option<usize> {
        (0..self.readers.len()).find(|&index| self.selected_input(index) == *input)
    }

    pub(crate) fn selected_input(&self, index: usize) -> SelectedInput {
        self.reader_tiebreak_keys[index].selected_input()
    }

    pub(crate) fn delivered_count_of(&self, index: usize) -> u64 {
        self.delivered_count_by_reader[index].0
    }

    pub(crate) fn check_archived_counts(
        &self,
        index: usize,
        archived: &ArchivedSelection,
    ) -> Result<(), BoxError> {
        self.check_archived_counts_at(self.delivered_total(), index, archived)
    }

    pub(crate) fn check_archived_counts_at(
        &self,
        delivered_total: u64,
        index: usize,
        archived: &ArchivedSelection,
    ) -> Result<(), BoxError> {
        if delivered_total != archived.delivered_count {
            return Err(selection_failure(
                &self.owner_label,
                archived,
                &format!("opens at delivered_count {delivered_total} in this run"),
            ));
        }
        let input_delivered = self.delivered_count_of(index);
        if input_delivered != archived.input_delivered_count {
            return Err(selection_failure(
                &self.owner_label,
                archived,
                &format!("finds that input at input_delivered_count {input_delivered}"),
            ));
        }
        Ok(())
    }

    pub(crate) async fn append_input_selection(
        &self,
        input: SelectedInput,
        delivered_count: u64,
        input_delivered_count: u64,
    ) -> Result<(), BoxError> {
        let tracker = self.contract_tracker.as_ref().ok_or_else(|| {
            format!(
                "recorded fan-in '{}' has no stage journal to record its selections",
                self.owner_label
            )
        })?;
        let event = tracker.with_owner_context(ChainEventFactory::input_selected_event(
            tracker.writer_id,
            input,
            delivered_count,
            input_delivered_count,
        ));
        crate::supervised_base::publication::append(&tracker.journal, event, Default::default())
            .await?;
        Ok(())
    }
}

pub(crate) enum NamedHead {
    Held,
    Waiting,
    Exhausted,
    CursorAdvanced {
        upstream: StageId,
        completed_data_rows: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::id::StageKey;

    fn input(key: &str) -> SelectedInput {
        SelectedInput {
            stage_key: StageKey(key.to_string()),
            feed: Vec::new(),
        }
    }

    fn row(key: &str, delivered_count: u64, input_delivered_count: u64) -> ArchivedSelection {
        ArchivedSelection {
            input: input(key),
            delivered_count,
            input_delivered_count,
        }
    }

    #[test]
    fn follows_each_selection_for_the_length_the_next_row_implies() {
        let mut order = RecordedOrder::following(
            vec![row("tx", 0, 0), row("accounts", 2, 0), row("tx", 3, 2)],
            AfterRecord::Canonical,
        );
        assert_eq!(order.step(), RecordedStep::Open { row: 0 });
        order.opened();
        order.followed(false);
        assert_eq!(order.step(), RecordedStep::Continue { row: 0 });
        order.followed(false);
        assert_eq!(order.step(), RecordedStep::Open { row: 1 });
        order.opened();
        order.followed(false);
        assert_eq!(order.step(), RecordedStep::Open { row: 2 });
        order.opened();
        order.followed(false);
        assert_eq!(
            order.step(),
            RecordedStep::Continue { row: 2 },
            "the last row's tail continues past any count"
        );
        order.followed(true);
        assert_eq!(order.step(), RecordedStep::Canonical);
    }

    #[test]
    fn a_resume_records_a_new_selection_after_the_tail_even_on_the_same_input() {
        let mut order = RecordedOrder::following(vec![row("tx", 0, 0)], AfterRecord::Record);
        assert_eq!(order.step(), RecordedStep::Open { row: 0 });
        order.opened();
        order.followed(true);
        assert_eq!(order.step(), RecordedStep::Record);
        assert!(order.opens_selection(&input("tx")));
        order.selected(input("tx"));
        assert!(!order.opens_selection(&input("tx")));
        assert!(order.opens_selection(&input("accounts")));
    }

    #[test]
    fn a_live_run_records_its_first_selection() {
        let mut order = RecordedOrder::live();
        assert_eq!(order.step(), RecordedStep::Record);
        assert!(order.opens_selection(&input("tx")));
    }
}
