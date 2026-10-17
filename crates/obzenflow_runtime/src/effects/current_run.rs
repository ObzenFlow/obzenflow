// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::history::{
    validate_abandonment_event, validate_atomic_group_members, validate_attempt_event,
    validate_cursor_history,
};
use super::*;
use obzenflow_core::event::provenance::JournalGroupMember;
use obzenflow_core::event::{CausalError, CausalFrontier, JournalRecord};
use obzenflow_core::journal::JournalReader;
use std::collections::{BTreeMap, HashSet};

const OUTCOME_GROUP_PREFIX: &str = "effect-outcome:v1:";
const ESCAPE_CONTROLS_GROUP_PREFIX: &str = "effect-escape-controls:v1:";

/// The current run's effect history for one stage, folded once, in append
/// order, from the data journal that stage appends to.
#[derive(Clone)]
pub struct CurrentRunHistory {
    journal: Arc<dyn Journal<ChainEvent>>,
    fold: Arc<tokio::sync::Mutex<Fold>>,
}

impl CurrentRunHistory {
    /// The history of `journal`, which must be the data journal of the stage
    /// whose effect invocations consult it.
    pub fn new(journal: Arc<dyn Journal<ChainEvent>>) -> Self {
        Self {
            journal,
            fold: Arc::new(tokio::sync::Mutex::new(Fold::default())),
        }
    }

    pub(crate) async fn cursor_history(
        &self,
        journal: &Arc<dyn Journal<ChainEvent>>,
        cursor: &EffectCursor,
    ) -> Result<EffectCursorHistory, CurrentRunFailure> {
        if !Arc::ptr_eq(journal, &self.journal) {
            return Err(CurrentRunFailure::History(
                EffectError::EffectProvenanceMismatch(format!(
                    "stage '{}' consulted current-run history folded from another journal",
                    cursor.stage_key
                )),
            ));
        }
        self.fold.lock().await.lookup(&self.journal, cursor).await
    }

    #[cfg(test)]
    pub(crate) async fn retained_cursors(&self) -> usize {
        self.fold.lock().await.folded.cursors.len()
    }
}

impl std::fmt::Debug for CurrentRunHistory {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CurrentRunHistory")
            .field("journal", self.journal.id())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
pub(crate) enum CurrentRunFailure {
    Journal(EffectError),
    History(EffectError),
}

#[derive(Default)]
struct Fold {
    reader: Option<Box<dyn JournalReader<ChainEvent>>>,
    folded: Folded,
}

impl Fold {
    async fn lookup(
        &mut self,
        journal: &Arc<dyn Journal<ChainEvent>>,
        cursor: &EffectCursor,
    ) -> Result<EffectCursorHistory, CurrentRunFailure> {
        self.folded.refuse_after_failure()?;
        self.folded.pin(cursor)?;
        self.read_to_end(journal).await?;
        self.folded.settle_before(cursor)?;
        self.folded.cursor_history(cursor)
    }

    async fn read_to_end(
        &mut self,
        journal: &Arc<dyn Journal<ChainEvent>>,
    ) -> Result<(), CurrentRunFailure> {
        let reader = match self.reader.take() {
            Some(reader) => reader,
            None => journal.reader().await.map_err(journal_failure)?,
        };
        let reader = self.reader.insert(reader);
        let mut unconfirmed_end = false;
        loop {
            match reader.next().await.map_err(journal_failure)? {
                Some(record) => {
                    unconfirmed_end = false;
                    self.folded.fold(journal, record)?;
                }
                None if reader.is_at_end() => break,
                None if !unconfirmed_end => unconfirmed_end = true,
                None => {
                    return Err(CurrentRunFailure::Journal(EffectError::Journal(format!(
                        "stage journal {} holds an uncommitted record after its first {} records",
                        journal.id(),
                        reader.position()
                    ))))
                }
            }
        }
        self.folded.close_incomplete_groups()
    }
}

#[derive(Default)]
struct Folded {
    position: usize,
    stage: Option<(RecordedFlowId, EffectStageKey)>,
    settled: Option<u64>,
    cursors: HashMap<EffectCursor, CursorFold>,
    groups: HashMap<String, PendingGroup>,
    failure: Option<CurrentRunFailure>,
}

#[derive(Default)]
struct CursorFold {
    frontiers: Vec<CausalFrontier>,
    records: Vec<(Option<EffectAttemptOrdinal>, EffectRecord)>,
    starts: Vec<(usize, EffectAttemptStarted, ChainEvent)>,
    abandonments: Vec<(ChainEvent, EffectRecoveryAbandoned)>,
    terminal: Option<FoldedGroup>,
    escape_controls: BTreeMap<EffectAttemptOrdinal, FoldedGroup>,
}

#[derive(Default)]
struct PendingGroup {
    members: Vec<(usize, Option<JournalGroupMember>, ChainEvent)>,
    causal: CausalFrontier,
}

struct FoldedGroup {
    position: usize,
    events: Vec<ChainEvent>,
    causal: CausalFrontier,
}

enum GroupCursor {
    Outcome(EffectCursor),
    EscapeControls(EffectCursor, EffectAttemptOrdinal),
}

enum Claim {
    Retain,
    Ignore,
}

impl Folded {
    fn refuse_after_failure(&self) -> Result<(), CurrentRunFailure> {
        match &self.failure {
            Some(failure) => Err(failure.clone()),
            None => Ok(()),
        }
    }

    fn fail(&mut self, failure: CurrentRunFailure) -> CurrentRunFailure {
        self.failure.get_or_insert(failure).clone()
    }

    fn pin(&mut self, cursor: &EffectCursor) -> Result<(), CurrentRunFailure> {
        let refusal = match &self.stage {
            None => {
                self.stage = Some((cursor.recorded_flow_id.clone(), cursor.stage_key.clone()));
                return Ok(());
            }
            Some((namespace, stage))
                if *namespace == cursor.recorded_flow_id && *stage == cursor.stage_key =>
            {
                return Ok(());
            }
            Some((namespace, stage)) => format!(
                "current-run history of stage '{stage}' in namespace `{namespace}` cannot answer for cursor {cursor:?}"
            ),
        };
        Err(self.fail(history_failure(refusal)))
    }

    fn settle_before(&mut self, cursor: &EffectCursor) -> Result<(), CurrentRunFailure> {
        let input = cursor.input_seq.get();
        if let Some(settled) = self.settled.filter(|settled| input <= *settled) {
            let refusal = format!(
                "stage '{}' looked up input {input} after settling every input through {settled}",
                cursor.stage_key
            );
            return Err(self.fail(history_failure(refusal)));
        }
        if let Some(previous) = input.checked_sub(1) {
            if self.settled.is_none_or(|settled| previous > settled) {
                self.settled = Some(previous);
                self.cursors
                    .retain(|retained, _| retained.input_seq.get() > previous);
            }
        }
        Ok(())
    }

    fn fold(
        &mut self,
        journal: &Arc<dyn Journal<ChainEvent>>,
        record: JournalRecord<ChainPayload>,
    ) -> Result<(), CurrentRunFailure> {
        let position = self.position;
        self.position = self.position.saturating_add(1);
        self.fold_record(journal, position, record)
            .map_err(|failure| self.fail(failure))
    }

    fn fold_record(
        &mut self,
        journal: &Arc<dyn Journal<ChainEvent>>,
        position: usize,
        record: JournalRecord<ChainPayload>,
    ) -> Result<(), CurrentRunFailure> {
        if record.causal_coordinate().journal_writer_id.as_journal_id() != journal.id() {
            return Err(journal_failure(CausalError::ConflictingCommitment));
        }
        let event = record.authored();
        if let Some(provenance) = &event.effect_provenance {
            let frontier = CausalFrontier::from_record(&record).map_err(journal_failure)?;
            if let Claim::Retain =
                self.claim(&provenance.cursor, provenance.fact_owner.is_framework())?
            {
                self.cursors
                    .entry(provenance.cursor.clone())
                    .or_default()
                    .frontiers
                    .push(frontier);
            }
        }
        if let Some(group_id) = &record.envelope.provenance.journal.journal_group_id {
            let frontier = CausalFrontier::from_record(&record).map_err(journal_failure)?;
            self.join_group(
                group_id,
                position,
                record.envelope.provenance.journal.journal_group_member,
                event.clone(),
                &frontier,
            )?;
        }
        if EffectAttemptStarted::matches_event_type(&event.event_type()) {
            return self.fold_start(position, event);
        }
        if EffectRecoveryAbandoned::matches_event_type(&event.event_type()) {
            let abandoned = EffectRecoveryAbandoned::try_from_event(&event).map_err(|error| {
                CurrentRunFailure::History(EffectError::Serialization(error.to_string()))
            })?;
            if let Claim::Retain = self.claim(&abandoned.cursor, true)? {
                self.cursors
                    .entry(abandoned.cursor.clone())
                    .or_default()
                    .abandonments
                    .push((event, abandoned));
            }
            return Ok(());
        }
        if let Some(effect_record) =
            effect_record_from_event(&event).map_err(CurrentRunFailure::History)?
        {
            let provenance = event.effect_provenance.as_ref();
            let framework =
                provenance.is_none_or(|provenance| provenance.fact_owner.is_framework());
            if let Claim::Retain = self.claim(&effect_record.cursor, framework)? {
                let attempt = provenance.and_then(|provenance| provenance.attempt);
                self.cursors
                    .entry(effect_record.cursor.clone())
                    .or_default()
                    .records
                    .push((attempt, effect_record));
            }
        }
        Ok(())
    }

    fn fold_start(&mut self, position: usize, event: ChainEvent) -> Result<(), CurrentRunFailure> {
        let started = EffectAttemptStarted::try_from_event(&event).map_err(|error| {
            CurrentRunFailure::History(EffectError::Serialization(error.to_string()))
        })?;
        validate_attempt_event(&event, &started).map_err(CurrentRunFailure::History)?;
        if let Claim::Ignore = self.claim(&started.cursor, true)? {
            return Ok(());
        }
        let fold = self.cursors.entry(started.cursor.clone()).or_default();
        if fold
            .starts
            .iter()
            .any(|(_, existing, _)| existing.attempt == started.attempt)
        {
            return Err(history_failure(format!(
                "effect cursor {:?} has duplicate Start({})",
                started.cursor, started.attempt
            )));
        }
        if fold.terminal.is_some() {
            return Err(history_failure(format!(
                "effect terminal group '{}' appears before its highest Start",
                effect_outcome_group_id(&started.cursor)
            )));
        }
        fold.starts.push((position, started, event));
        Ok(())
    }

    fn claim(&self, cursor: &EffectCursor, framework: bool) -> Result<Claim, CurrentRunFailure> {
        let ours = self.stage.as_ref().is_some_and(|(namespace, stage)| {
            *namespace == cursor.recorded_flow_id && *stage == cursor.stage_key
        });
        if !ours {
            if framework {
                return Err(history_failure(format!(
                    "a stage journal holds framework effect evidence for another stage's cursor {cursor:?}"
                )));
            }
            return Ok(Claim::Ignore);
        }
        if let Some(settled) = self
            .settled
            .filter(|settled| cursor.input_seq.get() <= *settled)
        {
            return Err(history_failure(format!(
                "stage '{}' recorded effect evidence for cursor {cursor:?} after settling every input through {settled}",
                cursor.stage_key
            )));
        }
        Ok(Claim::Retain)
    }

    fn join_group(
        &mut self,
        group_id: &str,
        position: usize,
        member: Option<JournalGroupMember>,
        event: ChainEvent,
        frontier: &CausalFrontier,
    ) -> Result<(), CurrentRunFailure> {
        let pending = self.groups.entry(group_id.to_string()).or_default();
        pending.members.push((position, member, event));
        pending.causal.merge(frontier).map_err(journal_failure)?;
        let expected = pending
            .members
            .first()
            .and_then(|(_, member, _)| member.map(|member| member.size));
        if expected.is_some_and(|size| pending.members.len() < size as usize) {
            return Ok(());
        }
        match self.groups.remove(group_id) {
            Some(group) => self.finish_group(group_id, group),
            None => Ok(()),
        }
    }

    fn close_incomplete_groups(&mut self) -> Result<(), CurrentRunFailure> {
        let pending = std::mem::take(&mut self.groups);
        for (group_id, group) in pending {
            self.finish_group(&group_id, group)
                .map_err(|failure| self.fail(failure))?;
        }
        Ok(())
    }

    fn finish_group(
        &mut self,
        group_id: &str,
        group: PendingGroup,
    ) -> Result<(), CurrentRunFailure> {
        let effect_group = group_id.starts_with(OUTCOME_GROUP_PREFIX)
            || group_id.starts_with(ESCAPE_CONTROLS_GROUP_PREFIX);
        if effect_group {
            validate_atomic_group_members(group_id, &group.members)
                .map_err(CurrentRunFailure::History)?;
        }
        let mut ids = HashSet::new();
        if group
            .members
            .iter()
            .any(|(_, _, event)| !ids.insert(event.id))
        {
            return Err(history_failure(format!(
                "atomic journal group '{group_id}' repeats an event id"
            )));
        }
        let position = group
            .members
            .iter()
            .map(|(position, _, _)| *position)
            .min()
            .unwrap_or(self.position);
        let folded = FoldedGroup {
            position,
            events: group
                .members
                .into_iter()
                .map(|(_, _, event)| event)
                .collect(),
            causal: group.causal,
        };
        match group_cursor(group_id) {
            Some(GroupCursor::Outcome(cursor)) => self.fold_outcome_group(group_id, cursor, folded),
            Some(GroupCursor::EscapeControls(cursor, attempt)) => {
                self.fold_escape_controls(group_id, cursor, attempt, folded)
            }
            None if group_id.starts_with(ESCAPE_CONTROLS_GROUP_PREFIX) => Err(history_failure(
                format!("escape-control batch '{group_id}' has no matching effect Start"),
            )),
            None => Ok(()),
        }
    }

    fn fold_outcome_group(
        &mut self,
        group_id: &str,
        cursor: EffectCursor,
        group: FoldedGroup,
    ) -> Result<(), CurrentRunFailure> {
        if let Claim::Ignore = self.claim(&cursor, true)? {
            return Ok(());
        }
        let fold = self.cursors.entry(cursor).or_default();
        if fold.terminal.is_some() {
            return Err(history_failure(format!(
                "atomic effect group '{group_id}' has non-contiguous, missing, or duplicate frame membership"
            )));
        }
        fold.terminal = Some(group);
        Ok(())
    }

    fn fold_escape_controls(
        &mut self,
        group_id: &str,
        cursor: EffectCursor,
        attempt: EffectAttemptOrdinal,
        group: FoldedGroup,
    ) -> Result<(), CurrentRunFailure> {
        if let Claim::Ignore = self.claim(&cursor, true)? {
            return Ok(());
        }
        let fold = self.cursors.entry(cursor).or_default();
        let Some(start_position) = fold
            .starts
            .iter()
            .find(|(_, started, _)| started.attempt == attempt)
            .map(|(position, _, _)| *position)
        else {
            return Err(history_failure(format!(
                "escape-control batch '{group_id}' has no matching effect Start"
            )));
        };
        if group.events.iter().any(ChainEvent::consumes_data_credit) {
            return Err(history_failure(format!(
                "escape-control batch '{group_id}' contains Data"
            )));
        }
        if group.position <= start_position {
            return Err(history_failure(format!(
                "escape-control batch '{group_id}' appears before its Start"
            )));
        }
        if fold
            .terminal
            .as_ref()
            .is_some_and(|terminal| group.position >= terminal.position)
        {
            return Err(history_failure(format!(
                "escape-control batch '{group_id}' appears after settlement"
            )));
        }
        if fold.escape_controls.contains_key(&attempt) {
            return Err(history_failure(format!(
                "atomic effect group '{group_id}' has non-contiguous, missing, or duplicate frame membership"
            )));
        }
        fold.escape_controls.insert(attempt, group);
        Ok(())
    }

    fn cursor_history(
        &self,
        cursor: &EffectCursor,
    ) -> Result<EffectCursorHistory, CurrentRunFailure> {
        let mut history = EffectCursorHistory::default();
        let Some(fold) = self.cursors.get(cursor) else {
            validate_cursor_history(cursor, &history).map_err(CurrentRunFailure::History)?;
            return Ok(history);
        };
        for frontier in &fold.frontiers {
            history.causal.merge(frontier).map_err(journal_failure)?;
        }
        for (attempt, record) in &fold.records {
            if history
                .terminal_attempt
                .is_some_and(|existing| existing != *attempt)
            {
                return Err(history_failure(format!(
                    "effect cursor {cursor:?} terminal group disagrees on attempt ordinal"
                )));
            }
            history.terminal_attempt = Some(*attempt);
            history.records.push(record.clone());
        }
        for (_, started, event) in &fold.starts {
            history
                .attempt_events
                .insert(started.attempt, event.clone());
            history.attempts.push(started.clone());
        }
        for (event, abandoned) in &fold.abandonments {
            validate_abandonment_event(event, abandoned).map_err(CurrentRunFailure::History)?;
            if history.abandonment.replace(abandoned.clone()).is_some() {
                return Err(history_failure(format!(
                    "effect cursor {cursor:?} has duplicate recovery abandonment"
                )));
            }
        }
        if let Some(terminal) = &fold.terminal {
            history.terminal_group_events = terminal.events.clone();
            history
                .causal
                .merge(&terminal.causal)
                .map_err(journal_failure)?;
        }
        for started in &history.attempts {
            if let Some(batch) = fold.escape_controls.get(&started.attempt) {
                history
                    .escape_control_batches
                    .insert(started.attempt, batch.events.clone());
            }
        }
        for batch in history
            .attempts
            .iter()
            .filter_map(|started| fold.escape_controls.get(&started.attempt))
        {
            history
                .causal
                .merge(&batch.causal)
                .map_err(journal_failure)?;
        }
        validate_cursor_history(cursor, &history).map_err(CurrentRunFailure::History)?;
        Ok(history)
    }
}

fn group_cursor(group_id: &str) -> Option<GroupCursor> {
    let (fields, outcome) = match group_id.strip_prefix(OUTCOME_GROUP_PREFIX) {
        Some(fields) => (fields, true),
        None => (group_id.strip_prefix(ESCAPE_CONTROLS_GROUP_PREFIX)?, false),
    };
    let (namespace, fields) = length_prefixed(fields)?;
    let (stage, fields) = length_prefixed(fields)?;
    let mut fields = fields.split(':');
    let input = fields.next()?.parse::<u64>().ok()?;
    let ordinal = EffectOrdinal::new(fields.next()?.parse::<u32>().ok()?);
    let cursor = EffectCursor::new(namespace, stage, input, ordinal);
    let parsed = if outcome {
        GroupCursor::Outcome(cursor)
    } else {
        let attempt = EffectAttemptOrdinal::new(fields.next()?.parse::<u32>().ok()?);
        GroupCursor::EscapeControls(cursor, attempt)
    };
    if fields.next().is_some() {
        return None;
    }
    let canonical = match &parsed {
        GroupCursor::Outcome(cursor) => effect_outcome_group_id(cursor),
        GroupCursor::EscapeControls(cursor, attempt) => {
            effect_escape_controls_group_id(cursor, *attempt)
        }
    };
    (canonical.as_str() == group_id).then_some(parsed)
}

fn length_prefixed(fields: &str) -> Option<(&str, &str)> {
    let (length, rest) = fields.split_once(':')?;
    let length = length.parse::<usize>().ok()?;
    let value = rest.get(..length)?;
    let rest = rest.get(length..)?.strip_prefix(':')?;
    Some((value, rest))
}

fn journal_failure(error: impl std::fmt::Display) -> CurrentRunFailure {
    CurrentRunFailure::Journal(EffectError::Journal(error.to_string()))
}

fn history_failure(detail: String) -> CurrentRunFailure {
    CurrentRunFailure::History(EffectError::EffectProvenanceMismatch(detail))
}
