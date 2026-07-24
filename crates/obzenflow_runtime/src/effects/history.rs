// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use obzenflow_core::event::event_envelope::JournalGroupMember;
use obzenflow_core::journal::ArchiveStatus;
use std::collections::{BTreeMap, HashSet};

type GroupedJournalEvents = HashMap<String, Vec<(usize, Option<JournalGroupMember>, ChainEvent)>>;

#[derive(Debug, Default)]
pub(crate) struct EffectCursorCoordinator {
    locks: Mutex<HashMap<EffectCursor, Arc<tokio::sync::Mutex<()>>>>,
    poisoned: Mutex<HashSet<EffectCursor>>,
}

impl EffectCursorCoordinator {
    pub(crate) async fn lock(
        &self,
        cursor: &EffectCursor,
    ) -> Result<tokio::sync::OwnedMutexGuard<()>, EffectError> {
        if self
            .poisoned
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains(cursor)
        {
            return Err(EffectError::Execution(format!(
                "effect cursor {cursor:?} is poisoned after a failed publication"
            )));
        }
        let lock = self
            .locks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entry(cursor.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let guard = lock.lock_owned().await;
        if self
            .poisoned
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains(cursor)
        {
            drop(guard);
            return Err(EffectError::Execution(format!(
                "effect cursor {cursor:?} is poisoned after a failed publication"
            )));
        }
        Ok(guard)
    }

    pub(crate) fn poison(&self, cursor: EffectCursor) {
        self.poisoned
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(cursor);
    }
}

#[async_trait]
pub trait EffectHistoryReader: Send {
    async fn read_effect(
        &mut self,
        cursor: &EffectCursor,
    ) -> Result<Option<EffectRecord>, EffectError>;
}

#[async_trait]
pub trait EffectHistoryStore: Send + Sync {
    async fn open_effect_history(
        &self,
        stage_key: &str,
    ) -> Result<Box<dyn EffectHistoryReader>, EffectError>;
}

#[derive(Clone, Debug)]
pub struct EffectHistory {
    recorded_flow_id: RecordedFlowId,
    records: Arc<Vec<EffectRecord>>,
    index: Arc<HashMap<EffectCursor, Vec<usize>>>,
    attempts: Arc<HashMap<EffectCursor, Vec<EffectAttemptStarted>>>,
    abandonments: Arc<HashMap<EffectCursor, EffectRecoveryAbandoned>>,
    terminal_attempts: Arc<HashMap<EffectCursor, Option<EffectAttemptOrdinal>>>,
    grouped_events: Arc<GroupedJournalEvents>,
    attempt_positions: Arc<HashMap<(EffectCursor, EffectAttemptOrdinal), usize>>,
    attempt_events: Arc<HashMap<(EffectCursor, EffectAttemptOrdinal), ChainEvent>>,
}

#[derive(Debug, Clone)]
pub(crate) enum EffectHistorySelection {
    Miss,
    Hit(Vec<EffectRecord>),
    InDoubt(Vec<EffectAttemptStarted>),
}

#[derive(Debug, Clone, Default)]
pub(crate) struct EffectCursorHistory {
    pub records: Vec<EffectRecord>,
    pub attempts: Vec<EffectAttemptStarted>,
    pub attempt_events: BTreeMap<EffectAttemptOrdinal, ChainEvent>,
    pub abandonment: Option<EffectRecoveryAbandoned>,
    pub terminal_attempt: Option<Option<EffectAttemptOrdinal>>,
    pub terminal_group_events: Vec<ChainEvent>,
    pub escape_control_batches: BTreeMap<EffectAttemptOrdinal, Vec<ChainEvent>>,
}

impl EffectCursorHistory {
    pub(crate) fn select(&self) -> EffectHistorySelection {
        if !self.records.is_empty() {
            EffectHistorySelection::Hit(self.records.clone())
        } else if !self.attempts.is_empty() {
            EffectHistorySelection::InDoubt(self.attempts.clone())
        } else {
            EffectHistorySelection::Miss
        }
    }

    pub(crate) fn highest_attempt(&self) -> Option<EffectAttemptOrdinal> {
        self.attempts.last().map(|attempt| attempt.attempt)
    }
}

impl EffectHistory {
    pub async fn load(
        archive: &Arc<dyn ReplayArchive>,
        stage_key: &str,
    ) -> Result<Self, EffectError> {
        let mut reader = archive.open_effect_history(stage_key).await?;
        let mut records = Vec::new();
        let mut attempts: HashMap<EffectCursor, Vec<EffectAttemptStarted>> = HashMap::new();
        let mut abandonments = HashMap::new();
        let mut terminal_attempts = HashMap::new();
        let mut grouped_events: GroupedJournalEvents = HashMap::new();
        let mut attempt_positions = HashMap::new();
        let mut attempt_events = HashMap::new();
        let mut position = 0_usize;

        while let Some(envelope) = reader
            .next()
            .await
            .map_err(|e| EffectError::ReplayArchive(e.to_string()))?
        {
            if let Some(group_id) = envelope.journal_group_id.as_ref() {
                grouped_events.entry(group_id.clone()).or_default().push((
                    position,
                    envelope.journal_group_member,
                    envelope.event.clone(),
                ));
            }
            position = position.saturating_add(1);
            if EffectAttemptStarted::event_type_matches(&envelope.event.event_type()) {
                let started = EffectAttemptStarted::try_from_event(&envelope.event)
                    .map_err(|error| EffectError::Serialization(error.to_string()))?;
                validate_attempt_event(&envelope.event, &started)?;
                if attempt_positions
                    .insert(
                        (started.cursor.clone(), started.attempt),
                        position.saturating_sub(1),
                    )
                    .is_some()
                {
                    return Err(EffectError::EffectProvenanceMismatch(format!(
                        "effect cursor {:?} has duplicate Start({})",
                        started.cursor, started.attempt
                    )));
                }
                attempt_events.insert(
                    (started.cursor.clone(), started.attempt),
                    envelope.event.clone(),
                );
                attempts
                    .entry(started.cursor.clone())
                    .or_default()
                    .push(started);
                continue;
            }
            if EffectRecoveryAbandoned::event_type_matches(&envelope.event.event_type()) {
                let abandoned = EffectRecoveryAbandoned::try_from_event(&envelope.event)
                    .map_err(|error| EffectError::Serialization(error.to_string()))?;
                validate_abandonment_event(&envelope.event, &abandoned)?;
                if abandonments
                    .insert(abandoned.cursor.clone(), abandoned)
                    .is_some()
                {
                    return Err(EffectError::EffectProvenanceMismatch(
                        "effect cursor has more than one recovery-abandonment terminal".to_string(),
                    ));
                }
                continue;
            }
            if let Some(record) = effect_record_from_event(&envelope.event)? {
                let attempt = envelope
                    .event
                    .effect_provenance
                    .as_ref()
                    .and_then(|provenance| provenance.attempt);
                match terminal_attempts.entry(record.cursor.clone()) {
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(attempt);
                    }
                    std::collections::hash_map::Entry::Occupied(entry)
                        if *entry.get() != attempt =>
                    {
                        return Err(EffectError::EffectProvenanceMismatch(format!(
                            "effect outcome group for cursor {:?} disagrees on attempt ordinal",
                            record.cursor
                        )));
                    }
                    std::collections::hash_map::Entry::Occupied(_) => {}
                }
                records.push(record);
            }
        }

        // Only a non-`Completed` archive may carry a torn tail, so only then is
        // an incomplete outcome group tolerated as absent.
        let mut history = Self::from_records_with_policy(
            archive.archive_flow_id().to_string(),
            records,
            archive.archive_status() != ArchiveStatus::Completed,
        )?;
        validate_attempt_grammar(
            &attempts,
            &history.index,
            &terminal_attempts,
            &abandonments,
            &history.records,
        )?;
        history.attempts = Arc::new(attempts);
        history.abandonments = Arc::new(abandonments);
        history.terminal_attempts = Arc::new(terminal_attempts);
        history.attempt_positions = Arc::new(attempt_positions);
        history.attempt_events = Arc::new(attempt_events);
        validate_group_grammar(&history, &grouped_events)?;
        history.grouped_events = Arc::new(grouped_events);
        Ok(history)
    }

    /// Tolerant constructor: a torn-tail incomplete outcome group is dropped as
    /// absent. `load` uses [`Self::from_records_with_policy`] for the
    /// status-derived policy.
    pub fn from_records(
        recorded_flow_id: impl Into<RecordedFlowId>,
        records: Vec<EffectRecord>,
    ) -> Result<Self, EffectError> {
        Self::from_records_with_policy(recorded_flow_id, records, true)
    }

    /// Build under an explicit torn-tail policy. With `tolerate_torn_tail` false
    /// (a `Completed` archive), an incomplete outcome group is corruption and
    /// fails loud instead of being dropped.
    pub(crate) fn from_records_with_policy(
        recorded_flow_id: impl Into<RecordedFlowId>,
        records: Vec<EffectRecord>,
        tolerate_torn_tail: bool,
    ) -> Result<Self, EffectError> {
        let fallback_recorded_flow_id = recorded_flow_id.into();
        let recorded_flow_id =
            infer_recorded_flow_id(&records)?.unwrap_or(fallback_recorded_flow_id);
        let mut index = HashMap::new();
        for (position, record) in records.iter().enumerate() {
            index
                .entry(record.cursor.clone())
                .or_insert_with(Vec::new)
                .push(position);
        }

        // FLOWIP-120q: an incomplete outcome group is an uncommitted torn tail.
        // Treat it as absent (drop it from the index) so strict replay raises
        // MissingRecordedEffect and resume re-executes, rather than feeding a
        // partial outcome back. Any other validation error is corruption.
        let mut incomplete = Vec::new();
        for (cursor, positions) in index.iter() {
            let group = positions
                .iter()
                .filter_map(|position| records.get(*position))
                .collect::<Vec<_>>();
            match validate_effect_outcome_group(&group) {
                Ok(()) => {}
                Err(EffectError::IncompleteOutcomeGroup {
                    expected, present, ..
                }) if tolerate_torn_tail => {
                    tracing::warn!(
                        ?cursor,
                        expected,
                        present,
                        "dropping uncommitted effect outcome group (torn tail); effect treated as absent"
                    );
                    incomplete.push(cursor.clone());
                }
                // No torn tail permitted here, so an incomplete group is
                // corruption, not an uncommitted tail. Fail loud rather than
                // dropping it as absent.
                Err(EffectError::IncompleteOutcomeGroup {
                    expected, present, ..
                }) => {
                    return Err(EffectError::EffectProvenanceMismatch(format!(
                        "effect outcome group for cursor {cursor:?} is incomplete (present {present} of {expected}) on a completed archive, which permits no torn tail"
                    )));
                }
                Err(other) => return Err(other),
            }
        }
        for cursor in incomplete {
            index.remove(&cursor);
        }

        let terminal_attempts = index.keys().cloned().map(|cursor| (cursor, None)).collect();

        Ok(Self {
            recorded_flow_id,
            records: Arc::new(records),
            index: Arc::new(index),
            attempts: Arc::new(HashMap::new()),
            abandonments: Arc::new(HashMap::new()),
            // This constructor receives decoded records rather than journal
            // envelopes, so it cannot recover an affine attempt ordinal.
            // Treat surviving records as the legacy zero-attempt form. Real
            // archive loading replaces this map from envelope provenance, and
            // affine execution separately requires its Start and atomic group.
            terminal_attempts: Arc::new(terminal_attempts),
            grouped_events: Arc::new(HashMap::new()),
            attempt_positions: Arc::new(HashMap::new()),
            attempt_events: Arc::new(HashMap::new()),
        })
    }

    // FLOWIP-120a: the cursor IS the index key (populated in `from_records`), so a
    // found record's stored cursor equals the lookup cursor by construction; a
    // "found under a different cursor" state is unrepresentable and needs no explicit
    // cursor-mismatch error. The two real divergence axes are handled elsewhere: an
    // absent cursor yields `None` (a `MissingRecordedEffect` under strict replay), and
    // a present-but-diverged record is caught by the independent `descriptor_hash`
    // check (`DescriptorMismatch`).
    fn find(&self, cursor: &EffectCursor) -> Option<&EffectRecord> {
        self.index
            .get(cursor)
            .and_then(|positions| positions.first())
            .and_then(|position| self.records.get(*position))
    }

    pub(super) fn find_group(&self, cursor: &EffectCursor) -> Option<Vec<&EffectRecord>> {
        self.index.get(cursor).map(|positions| {
            positions
                .iter()
                .filter_map(|position| self.records.get(*position))
                .collect()
        })
    }

    pub(crate) fn select(&self, cursor: &EffectCursor) -> EffectHistorySelection {
        self.cursor_history(cursor).select()
    }

    pub(crate) fn attempts(&self, cursor: &EffectCursor) -> &[EffectAttemptStarted] {
        self.attempts
            .get(cursor)
            .map(Vec::as_slice)
            .unwrap_or_default()
    }

    pub(crate) fn abandonment(&self, cursor: &EffectCursor) -> Option<&EffectRecoveryAbandoned> {
        self.abandonments.get(cursor)
    }

    pub(crate) fn cursor_history(&self, cursor: &EffectCursor) -> EffectCursorHistory {
        let attempts = self.attempts(cursor).to_vec();
        let attempt_events = attempts
            .iter()
            .filter_map(|started| {
                self.attempt_events
                    .get(&(cursor.clone(), started.attempt))
                    .map(|event| (started.attempt, event.clone()))
            })
            .collect();
        let terminal_group_id = effect_outcome_group_id(cursor);
        let terminal_group_events = self
            .grouped_events
            .get(terminal_group_id.as_str())
            .map(|events| {
                events
                    .iter()
                    .map(|(_, _, event)| event.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let escape_control_batches = attempts
            .iter()
            .filter_map(|started| {
                let group_id = effect_escape_controls_group_id(cursor, started.attempt);
                self.grouped_events.get(group_id.as_str()).map(|events| {
                    (
                        started.attempt,
                        events
                            .iter()
                            .map(|(_, _, event)| event.clone())
                            .collect::<Vec<_>>(),
                    )
                })
            })
            .collect();
        EffectCursorHistory {
            records: self
                .find_group(cursor)
                .unwrap_or_default()
                .into_iter()
                .cloned()
                .collect(),
            attempts,
            attempt_events,
            abandonment: self.abandonment(cursor).cloned(),
            terminal_attempt: self.terminal_attempts.get(cursor).copied(),
            terminal_group_events,
            escape_control_batches,
        }
    }

    pub fn recorded_flow_id(&self) -> &RecordedFlowId {
        &self.recorded_flow_id
    }

    /// Maximum recorded `input_seq` in the surviving index (FLOWIP-120n F7):
    /// a position at or below it is recorded prefix, beyond it live tail.
    /// Computed after torn-tail groups are dropped, so a torn boundary group
    /// lowers the mark and resume re-executes it rather than failing loud.
    pub fn max_recorded_input_seq(&self) -> Option<u64> {
        self.index
            .keys()
            .chain(self.attempts.keys())
            .map(|cursor| cursor.input_seq.get())
            .max()
    }
}

pub(crate) async fn current_cursor_history(
    journal: &Arc<dyn Journal<ChainEvent>>,
    cursor: &EffectCursor,
) -> Result<EffectCursorHistory, EffectError> {
    let envelopes = journal
        .read_all_unordered()
        .await
        .map_err(|error| EffectError::Journal(error.to_string()))?;
    let mut history = EffectCursorHistory::default();
    let mut groups: GroupedJournalEvents = HashMap::new();
    let mut all_attempt_positions = HashMap::new();

    for (position, envelope) in envelopes.into_iter().enumerate() {
        if let Some(group_id) = envelope.journal_group_id.as_ref() {
            groups.entry(group_id.clone()).or_default().push((
                position,
                envelope.journal_group_member,
                envelope.event.clone(),
            ));
        }
        if EffectAttemptStarted::event_type_matches(&envelope.event.event_type()) {
            let started = EffectAttemptStarted::try_from_event(&envelope.event)
                .map_err(|error| EffectError::Serialization(error.to_string()))?;
            validate_attempt_event(&envelope.event, &started)?;
            if all_attempt_positions
                .insert((started.cursor.clone(), started.attempt), position)
                .is_some()
            {
                return Err(EffectError::EffectProvenanceMismatch(format!(
                    "effect cursor {:?} has duplicate Start({})",
                    started.cursor, started.attempt
                )));
            }
            if &started.cursor == cursor {
                history
                    .attempt_events
                    .insert(started.attempt, envelope.event.clone());
                history.attempts.push(started);
            }
            continue;
        }
        if EffectRecoveryAbandoned::event_type_matches(&envelope.event.event_type()) {
            let abandoned = EffectRecoveryAbandoned::try_from_event(&envelope.event)
                .map_err(|error| EffectError::Serialization(error.to_string()))?;
            if &abandoned.cursor == cursor {
                validate_abandonment_event(&envelope.event, &abandoned)?;
                if history.abandonment.replace(abandoned).is_some() {
                    return Err(EffectError::EffectProvenanceMismatch(format!(
                        "effect cursor {cursor:?} has duplicate recovery abandonment"
                    )));
                }
            }
            continue;
        }
        if let Some(record) = effect_record_from_event(&envelope.event)? {
            if &record.cursor == cursor {
                let attempt = envelope
                    .event
                    .effect_provenance
                    .as_ref()
                    .and_then(|provenance| provenance.attempt);
                if history
                    .terminal_attempt
                    .is_some_and(|existing| existing != attempt)
                {
                    return Err(EffectError::EffectProvenanceMismatch(format!(
                        "effect cursor {cursor:?} terminal group disagrees on attempt ordinal"
                    )));
                }
                history.terminal_attempt = Some(attempt);
                history.records.push(record);
            }
        }
    }

    validate_group_grammar_parts(&all_attempt_positions, &groups)?;
    let terminal_group_id = effect_outcome_group_id(cursor);
    history.terminal_group_events = groups
        .remove(terminal_group_id.as_str())
        .unwrap_or_default()
        .into_iter()
        .map(|(_, _, event)| event)
        .collect();
    history.escape_control_batches = history
        .attempts
        .iter()
        .filter_map(|started| {
            let group_id = effect_escape_controls_group_id(cursor, started.attempt);
            groups.remove(group_id.as_str()).map(|events| {
                (
                    started.attempt,
                    events
                        .into_iter()
                        .map(|(_, _, event)| event)
                        .collect::<Vec<_>>(),
                )
            })
        })
        .collect();
    validate_cursor_history(cursor, &history)?;
    Ok(history)
}

pub(crate) fn merge_cursor_histories(
    cursor: &EffectCursor,
    archived: EffectCursorHistory,
    current: EffectCursorHistory,
) -> Result<EffectCursorHistory, EffectError> {
    let mut attempt_events = archived.attempt_events;
    for (attempt, event) in current.attempt_events {
        if let Some(existing) = attempt_events.insert(attempt, event.clone()) {
            if !effect_identity_equal(&existing, &event)? {
                return Err(EffectError::EffectProvenanceMismatch(format!(
                    "effect cursor {cursor:?} has conflicting durable identity for Start({attempt})"
                )));
            }
        }
    }
    let mut attempts = BTreeMap::new();
    for started in archived.attempts.iter().chain(current.attempts.iter()) {
        if let Some(existing) = attempts.insert(started.attempt, started.clone()) {
            if existing.cursor != started.cursor
                || existing.descriptor_hash != started.descriptor_hash
                || existing.effect_type != started.effect_type
                || existing.outcome_group_id != started.outcome_group_id
            {
                return Err(EffectError::EffectProvenanceMismatch(format!(
                    "effect cursor {cursor:?} has conflicting duplicate attempt {}",
                    started.attempt
                )));
            }
        }
    }

    let records = match (archived.records.is_empty(), current.records.is_empty()) {
        (true, _) => current.records.clone(),
        (_, true) => archived.records.clone(),
        (false, false) if archived.records == current.records => current.records.clone(),
        (false, false) => {
            return Err(EffectError::EffectProvenanceMismatch(format!(
                "effect cursor {cursor:?} has conflicting archived and current-run terminals"
            )));
        }
    };
    let terminal_attempt = if current.terminal_attempt.is_some() {
        current.terminal_attempt
    } else {
        archived.terminal_attempt
    };
    let abandonment = match (&archived.abandonment, &current.abandonment) {
        (Some(left), Some(right)) if left != right => {
            return Err(EffectError::EffectProvenanceMismatch(format!(
                "effect cursor {cursor:?} has conflicting recovery-abandonment evidence"
            )));
        }
        (_, Some(current)) => Some(current.clone()),
        (Some(archived), None) => Some(archived.clone()),
        (None, None) => None,
    };
    let mut escape_control_batches = archived.escape_control_batches;
    for (attempt, events) in current.escape_control_batches {
        if let Some(existing) = escape_control_batches.insert(attempt, events.clone()) {
            if !events_equal(&existing, &events)? {
                return Err(EffectError::EffectProvenanceMismatch(format!(
                    "effect cursor {cursor:?} has conflicting escape-control batch for attempt {attempt}"
                )));
            }
        }
    }
    let terminal_group_events = if current.terminal_group_events.is_empty() {
        archived.terminal_group_events
    } else if archived.terminal_group_events.is_empty() {
        current.terminal_group_events
    } else if event_groups_identity_equal(
        &archived.terminal_group_events,
        &current.terminal_group_events,
    )? {
        archived.terminal_group_events
    } else {
        return Err(EffectError::EffectProvenanceMismatch(format!(
            "effect cursor {cursor:?} has conflicting archived and current-run terminal-group identity"
        )));
    };
    let merged = EffectCursorHistory {
        records,
        attempts: attempts.into_values().collect(),
        attempt_events,
        abandonment,
        terminal_attempt,
        terminal_group_events,
        escape_control_batches,
    };
    validate_cursor_history(cursor, &merged)?;
    Ok(merged)
}

pub(crate) fn validate_affine_terminal_group(
    cursor: &EffectCursor,
    history: &EffectCursorHistory,
) -> Result<(), EffectError> {
    if history.records.is_empty() {
        return Ok(());
    }
    if history.terminal_group_events.is_empty() {
        return Err(EffectError::EffectProvenanceMismatch(format!(
            "affine effect cursor {cursor:?} has a terminal outside its deterministic atomic group"
        )));
    }

    let mut grouped_records = Vec::new();
    let mut grouped_abandonment = None;
    for event in &history.terminal_group_events {
        if EffectRecoveryAbandoned::event_type_matches(&event.event_type()) {
            let abandonment = EffectRecoveryAbandoned::try_from_event(event)
                .map_err(|error| EffectError::Serialization(error.to_string()))?;
            validate_abandonment_event(event, &abandonment)?;
            if grouped_abandonment.replace(abandonment).is_some() {
                return Err(EffectError::EffectProvenanceMismatch(format!(
                    "affine effect cursor {cursor:?} has duplicate abandonment in its terminal group"
                )));
            }
            continue;
        }
        if let Some(record) = effect_record_from_event(event)? {
            grouped_records.push(record);
            continue;
        }
        if event.is_data() {
            return Err(EffectError::EffectProvenanceMismatch(format!(
                "affine effect cursor {cursor:?} terminal group contains unrecognised Data"
            )));
        }
    }

    if grouped_records != history.records || grouped_abandonment != history.abandonment {
        return Err(EffectError::EffectProvenanceMismatch(format!(
            "affine effect cursor {cursor:?} terminal group does not physically close its recorded outcome"
        )));
    }
    if history.records.iter().any(|record| {
        matches!(
            &record.outcome,
            EffectOutcomePayload::Failed { error_type, .. }
                if error_type.as_str() == "effect_port_binding_invariant_violation"
        )
    }) {
        let attempt = history.terminal_attempt.flatten().ok_or_else(|| {
            EffectError::EffectProvenanceMismatch(format!(
                "binding-invariant terminal for cursor {cursor:?} has no effect-execution attempt"
            ))
        })?;
        validate_invariant_settlement_evidence(cursor, attempt, &history.terminal_group_events)?;
    }
    Ok(())
}

pub(crate) fn validate_invariant_settlement_evidence(
    cursor: &EffectCursor,
    attempt: EffectAttemptOrdinal,
    control_events: &[ChainEvent],
) -> Result<(usize, usize), EffectError> {
    use obzenflow_core::event::payloads::observability_payload::{
        CircuitBreakerEvent, CircuitBreakerHealthClassification, MiddlewareLifecycle,
        ObservabilityPayload,
    };

    let mut settlement_index = None;
    let mut recovery_index = None;
    for (index, event) in control_events.iter().enumerate() {
        let ChainEventContent::Observability(ObservabilityPayload::Middleware(
            MiddlewareLifecycle::CircuitBreaker(circuit_breaker),
        )) = &event.content
        else {
            continue;
        };
        match circuit_breaker {
            CircuitBreakerEvent::AttemptSettled {
                cursor: observed_cursor,
                attempt: observed_attempt,
                health_classification,
                ..
            } => {
                if observed_cursor != cursor || *observed_attempt != attempt.get() {
                    return Err(EffectError::EffectProvenanceMismatch(format!(
                        "binding-invariant settlement evidence disagrees with cursor {cursor:?} attempt {attempt}"
                    )));
                }
                if !matches!(
                    health_classification,
                    CircuitBreakerHealthClassification::Ignored
                ) {
                    return Err(EffectError::EffectProvenanceMismatch(format!(
                        "binding-invariant terminal for cursor {cursor:?} attempt {attempt} must settle breaker health as Ignored"
                    )));
                }
                if settlement_index.replace(index).is_some() {
                    return Err(EffectError::EffectProvenanceMismatch(format!(
                        "binding-invariant terminal for cursor {cursor:?} attempt {attempt} has duplicate AttemptSettled evidence"
                    )));
                }
            }
            CircuitBreakerEvent::RecoveryCompleted {
                cursor: observed_cursor,
                total_attempts,
                ..
            } => {
                if observed_cursor != cursor || *total_attempts != attempt.get() {
                    return Err(EffectError::EffectProvenanceMismatch(format!(
                        "binding-invariant recovery evidence disagrees with cursor {cursor:?} attempt {attempt}"
                    )));
                }
                if recovery_index.replace(index).is_some() {
                    return Err(EffectError::EffectProvenanceMismatch(format!(
                        "binding-invariant terminal for cursor {cursor:?} attempt {attempt} has duplicate RecoveryCompleted evidence"
                    )));
                }
            }
            _ => {}
        }
    }

    let settlement_index = settlement_index.ok_or_else(|| {
        EffectError::EffectProvenanceMismatch(format!(
            "binding-invariant terminal for cursor {cursor:?} attempt {attempt} lacks AttemptSettled"
        ))
    })?;
    let recovery_index = recovery_index.ok_or_else(|| {
        EffectError::EffectProvenanceMismatch(format!(
            "binding-invariant terminal for cursor {cursor:?} attempt {attempt} lacks RecoveryCompleted"
        ))
    })?;
    if recovery_index <= settlement_index {
        return Err(EffectError::EffectProvenanceMismatch(format!(
            "binding-invariant terminal for cursor {cursor:?} attempt {attempt} orders RecoveryCompleted before AttemptSettled"
        )));
    }
    Ok((settlement_index, recovery_index))
}

fn events_equal(left: &[ChainEvent], right: &[ChainEvent]) -> Result<bool, EffectError> {
    Ok(
        serde_json::to_value(left)
            .map_err(|error| EffectError::Serialization(error.to_string()))?
            == serde_json::to_value(right)
                .map_err(|error| EffectError::Serialization(error.to_string()))?,
    )
}

fn effect_identity_equal(left: &ChainEvent, right: &ChainEvent) -> Result<bool, EffectError> {
    let left_content = serde_json::to_value(&left.content)
        .map_err(|error| EffectError::Serialization(error.to_string()))?;
    let right_content = serde_json::to_value(&right.content)
        .map_err(|error| EffectError::Serialization(error.to_string()))?;
    Ok(left.id == right.id
        && left.processing_info.event_time == right.processing_info.event_time
        && left.effect_provenance == right.effect_provenance
        && left_content == right_content)
}

fn event_groups_identity_equal(
    left: &[ChainEvent],
    right: &[ChainEvent],
) -> Result<bool, EffectError> {
    if left.len() != right.len() {
        return Ok(false);
    }
    for (left, right) in left.iter().zip(right) {
        if !effect_identity_equal(left, right)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn validate_cursor_history(
    cursor: &EffectCursor,
    history: &EffectCursorHistory,
) -> Result<(), EffectError> {
    let ordinals = history
        .attempts
        .iter()
        .map(|started| started.attempt.get())
        .collect::<Vec<_>>();
    let expected = (1..=u32::try_from(ordinals.len()).map_err(|_| {
        EffectError::EffectProvenanceMismatch(
            "effect attempt history exceeds u32 range".to_string(),
        )
    })?)
        .collect::<Vec<_>>();
    if ordinals != expected {
        return Err(EffectError::EffectProvenanceMismatch(format!(
            "effect attempts for cursor {cursor:?} are not the contiguous sequence 1..m"
        )));
    }
    if history.attempt_events.len() != history.attempts.len() {
        return Err(EffectError::EffectProvenanceMismatch(format!(
            "effect cursor {cursor:?} does not retain one durable event identity per Start"
        )));
    }
    for started in &history.attempts {
        let event = history
            .attempt_events
            .get(&started.attempt)
            .ok_or_else(|| {
                EffectError::EffectProvenanceMismatch(format!(
                    "effect cursor {cursor:?} Start({}) lacks its durable event identity",
                    started.attempt
                ))
            })?;
        validate_attempt_event(event, started)?;
    }
    validate_started_identity(cursor, &history.attempts)?;

    let record_refs = history.records.iter().collect::<Vec<_>>();
    validate_effect_outcome_group(&record_refs)?;
    validate_terminal_identity(
        cursor,
        &history.attempts,
        &history.records,
        history.abandonment.as_ref(),
    )?;

    if history.records.is_empty() {
        if history.abandonment.is_some() || history.terminal_attempt.is_some() {
            return Err(EffectError::EffectProvenanceMismatch(format!(
                "effect cursor {cursor:?} has terminal metadata without an outcome record"
            )));
        }
        return Ok(());
    }

    let recovery_abandoned = history.records.iter().any(|record| {
        matches!(
            &record.outcome,
            EffectOutcomePayload::Failed { error_type, .. }
                if error_type.as_str() == "recovery_abandoned"
        )
    });
    if recovery_abandoned {
        let highest = history.highest_attempt().ok_or_else(|| {
            EffectError::EffectProvenanceMismatch(format!(
                "effect cursor {cursor:?} recovery abandonment has no started attempt"
            ))
        })?;
        let abandoned = history.abandonment.as_ref().ok_or_else(|| {
            EffectError::EffectProvenanceMismatch(format!(
                "effect cursor {cursor:?} recovery failure lacks named abandonment"
            ))
        })?;
        if abandoned.highest_started_attempt != highest || history.terminal_attempt != Some(None) {
            return Err(EffectError::EffectProvenanceMismatch(format!(
                "effect cursor {cursor:?} recovery-abandonment terminal disagrees with attempt history"
            )));
        }
    } else {
        if history.abandonment.is_some() {
            return Err(EffectError::EffectProvenanceMismatch(format!(
                "effect cursor {cursor:?} has abandonment beside a non-abandonment terminal"
            )));
        }
        let expected_attempt = history.highest_attempt();
        if history.terminal_attempt != Some(expected_attempt) {
            return Err(EffectError::EffectProvenanceMismatch(format!(
                "effect cursor {cursor:?} terminal attempt does not match its highest Start"
            )));
        }
    }
    Ok(())
}

fn validate_attempt_event(
    event: &ChainEvent,
    started: &EffectAttemptStarted,
) -> Result<(), EffectError> {
    let decoded = EffectAttemptStarted::try_from_event(event)
        .map_err(|error| EffectError::Serialization(error.to_string()))?;
    if &decoded != started {
        return Err(EffectError::EffectProvenanceMismatch(
            "effect attempt start payload disagrees with its indexed history".to_string(),
        ));
    }
    let provenance = event.effect_provenance.as_ref().ok_or_else(|| {
        EffectError::EffectProvenanceMismatch(
            "effect attempt start is missing effect provenance".to_string(),
        )
    })?;
    let expected_group_id = effect_outcome_group_id(&started.cursor);
    if !provenance.fact_owner.is_framework()
        || provenance.cursor != started.cursor
        || provenance.descriptor_hash != started.descriptor_hash
        || provenance.descriptor.effect_type != started.effect_type
        || provenance.attempt != Some(started.attempt)
        || provenance.group_id.as_ref() != Some(&started.outcome_group_id)
        || started.outcome_group_id != expected_group_id
    {
        return Err(EffectError::EffectProvenanceMismatch(
            "effect attempt start disagrees with its effect provenance".to_string(),
        ));
    }
    Ok(())
}

fn validate_abandonment_event(
    event: &ChainEvent,
    abandoned: &EffectRecoveryAbandoned,
) -> Result<(), EffectError> {
    let provenance = event.effect_provenance.as_ref().ok_or_else(|| {
        EffectError::EffectProvenanceMismatch(
            "effect recovery abandonment is missing effect provenance".to_string(),
        )
    })?;
    let expected_group_id = effect_outcome_group_id(&abandoned.cursor);
    if !provenance.fact_owner.is_framework()
        || provenance.cursor != abandoned.cursor
        || provenance.descriptor_hash != abandoned.descriptor_hash
        || provenance.descriptor.effect_type != abandoned.effect_type
        || provenance.attempt.is_some()
        || provenance.group_id.as_ref() != Some(&abandoned.outcome_group_id)
        || abandoned.outcome_group_id != expected_group_id
    {
        return Err(EffectError::EffectProvenanceMismatch(
            "effect recovery abandonment disagrees with its effect provenance".to_string(),
        ));
    }
    Ok(())
}

fn validate_started_identity(
    cursor: &EffectCursor,
    starts: &[EffectAttemptStarted],
) -> Result<(), EffectError> {
    let Some(first) = starts.first() else {
        return Ok(());
    };
    let expected_group_id = effect_outcome_group_id(cursor);
    if starts.iter().any(|start| {
        start.cursor != *cursor
            || start.descriptor_hash != first.descriptor_hash
            || start.effect_type != first.effect_type
            || start.outcome_group_id != expected_group_id
            || start.causal_input_id != first.causal_input_id
    }) {
        return Err(EffectError::EffectProvenanceMismatch(format!(
            "effect attempts for cursor {cursor:?} disagree on immutable identity"
        )));
    }
    Ok(())
}

fn validate_terminal_identity(
    cursor: &EffectCursor,
    starts: &[EffectAttemptStarted],
    records: &[EffectRecord],
    abandonment: Option<&EffectRecoveryAbandoned>,
) -> Result<(), EffectError> {
    let Some(first) = starts.first() else {
        if abandonment.is_some() {
            return Err(EffectError::EffectProvenanceMismatch(format!(
                "effect cursor {cursor:?} has recovery abandonment without a Start"
            )));
        }
        return Ok(());
    };

    if records.iter().any(|record| {
        record.cursor != *cursor
            || record.descriptor_hash != first.descriptor_hash
            || record.descriptor.effect_type != first.effect_type
    }) {
        return Err(EffectError::EffectProvenanceMismatch(format!(
            "effect cursor {cursor:?} terminal disagrees with its Start identity"
        )));
    }

    if abandonment.is_some_and(|abandoned| {
        abandoned.cursor != *cursor
            || abandoned.descriptor_hash != first.descriptor_hash
            || abandoned.effect_type != first.effect_type
            || abandoned.outcome_group_id != first.outcome_group_id
            || abandoned.causal_input_id != first.causal_input_id
    }) {
        return Err(EffectError::EffectProvenanceMismatch(format!(
            "effect cursor {cursor:?} recovery abandonment disagrees with its Start identity"
        )));
    }
    Ok(())
}

fn validate_attempt_grammar(
    attempts: &HashMap<EffectCursor, Vec<EffectAttemptStarted>>,
    terminals: &HashMap<EffectCursor, Vec<usize>>,
    terminal_attempts: &HashMap<EffectCursor, Option<EffectAttemptOrdinal>>,
    abandonments: &HashMap<EffectCursor, EffectRecoveryAbandoned>,
    records: &[EffectRecord],
) -> Result<(), EffectError> {
    for (cursor, starts) in attempts {
        let ordinals = starts
            .iter()
            .map(|start| start.attempt.get())
            .collect::<Vec<_>>();
        let expected = (1..=u32::try_from(ordinals.len()).map_err(|_| {
            EffectError::EffectProvenanceMismatch(
                "effect attempt history exceeds u32 range".to_string(),
            )
        })?)
            .collect::<Vec<_>>();
        if ordinals != expected {
            return Err(EffectError::EffectProvenanceMismatch(format!(
                "effect attempts for cursor {cursor:?} are not the contiguous sequence 1..m"
            )));
        }
        validate_started_identity(cursor, starts)?;
        if let Some(positions) = terminals.get(cursor) {
            let terminal_records = positions
                .iter()
                .filter_map(|position| records.get(*position))
                .cloned()
                .collect::<Vec<_>>();
            validate_terminal_identity(
                cursor,
                starts,
                &terminal_records,
                abandonments.get(cursor),
            )?;
            let recovery_abandoned = positions.iter().any(|position| {
                records.get(*position).is_some_and(|record| {
                    matches!(
                        &record.outcome,
                        EffectOutcomePayload::Failed { error_type, .. }
                            if error_type.as_str() == "recovery_abandoned"
                    )
                })
            });
            let highest = starts
                .last()
                .expect("non-empty indexed attempt vector")
                .attempt;
            if recovery_abandoned {
                let abandonment = abandonments.get(cursor).ok_or_else(|| {
                    EffectError::EffectProvenanceMismatch(format!(
                        "effect cursor {cursor:?} recovery terminal lacks named abandonment"
                    ))
                })?;
                if abandonment.highest_started_attempt != highest
                    || terminal_attempts.get(cursor) != Some(&None)
                {
                    return Err(EffectError::EffectProvenanceMismatch(format!(
                        "effect cursor {cursor:?} recovery abandonment disagrees with Start history"
                    )));
                }
            } else if terminal_attempts.get(cursor) != Some(&Some(highest)) {
                return Err(EffectError::EffectProvenanceMismatch(format!(
                    "effect cursor {cursor:?} terminal does not name the highest started attempt"
                )));
            }
        } else if abandonments.contains_key(cursor) {
            return Err(EffectError::EffectProvenanceMismatch(format!(
                "effect cursor {cursor:?} has named abandonment without a generic terminal"
            )));
        }
    }

    for cursor in terminals.keys() {
        if !attempts.contains_key(cursor)
            && terminal_attempts.get(cursor).is_some_and(Option::is_some)
        {
            return Err(EffectError::EffectProvenanceMismatch(format!(
                "effect cursor {cursor:?} has a terminal attempt without a Start"
            )));
        }
    }
    for cursor in abandonments.keys() {
        if !attempts.contains_key(cursor) || !terminals.contains_key(cursor) {
            return Err(EffectError::EffectProvenanceMismatch(format!(
                "effect cursor {cursor:?} has orphan recovery-abandonment evidence"
            )));
        }
    }
    Ok(())
}

fn validate_group_grammar(
    history: &EffectHistory,
    grouped_events: &GroupedJournalEvents,
) -> Result<(), EffectError> {
    validate_group_grammar_parts(&history.attempt_positions, grouped_events)
}

fn validate_group_grammar_parts(
    attempt_positions: &HashMap<(EffectCursor, EffectAttemptOrdinal), usize>,
    grouped_events: &GroupedJournalEvents,
) -> Result<(), EffectError> {
    let mut highest_start_positions = HashMap::new();
    for ((cursor, _), position) in attempt_positions {
        highest_start_positions
            .entry(cursor)
            .and_modify(|highest: &mut usize| *highest = (*highest).max(*position))
            .or_insert(*position);
    }
    for (cursor, highest_start_position) in highest_start_positions {
        let terminal_group_id = effect_outcome_group_id(cursor);
        let Some(terminal_events) = grouped_events.get(terminal_group_id.as_str()) else {
            continue;
        };
        let terminal_position = terminal_events
            .iter()
            .map(|(position, _, _)| *position)
            .min()
            .expect("grouped journal events are non-empty");
        if terminal_position <= highest_start_position {
            return Err(EffectError::EffectProvenanceMismatch(format!(
                "effect terminal group '{terminal_group_id}' appears before its highest Start"
            )));
        }
    }

    let expected_escape_groups = attempt_positions
        .iter()
        .map(|((cursor, attempt), position)| {
            (
                effect_escape_controls_group_id(cursor, *attempt).to_string(),
                (cursor, *attempt, *position),
            )
        })
        .collect::<HashMap<_, _>>();
    for (group_id, events) in grouped_events {
        if group_id.starts_with("effect-outcome:v1:")
            || group_id.starts_with("effect-escape-controls:v1:")
        {
            validate_atomic_group_members(group_id, events)?;
        }
        let mut ids = HashSet::new();
        if events.iter().any(|(_, _, event)| !ids.insert(event.id)) {
            return Err(EffectError::EffectProvenanceMismatch(format!(
                "atomic journal group '{group_id}' repeats an event id"
            )));
        }
        if !group_id.starts_with("effect-escape-controls:v1:") {
            continue;
        }
        let Some((cursor, _attempt, start_position)) = expected_escape_groups.get(group_id) else {
            return Err(EffectError::EffectProvenanceMismatch(format!(
                "escape-control batch '{group_id}' has no matching effect Start"
            )));
        };
        if events.iter().any(|(_, _, event)| event.is_data()) {
            return Err(EffectError::EffectProvenanceMismatch(format!(
                "escape-control batch '{group_id}' contains Data"
            )));
        }
        let batch_position = events
            .iter()
            .map(|(position, _, _)| *position)
            .min()
            .expect("grouped journal events are non-empty");
        if batch_position <= *start_position {
            return Err(EffectError::EffectProvenanceMismatch(format!(
                "escape-control batch '{group_id}' appears before its Start"
            )));
        }
        let terminal_group_id = effect_outcome_group_id(cursor);
        if let Some(terminal_events) = grouped_events.get(terminal_group_id.as_str()) {
            let terminal_position = terminal_events
                .iter()
                .map(|(position, _, _)| *position)
                .min()
                .expect("grouped journal events are non-empty");
            if batch_position >= terminal_position {
                return Err(EffectError::EffectProvenanceMismatch(format!(
                    "escape-control batch '{group_id}' appears after settlement"
                )));
            }
        }
    }
    Ok(())
}

fn validate_atomic_group_members(
    group_id: &str,
    events: &[(usize, Option<JournalGroupMember>, ChainEvent)],
) -> Result<(), EffectError> {
    let Some((first_position, Some(first), _)) = events.first() else {
        return Err(EffectError::EffectProvenanceMismatch(format!(
            "atomic effect group '{group_id}' lacks physical-frame membership"
        )));
    };
    if first.size == 0 || usize::try_from(first.size).ok() != Some(events.len()) {
        return Err(EffectError::EffectProvenanceMismatch(format!(
            "atomic effect group '{group_id}' has an invalid or repeated physical-frame size"
        )));
    }
    for (expected_index, (position, member, _)) in events.iter().enumerate() {
        let expected_index = u32::try_from(expected_index).map_err(|_| {
            EffectError::EffectProvenanceMismatch(format!(
                "atomic effect group '{group_id}' exceeds u32 member capacity"
            ))
        })?;
        if *position != first_position.saturating_add(expected_index as usize)
            || *member
                != Some(JournalGroupMember {
                    index: expected_index,
                    size: first.size,
                })
        {
            return Err(EffectError::EffectProvenanceMismatch(format!(
                "atomic effect group '{group_id}' has non-contiguous, missing, or duplicate frame membership"
            )));
        }
    }
    Ok(())
}

fn infer_recorded_flow_id(records: &[EffectRecord]) -> Result<Option<RecordedFlowId>, EffectError> {
    let Some(first) = records.first() else {
        return Ok(None);
    };
    let recorded_flow_id = first.cursor.recorded_flow_id.clone();
    for record in records.iter().skip(1) {
        if record.cursor.recorded_flow_id != recorded_flow_id {
            return Err(EffectError::EffectProvenanceMismatch(format!(
                "effect history contains mixed recorded flow ids: `{}` and `{}`",
                recorded_flow_id, record.cursor.recorded_flow_id
            )));
        }
    }
    Ok(Some(recorded_flow_id))
}

#[async_trait]
impl EffectHistoryReader for EffectHistory {
    async fn read_effect(
        &mut self,
        cursor: &EffectCursor,
    ) -> Result<Option<EffectRecord>, EffectError> {
        Ok(self.find(cursor).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn control_event() -> ChainEvent {
        ChainEventFactory::eof_event(WriterId::from(StageId::new()), true)
    }

    fn member(index: u32, size: u32) -> Option<JournalGroupMember> {
        Some(JournalGroupMember { index, size })
    }

    fn descriptor() -> EffectDescriptor {
        EffectDescriptor::new("test.affine", "test", 1_u32, "test-v1", "canonical")
    }

    fn failed_record(cursor: EffectCursor) -> EffectRecord {
        EffectRecord {
            cursor,
            descriptor_hash: EffectDescriptorHash::new("descriptor"),
            descriptor: descriptor(),
            outcome: EffectOutcomePayload::Failed {
                error_type: EffectFailureKind::new("test_failure"),
                error_message: "failed".to_string(),
                retry: RetryDisposition::NotRetryable,
                cause: None,
                detail: None,
            },
            origin: None,
        }
    }

    #[test]
    fn one_physical_atomic_group_has_a_closed_member_sequence() {
        let events = vec![
            (4, member(0, 2), control_event()),
            (5, member(1, 2), control_event()),
        ];

        validate_atomic_group_members("effect-escape-controls:v1:test", &events)
            .expect("one physical frame is valid");
    }

    #[test]
    fn repeated_atomic_group_identity_is_replay_corruption() {
        let events = vec![
            (4, member(0, 2), control_event()),
            (5, member(1, 2), control_event()),
            (6, member(0, 2), control_event()),
            (7, member(1, 2), control_event()),
        ];

        let error = validate_atomic_group_members("effect-escape-controls:v1:test", &events)
            .expect_err("two frames under one identity must not collapse");
        assert!(matches!(error, EffectError::EffectProvenanceMismatch(_)));
    }

    #[test]
    fn escape_batch_without_a_matching_start_is_replay_corruption() {
        let mut groups = GroupedJournalEvents::new();
        groups.insert(
            "effect-escape-controls:v1:orphan".to_string(),
            vec![(0, member(0, 1), control_event())],
        );

        let error = validate_group_grammar_parts(&HashMap::new(), &groups)
            .expect_err("an escape batch cannot invent an attempt");
        assert!(matches!(error, EffectError::EffectProvenanceMismatch(_)));
    }

    #[test]
    fn escape_batch_after_settlement_is_replay_corruption() {
        let cursor = EffectCursor::new("flow", "stage", 1_u64, 0_u32);
        let attempt = EffectAttemptOrdinal::new(1);
        let escape_group = effect_escape_controls_group_id(&cursor, attempt).to_string();
        let terminal_group = effect_outcome_group_id(&cursor).to_string();
        let mut starts = HashMap::new();
        starts.insert((cursor, attempt), 0);
        let mut groups = GroupedJournalEvents::new();
        groups.insert(terminal_group, vec![(1, member(0, 1), control_event())]);
        groups.insert(escape_group, vec![(2, member(0, 1), control_event())]);

        let error = validate_group_grammar_parts(&starts, &groups)
            .expect_err("escape evidence must precede settlement");
        assert!(matches!(error, EffectError::EffectProvenanceMismatch(_)));
    }

    #[test]
    fn terminal_before_its_highest_start_is_replay_corruption() {
        let cursor = EffectCursor::new("flow", "stage", 1_u64, 0_u32);
        let attempt = EffectAttemptOrdinal::new(1);
        let terminal_group = effect_outcome_group_id(&cursor).to_string();
        let starts = HashMap::from([((cursor, attempt), 1)]);
        let groups = HashMap::from([(terminal_group, vec![(0, member(0, 1), control_event())])]);

        let error = validate_group_grammar_parts(&starts, &groups)
            .expect_err("a terminal cannot physically precede its Start");
        assert!(matches!(error, EffectError::EffectProvenanceMismatch(_)));
    }

    #[test]
    fn attempt_history_cannot_change_its_named_causal_input() {
        let cursor = EffectCursor::new("flow", "stage", 1_u64, 0_u32);
        let first = EffectAttemptStarted {
            cursor: cursor.clone(),
            descriptor_hash: EffectDescriptorHash::new("descriptor"),
            effect_type: EffectType::new("test.affine"),
            attempt: EffectAttemptOrdinal::new(1),
            outcome_group_id: effect_outcome_group_id(&cursor),
            causal_input_id: EventId::new(),
        };
        let second = EffectAttemptStarted {
            cursor: cursor.clone(),
            descriptor_hash: EffectDescriptorHash::new("descriptor"),
            effect_type: EffectType::new("test.affine"),
            attempt: EffectAttemptOrdinal::new(2),
            outcome_group_id: effect_outcome_group_id(&cursor),
            causal_input_id: EventId::new(),
        };

        let error = validate_started_identity(&cursor, &[first, second])
            .expect_err("one cursor cannot name two causal inputs");
        assert!(matches!(error, EffectError::EffectProvenanceMismatch(_)));
    }

    #[test]
    fn affine_terminal_without_its_physical_group_is_replay_corruption() {
        let cursor = EffectCursor::new("flow", "stage", 1_u64, 0_u32);
        let history = EffectCursorHistory {
            records: vec![failed_record(cursor.clone())],
            terminal_attempt: Some(None),
            ..EffectCursorHistory::default()
        };

        let error = validate_affine_terminal_group(&cursor, &history)
            .expect_err("an affine terminal cannot be reconstructed from an ungrouped row");
        assert!(matches!(error, EffectError::EffectProvenanceMismatch(_)));
    }

    #[test]
    fn binding_invariant_terminal_requires_attempt_and_recovery_settlement_evidence() {
        let cursor = EffectCursor::new("flow", "stage", 1_u64, 0_u32);
        let error =
            validate_invariant_settlement_evidence(&cursor, EffectAttemptOrdinal::new(1), &[])
                .expect_err("a typed binding-invariant hit cannot omit completed-attempt evidence");
        assert!(
            matches!(
                error,
                EffectError::EffectProvenanceMismatch(ref message)
                    if message.contains("AttemptSettled")
            ),
            "unexpected validation error: {error:?}"
        );
    }

    #[test]
    fn out_of_order_attempt_starts_are_replay_corruption() {
        let cursor = EffectCursor::new("flow", "stage", 1_u64, 0_u32);
        let make_start = |attempt| EffectAttemptStarted {
            cursor: cursor.clone(),
            descriptor_hash: EffectDescriptorHash::new("descriptor"),
            effect_type: EffectType::new("test.affine"),
            attempt: EffectAttemptOrdinal::new(attempt),
            outcome_group_id: effect_outcome_group_id(&cursor),
            causal_input_id: EventId::new(),
        };
        let starts = vec![make_start(2), make_start(1)];
        let attempts = HashMap::from([(cursor, starts)]);

        let error = validate_attempt_grammar(
            &attempts,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &[],
        )
        .expect_err("attempt ordinals must follow physical journal order");
        assert!(matches!(error, EffectError::EffectProvenanceMismatch(_)));
    }
}
