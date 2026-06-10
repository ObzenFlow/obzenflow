// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::types::{MergeCandidateStatus, MergeWaitState, ReaderSelectionPolicy};
use super::{
    DeliveryFilter, EofOutcome, HeldHead, MergeCandidateMeta, PollResult, ReaderProgress,
    StageInputPosition, UpstreamSubscription,
};
use obzenflow_core::event::payloads::effect_payload::is_framework_effect_event_type;
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::types::SeqNo;
use obzenflow_core::event::vector_clock::CausalOrderingService;
use obzenflow_core::event::{ChainEvent, ChainEventContent, EventEnvelope, JournalEvent};
use obzenflow_core::{StageId, WriterId};
use std::any::Any;
use std::io;
use tokio::time::Instant;

impl<T> UpstreamSubscription<T>
where
    T: JournalEvent + 'static,
{
    fn feed_selected_contract_chains_on_event(
        &mut self,
        reader_index: usize,
        upstream_stage: obzenflow_core::StageId,
        event: &ChainEvent,
        reader_stage: obzenflow_core::StageId,
    ) {
        if self
            .contract_feed_chains
            .get(reader_index)
            .is_none_or(Vec::is_empty)
        {
            return;
        }

        match &event.content {
            ChainEventContent::Data { event_type, .. } => {
                let feed_reads: Vec<(usize, SeqNo)> = self
                    .contract_feed_chains
                    .get(reader_index)
                    .into_iter()
                    .flat_map(|chains| chains.iter().enumerate())
                    .filter(|(_, feed_chain)| {
                        Self::selected_feed_matches_event_type(&feed_chain.metadata, event_type)
                    })
                    .map(|(feed_index, feed_chain)| {
                        (
                            feed_index,
                            self.selected_reader_seq_for_feed(reader_index, &feed_chain.metadata),
                        )
                    })
                    .collect();

                if let Some(chains) = self.contract_feed_chains.get_mut(reader_index) {
                    for (feed_index, reader_seq) in feed_reads {
                        if let Some(feed_chain) = chains.get_mut(feed_index) {
                            feed_chain.chain.on_read(
                                event,
                                reader_stage,
                                reader_seq,
                                upstream_stage,
                            );
                        }
                    }
                }
            }
            ChainEventContent::FlowControl(FlowControlPayload::Eof {
                writer_seq_by_event_type,
                ..
            }) if !writer_seq_by_event_type.is_empty() => {
                let feed_writes: Vec<(usize, SeqNo)> = self
                    .contract_feed_chains
                    .get(reader_index)
                    .into_iter()
                    .flat_map(|chains| chains.iter().enumerate())
                    .filter_map(|(feed_index, feed_chain)| {
                        writer_seq_by_event_type
                            .iter()
                            .find(|(event_type, _)| {
                                Self::selected_feed_matches_event_type(
                                    &feed_chain.metadata,
                                    event_type.as_str(),
                                )
                            })
                            .map(|(_, writer_seq)| (feed_index, *writer_seq))
                    })
                    .collect();

                if let Some(chains) = self.contract_feed_chains.get_mut(reader_index) {
                    for (feed_index, writer_seq) in feed_writes {
                        if let Some(feed_chain) = chains.get_mut(feed_index) {
                            let mut feed_event = event.clone();
                            if let ChainEventContent::FlowControl(FlowControlPayload::Eof {
                                writer_seq: eof_writer_seq,
                                ..
                            }) = &mut feed_event.content
                            {
                                *eof_writer_seq = Some(writer_seq);
                            }
                            feed_chain
                                .chain
                                .on_write(&feed_event, upstream_stage, SeqNo(0));
                        }
                    }
                }
            }
            _ => {}
        }
    }

    /// Poll for the next event without blocking
    ///
    /// This method tries once through the readers and returns immediately.
    /// The caller (FSM) decides whether to retry, sleep, or transition states.
    /// `fsm_state` is the caller's current FSM state (for diagnostics).
    ///
    /// Delivery order depends on the configured `ReaderSelectionPolicy`
    /// (FLOWIP-095d): availability-driven round-robin by default, or the
    /// canonical deterministic merge on ordered stages.
    pub async fn poll_next_with_state(
        &mut self,
        fsm_state: &str,
        reader_progress: Option<&mut [ReaderProgress]>,
    ) -> PollResult<T> {
        match self.reader_selection {
            ReaderSelectionPolicy::AvailabilityRoundRobin => {
                self.poll_round_robin(fsm_state, reader_progress).await
            }
            ReaderSelectionPolicy::CanonicalMerge => {
                self.poll_canonical_merge(fsm_state, reader_progress).await
            }
        }
    }

    /// Availability-driven round-robin polling (the historical behaviour).
    ///
    /// Cycles through readers and delivers whatever is available first, so
    /// delivery order depends on arrival timing.
    async fn poll_round_robin(
        &mut self,
        fsm_state: &str,
        reader_progress: Option<&mut [ReaderProgress]>,
    ) -> PollResult<T> {
        // Check for buffered events first (future enhancement)
        // For now, we don't buffer events

        if self.readers.is_empty() {
            tracing::error!("poll_next() called with no upstream readers");
            return PollResult::Error(Box::new(io::Error::other("No upstream readers configured")));
        }

        let total_readers = self.readers.len();
        let starting_index = self.state.current_reader_index;

        tracing::debug!(
            target: "flowip-080o",
            owner = %self.owner_label,
            total_readers = total_readers,
            starting_index = starting_index,
            eof_status = ?self.state.eof_received,
            fsm_state = fsm_state,
            "subscription: poll_next() starting round-robin"
        );

        // Try each reader once in round-robin fashion
        let mut readers_checked = 0;
        loop {
            let current_index = self.state.current_reader_index;

            tracing::debug!(
                target: "flowip-080o",
                owner = %self.owner_label,
                current_index = current_index,
                starting_index = starting_index,
                readers_checked = readers_checked,
                is_eof = self.state.is_reader_eof(current_index),
                fsm_state = fsm_state,
                "subscription: checking reader in round-robin"
            );

            // Skip EOF'd readers
            if self.state.is_reader_eof(current_index) {
                tracing::trace!(
                    target: "flowip-080o",
                    owner = %self.owner_label,
                    current_index = current_index,
                    "subscription: skipping EOF'd reader"
                );
                self.state.next_reader_index();

                // Have we checked all readers?
                if self.state.current_reader_index == starting_index {
                    tracing::debug!(
                        target: "flowip-080o",
                        owner = %self.owner_label,
                        "subscription: back to starting_index after skipping EOF'd readers, breaking"
                    );
                    break;
                }
                continue;
            }

            let (stage_id, stage_name, reader) = &mut self.readers[current_index];
            let stage_id = *stage_id;
            let stage_name = stage_name.clone();

            tracing::debug!(
                target: "flowip-080o",
                owner = %self.owner_label,
                stage_id = ?stage_id,
                stage_name = %stage_name,
                current_index = current_index,
                fsm_state = fsm_state,
                "subscription: calling reader.next()"
            );

            match reader.next().await {
                Ok(Some(envelope)) => {
                    // This reader has observed post-baseline data; it is no longer
                    // logically at EOF due to a tail-start baseline.
                    self.state.clear_reader_baseline_at_tail(current_index);

                    // Stage runtime subscriptions should not deliver upstream observability
                    // events to handlers. We still consume them from journals so that the
                    // subscription can make progress toward transport events and EOF.
                    if let Some(chain_event) =
                        (&envelope.event as &dyn Any).downcast_ref::<ChainEvent>()
                    {
                        if self.transport_filter_skips(chain_event, stage_id) {
                            // Filtered events are not part of the transport stream.
                            // Skip delivering to the caller, but keep progressing
                            // (round-robin advances to the next reader on a skip).
                            self.state.next_reader_index();
                            continue;
                        }
                    }

                    let chain_event_view =
                        (&envelope.event as &dyn Any).downcast_ref::<ChainEvent>();

                    tracing::debug!(
                        target: "flowip-080o",
                        owner = %self.owner_label,
                        stage_id = ?stage_id,
                        stage_name = %stage_name,
                        current_index = current_index,
                        fsm_state = fsm_state,
                        event_type = %envelope.event.event_type_name(),
                        "subscription: reader.next() returned Some(event)"
                    );
                    let (is_eof, is_drain) = chain_event_view
                        .map(|chain_event| Self::classify_eof_drain(chain_event, stage_id))
                        .unwrap_or((false, false));

                    // Advance to next reader for fairness, then run the shared
                    // delivery-side accounting (the FLOWIP-095d read/delivery
                    // split; round-robin reads and delivers in the same poll).
                    self.state.next_reader_index();
                    return self.deliver_from_reader(
                        current_index,
                        envelope,
                        is_eof,
                        is_drain,
                        fsm_state,
                        reader_progress,
                        true,
                    );
                }
                Ok(None) => {
                    // No events from this reader right now
                    tracing::debug!(
                        target: "flowip-080o",
                        owner = %self.owner_label,
                        stage_id = ?stage_id,
                        current_index = current_index,
                        fsm_state = fsm_state,
                        "subscription: reader.next() returned None (no events available)"
                    );
                    tracing::debug!(
                        "No events from stage {:?} (reader {}/{})",
                        stage_id,
                        current_index + 1,
                        total_readers
                    );
                    readers_checked += 1;
                }
                Err(e) => {
                    tracing::error!(
                        target: "flowip-080o",
                        owner = %self.owner_label,
                        stage_id = ?stage_id,
                        current_index = current_index,
                        error = %e,
                        "subscription: reader.next() returned Error"
                    );
                    tracing::error!("Error reading from stage {:?}: {}", stage_id, e);
                    return PollResult::Error(Box::new(e));
                }
            }

            // Move to next reader
            let old_index = self.state.current_reader_index;
            self.state.next_reader_index();
            let new_index = self.state.current_reader_index;

            tracing::debug!(
                target: "flowip-080o",
                owner = %self.owner_label,
                old_index = old_index,
                new_index = new_index,
                starting_index = starting_index,
                fsm_state = fsm_state,
                "subscription: advanced to next reader"
            );

            // Have we tried all readers once?
            if self.state.current_reader_index == starting_index {
                tracing::debug!(
                    target: "flowip-080o",
                    owner = %self.owner_label,
                    readers_checked = readers_checked,
                    total_readers = total_readers,
                    "subscription: completed one round-robin cycle, breaking"
                );
                break;
            }
        }

        // No events available right now
        tracing::debug!(
            target: "flowip-080o",
            owner = %self.owner_label,
            eof_count = self.state.eof_count(),
            total_readers = self.state.eof_received.len(),
            readers_checked = readers_checked,
            "subscription: returning NoEvents - no events available in this poll cycle"
        );
        tracing::debug!(
            target: "flowip-080o",
            "No events available. EOF status: {}/{} readers complete",
            self.state.eof_count(),
            self.state.eof_received.len()
        );
        PollResult::NoEvents
    }

    /// Detect terminal EOF and drain signals for one upstream reader.
    ///
    /// Important subtlety (FLOWIP-080o):
    /// - Journals can contain EOFs authored by *other* stages that were merely
    ///   forwarded (e.g., a join forwarding source EOFs).
    /// - Contracts and EOF accounting for this subscription must only treat
    ///   EOFs authored by the upstream stage associated with this reader as
    ///   terminal. Otherwise, downstream stages can observe "early EOF"
    ///   and stop consuming before the real writer has finished.
    ///
    /// Some historical or forwarded EOF events may omit the payload writer_id.
    /// In that case, fall back to the ChainEvent's writer_id, which still
    /// distinguishes "authored here" from "forwarded from elsewhere".
    fn classify_eof_drain(chain_event: &ChainEvent, stage_id: StageId) -> (bool, bool) {
        match &chain_event.content {
            ChainEventContent::FlowControl(FlowControlPayload::Eof { writer_id, .. }) => {
                let authored_by_upstream = match writer_id {
                    Some(WriterId::Stage(eof_stage)) => *eof_stage == stage_id,
                    Some(_) => false,
                    None => match &chain_event.writer_id {
                        WriterId::Stage(eof_stage) => *eof_stage == stage_id,
                        _ => false,
                    },
                };
                (authored_by_upstream, false)
            }
            ChainEventContent::FlowControl(FlowControlPayload::Drain) => (false, true),
            _ => (false, false),
        }
    }

    /// Read-side transport filtering decision (FLOWIP-095d D8 split).
    ///
    /// Filtered events are consumed from the journal but never become heads,
    /// take no ordinals, and are not delivered. The filter applies identically
    /// in live and replay runs, which is what keeps per-input ordinals aligned
    /// across runs.
    fn transport_filter_skips(&self, chain_event: &ChainEvent, stage_id: StageId) -> bool {
        if !matches!(self.delivery_filter, DeliveryFilter::TransportOnly) {
            return false;
        }
        if matches!(chain_event.content, ChainEventContent::Observability(_)) {
            return true;
        }
        if let ChainEventContent::Data { event_type, .. } = &chain_event.content {
            if chain_event
                .effect_provenance
                .as_ref()
                .is_some_and(|provenance| provenance.fact_owner.is_framework())
                && is_framework_effect_event_type(event_type)
            {
                return true;
            }
            if !self.data_event_selected_for_stage(stage_id, event_type) {
                return true;
            }
        }
        false
    }

    /// Read-side stall-attribution timestamp (FLOWIP-095d D8 split).
    ///
    /// Acquiring a head proves the upstream is alive, so the read instant is
    /// stamped at acquisition. A reader whose head is held while the merge
    /// waits on a sibling stays Healthy; stall detection keeps pointing at the
    /// actually-quiet upstream.
    fn note_read_instant(
        &self,
        reader_progress: &mut Option<&mut [ReaderProgress]>,
        reader_index: usize,
        is_data: bool,
    ) {
        if !is_data || self.contract_tracker.is_none() {
            return;
        }
        if let Some(progress_slice) = reader_progress.as_deref_mut() {
            if let Some(progress) = progress_slice.get_mut(reader_index) {
                progress.last_read_instant = Some(Instant::now());
            }
        }
    }

    /// Delivery-side accounting shared by both reader-selection policies
    /// (FLOWIP-095d D8 split).
    ///
    /// Everything that must observe exactly the delivered sequence in
    /// delivered order lives here: reader_seq and receipt tracking, advertised
    /// EOF evidence, contract chain on_read/on_write, selected-data counters,
    /// EOF exhaustion accounting, `StageInputPosition` assignment, and the
    /// per-reader delivered ordinal. Exhaustion happens strictly at EOF
    /// delivery, never when an EOF head is merely held.
    #[allow(clippy::too_many_arguments)]
    fn deliver_from_reader(
        &mut self,
        reader_index: usize,
        envelope: EventEnvelope<T>,
        is_eof: bool,
        is_drain: bool,
        fsm_state: &str,
        reader_progress: Option<&mut [ReaderProgress]>,
        set_read_instant: bool,
    ) -> PollResult<T> {
        let total_readers = self.readers.len();
        let (stage_id, stage_name) = {
            let (id, name, _) = &self.readers[reader_index];
            (*id, name.clone())
        };
        let original_chain_event = (&envelope.event as &dyn Any).downcast_ref::<ChainEvent>();

        let mut normalized_contract_event = None;
        if is_eof && self.has_selected_event_type_filter(stage_id) {
            if let Some(chain_event) = original_chain_event {
                let selected_writer_seq =
                    self.selected_writer_seq_for_reader(reader_index, stage_id);
                let mut normalized = chain_event.clone();
                if let ChainEventContent::FlowControl(FlowControlPayload::Eof {
                    writer_seq,
                    writer_seq_by_event_type,
                    ..
                }) = &mut normalized.content
                {
                    *writer_seq = self
                        .selected_writer_seq_from_eof_map(stage_id, writer_seq_by_event_type)
                        .or(Some(selected_writer_seq));
                }
                normalized_contract_event = Some(normalized);
            }
        }
        let contract_chain_event = normalized_contract_event.as_ref().or(original_chain_event);

        if let Some(ChainEventContent::Data { event_type, .. }) =
            original_chain_event.map(|event| &event.content)
        {
            if let Some(selected_seq) = self.selected_data_seq_by_reader.get_mut(reader_index) {
                selected_seq.0 = selected_seq.0.saturating_add(1);
            }
            if let Some(by_type) = self
                .selected_data_seq_by_reader_event_type
                .get_mut(reader_index)
            {
                by_type.increment(event_type.clone());
            }
        }

        // Update legacy contract tracking state if enabled and
        // contract progress has been provided by the owning FSM
        // context. Reader progress lives in FSM contexts; we only
        // update it here so that later contract checks can emit
        // progress/final events based on the same state during
        // replay.
        let mut reader_seq_for_contracts: Option<SeqNo> = None;
        if self.contract_tracker.is_some() {
            if let Some(progress_slice) = reader_progress {
                if let Some(progress) = progress_slice.get_mut(reader_index) {
                    // Update progress for data events
                    if let Some(chain_event) = contract_chain_event {
                        if chain_event.is_data() {
                            progress.reader_seq.0 += 1;
                            if set_read_instant {
                                progress.last_read_instant = Some(Instant::now());
                            }
                            if self.uses_receipt_watermark() {
                                progress.track_pending_receipt(
                                    *envelope.event.id(),
                                    chain_event.clone(),
                                    envelope.vector_clock.clone(),
                                );
                            } else {
                                progress.receipted_seq = progress.reader_seq;
                                progress.last_receipted_event_id = Some(*envelope.event.id());
                                progress.last_receipted_vector_clock =
                                    Some(envelope.vector_clock.clone());
                            }
                        }

                        // Capture advertised positions from EOF authored by this upstream
                        if let ChainEventContent::FlowControl(FlowControlPayload::Eof {
                            writer_id,
                            writer_seq,
                            writer_seq_by_event_type,
                            vector_clock,
                            ..
                        }) = &chain_event.content
                        {
                            let authored_by_upstream = match writer_id {
                                Some(WriterId::Stage(eof_stage)) => *eof_stage == stage_id,
                                Some(_) => false,
                                None => match &chain_event.writer_id {
                                    WriterId::Stage(eof_stage) => *eof_stage == stage_id,
                                    _ => false,
                                },
                            };

                            if authored_by_upstream {
                                progress.advertised_writer_seq = *writer_seq;
                                progress.last_vector_clock = vector_clock.clone();
                                if let Some(by_type) = self
                                    .advertised_writer_seq_by_reader_event_type
                                    .get_mut(reader_index)
                                {
                                    by_type.replace_from_eof(writer_seq_by_event_type);
                                }
                            }
                        }
                    }

                    progress.last_event_id = Some(*envelope.event.id());
                    if (&envelope.event as &dyn Any)
                        .downcast_ref::<ChainEvent>()
                        .is_some()
                    {
                        progress.last_vector_clock = Some(envelope.vector_clock.clone());
                    }

                    // Capture the updated reader_seq for contract chains.
                    reader_seq_for_contracts = Some(progress.reader_seq);
                }
            }
        }

        // Feed the event into the ContractChain for this edge, if configured.
        if let (Some(chain_event), Some(reader_stage)) = (
            contract_chain_event,
            self.contract_tracker.as_ref().and_then(|t| t.reader_stage),
        ) {
            if let Some(chain_slot) = self.contract_chains.get_mut(reader_index) {
                if let Some(chain) = chain_slot.as_mut() {
                    let reader_seq = reader_seq_for_contracts.unwrap_or(SeqNo(0));
                    // From the contract framework's perspective, the
                    // upstream writer is `stage_id` and the
                    // downstream reader is `reader_stage`.
                    chain.on_read(chain_event, reader_stage, reader_seq, stage_id);

                    // Also feed the event into the writer side of
                    // the transport contract via `on_write`. For
                    // transport, the writer-side count is derived
                    // from EOF writer_seq; `TransportContract`
                    // ignores non-EOF events on the write side.
                    chain.on_write(chain_event, stage_id, SeqNo(0));
                }
            }

            self.feed_selected_contract_chains_on_event(
                reader_index,
                stage_id,
                chain_event,
                reader_stage,
            );
        }

        tracing::debug!(
            target: "flowip-080o",
            owner = %self.owner_label,
            stage_id = ?stage_id,
            stage_name = %stage_name,
            reader_index = reader_index,
            fsm_state = fsm_state,
            event_type = %envelope.event.event_type_name(),
            is_eof = is_eof,
            "subscription: received event"
        );

        if is_eof {
            self.state.mark_reader_eof(reader_index);
            let eof_count = self.state.eof_count();
            let is_final = eof_count == total_readers;
            let outcome = EofOutcome {
                stage_id,
                stage_name: stage_name.clone(),
                reader_index,
                eof_count,
                total_readers,
                is_final,
            };
            self.last_eof_outcome = Some(outcome.clone());
            tracing::debug!(
                target: "flowip-080o",
                owner = %self.owner_label,
                stage_id = ?stage_id,
                stage_name = %stage_name,
                reader_index = reader_index,
                total_readers = total_readers,
                eof_status = ?self.state.eof_received,
                is_final = is_final,
                "Received EOF from stage"
            );
        } else if is_drain {
            tracing::debug!(
                target: "flowip-080o",
                owner = %self.owner_label,
                stage_id = ?stage_id,
                stage_name = %stage_name,
                reader_index = reader_index,
                total_readers = total_readers,
                "Received drain from stage — continuing to consume until EOF"
            );
        }

        tracing::debug!(
            "Received event from stage {:?} (reader {}/{})",
            stage_id,
            reader_index + 1,
            total_readers
        );

        let delivered_stage_input_position = if let Some(chain_event) =
            (&envelope.event as &dyn Any).downcast_ref::<ChainEvent>()
        {
            if chain_event.is_data() {
                let position = StageInputPosition(self.next_stage_input_position);
                self.next_stage_input_position = self.next_stage_input_position.saturating_add(1);
                Some(position)
            } else {
                None
            }
        } else {
            None
        };

        // FLOWIP-095d: every delivered transport event takes a per-reader
        // ordinal, post-filter, on the delivery side. This counter is the
        // canonical merge's tiebreak source and its checkpointable state.
        if let Some(delivered) = self.delivered_seq_by_reader.get_mut(reader_index) {
            *delivered = delivered.saturating_add(1);
        }
        self.last_merge_wait = None;
        self.merge_candidate_index = None;

        self.last_delivered_upstream_stage = Some(stage_id);
        self.last_delivered_stage_input_position = delivered_stage_input_position;

        PollResult::Event(envelope)
    }

    /// Acquire heads for every non-exhausted reader and select the canonical
    /// merge candidate (FLOWIP-095d).
    ///
    /// The Kahn discipline: while any non-exhausted reader has no head, the
    /// merge decides nothing. Heads are acquired post-filter; filtered events
    /// are drained from the journal without becoming heads or taking ordinals.
    pub async fn ensure_merge_candidate(
        &mut self,
        fsm_state: &str,
        reader_progress: &mut Option<&mut [ReaderProgress]>,
    ) -> std::result::Result<MergeCandidateStatus, Box<dyn std::error::Error + Send + Sync>> {
        for index in 0..self.readers.len() {
            if self.state.is_reader_eof(index) || self.held_heads[index].is_some() {
                continue;
            }
            let stage_id = self.readers[index].0;
            loop {
                let next = {
                    let (_, _, reader) = &mut self.readers[index];
                    reader.next().await
                };
                match next {
                    Ok(Some(envelope)) => {
                        self.state.clear_reader_baseline_at_tail(index);
                        let chain_event_view =
                            (&envelope.event as &dyn Any).downcast_ref::<ChainEvent>();
                        if let Some(chain_event) = chain_event_view {
                            if self.transport_filter_skips(chain_event, stage_id) {
                                continue;
                            }
                        }
                        let (is_authored_eof, is_drain) = chain_event_view
                            .map(|chain_event| Self::classify_eof_drain(chain_event, stage_id))
                            .unwrap_or((false, false));
                        let is_data = chain_event_view
                            .map(|chain_event| chain_event.is_data())
                            .unwrap_or(false);
                        self.note_read_instant(reader_progress, index, is_data);
                        tracing::debug!(
                            target: "flowip-095d",
                            owner = %self.owner_label,
                            reader_index = index,
                            fsm_state = fsm_state,
                            event_type = %envelope.event.event_type_name(),
                            is_authored_eof = is_authored_eof,
                            "canonical merge: acquired head"
                        );
                        self.held_heads[index] = Some(HeldHead {
                            envelope,
                            is_authored_eof,
                            is_drain,
                        });
                        break;
                    }
                    Ok(None) => break,
                    Err(e) => {
                        tracing::error!(
                            target: "flowip-095d",
                            owner = %self.owner_label,
                            reader_index = index,
                            error = %e,
                            "canonical merge: reader.next() returned Error"
                        );
                        return Err(Box::new(e));
                    }
                }
            }
        }

        let quiet_inputs: Vec<(StageId, String)> = (0..self.readers.len())
            .filter(|&index| !self.state.is_reader_eof(index) && self.held_heads[index].is_none())
            .map(|index| {
                let (id, name, _) = &self.readers[index];
                (*id, name.clone())
            })
            .collect();
        if !quiet_inputs.is_empty() {
            tracing::debug!(
                target: "flowip-095d",
                owner = %self.owner_label,
                quiet = ?quiet_inputs.iter().map(|(_, name)| name).collect::<Vec<_>>(),
                fsm_state = fsm_state,
                "canonical merge: waiting on quiet input(s)"
            );
            self.merge_candidate_index = None;
            self.last_merge_wait = Some(MergeWaitState { quiet_inputs });
            return Ok(MergeCandidateStatus::Quiet);
        }
        self.last_merge_wait = None;

        let candidates: Vec<usize> = (0..self.held_heads.len())
            .filter(|&index| self.held_heads[index].is_some())
            .collect();
        if candidates.is_empty() {
            self.merge_candidate_index = None;
            return Ok(MergeCandidateStatus::AllExhausted);
        }

        // Causality among heads: a non-EOF head is excluded while another
        // non-EOF head happened-before it. Authored EOFs are exempt and order
        // by the tiebreak alone. Happened-before is acyclic, so the candidate
        // set cannot empty; the fallback below is defensive only.
        let head = |index: usize| self.held_heads[index].as_ref().expect("candidate has head");
        let winner = candidates
            .iter()
            .copied()
            .filter(|&index| {
                if head(index).is_authored_eof {
                    return true;
                }
                !candidates.iter().copied().any(|other| {
                    other != index
                        && !head(other).is_authored_eof
                        && CausalOrderingService::happened_before(
                            &head(other).envelope.vector_clock,
                            &head(index).envelope.vector_clock,
                        )
                })
            })
            .min_by(|&a, &b| {
                (
                    self.delivered_seq_by_reader[a],
                    &self.reader_tiebreak_keys[a],
                )
                    .cmp(&(
                        self.delivered_seq_by_reader[b],
                        &self.reader_tiebreak_keys[b],
                    ))
            })
            .or_else(|| {
                tracing::error!(
                    target: "flowip-095d",
                    owner = %self.owner_label,
                    "canonical merge: causality exclusion emptied the candidate set; \
                     falling back to tiebreak-only selection (still deterministic)"
                );
                candidates.iter().copied().min_by(|&a, &b| {
                    (
                        self.delivered_seq_by_reader[a],
                        &self.reader_tiebreak_keys[a],
                    )
                        .cmp(&(
                            self.delivered_seq_by_reader[b],
                            &self.reader_tiebreak_keys[b],
                        ))
                })
            });

        self.merge_candidate_index = winner;
        Ok(MergeCandidateStatus::Candidate)
    }

    /// Comparison metadata for the currently selected merge candidate, if any.
    ///
    /// The join supervisor uses this to compose two subscriptions: each side
    /// selects its internal winner, and the cross-side choice applies the same
    /// (causality, then ordinal/stage-key/feed-identity) rule to the two metas.
    pub fn merge_candidate(&self) -> Option<MergeCandidateMeta<'_>> {
        let index = self.merge_candidate_index?;
        let head = self.held_heads.get(index)?.as_ref()?;
        let (stage_key, feed_identity) = self.reader_tiebreak_keys.get(index)?;
        Some(MergeCandidateMeta {
            ordinal: self.delivered_seq_by_reader[index].saturating_add(1),
            stage_key,
            feed_identity,
            vector_clock: &head.envelope.vector_clock,
            is_authored_eof: head.is_authored_eof,
        })
    }

    /// Deliver the selected merge candidate through the shared delivery-side
    /// accounting. Returns `NoEvents` if no candidate is selected.
    pub fn take_merge_candidate(
        &mut self,
        fsm_state: &str,
        reader_progress: Option<&mut [ReaderProgress]>,
    ) -> PollResult<T> {
        let Some(index) = self.merge_candidate_index.take() else {
            return PollResult::NoEvents;
        };
        let Some(head) = self.held_heads.get_mut(index).and_then(|slot| slot.take()) else {
            return PollResult::NoEvents;
        };
        self.deliver_from_reader(
            index,
            head.envelope,
            head.is_authored_eof,
            head.is_drain,
            fsm_state,
            reader_progress,
            false,
        )
    }

    /// Canonical deterministic merge polling (FLOWIP-095d).
    ///
    /// The wait is a poll outcome, never a blocking await: a quiet input makes
    /// this return `NoEvents` with `merge_wait()` naming the awaited inputs,
    /// and the supervisor dispatch loop keeps cycling, so heartbeats, contract
    /// ticks, drain handling, and shutdown stay live while the merge waits.
    async fn poll_canonical_merge(
        &mut self,
        fsm_state: &str,
        mut reader_progress: Option<&mut [ReaderProgress]>,
    ) -> PollResult<T> {
        if self.readers.is_empty() {
            tracing::error!("poll_next() called with no upstream readers");
            return PollResult::Error(Box::new(io::Error::other("No upstream readers configured")));
        }

        match self
            .ensure_merge_candidate(fsm_state, &mut reader_progress)
            .await
        {
            Err(e) => PollResult::Error(e),
            Ok(MergeCandidateStatus::Quiet) | Ok(MergeCandidateStatus::AllExhausted) => {
                PollResult::NoEvents
            }
            Ok(MergeCandidateStatus::Candidate) => {
                self.take_merge_candidate(fsm_state, reader_progress)
            }
        }
    }

    /// Backwards-compatible wrapper when FSM state is not provided
    pub async fn poll_next(&mut self) -> PollResult<T> {
        self.poll_next_with_state("unknown", None).await
    }
}
