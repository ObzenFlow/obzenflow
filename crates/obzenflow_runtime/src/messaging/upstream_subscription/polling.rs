// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::types::{
    MergeCandidateStatus, MergeWaitState, ReaderSelectionPolicy, ReaderTiebreakKey, StageKey,
};
use super::{
    DeliveryFilter, EofOutcome, HeldHead, MergeCandidateMeta, PollResult, ReaderProgress,
    StageInputPosition, UpstreamSubscription,
};
use obzenflow_core::event::payloads::effect_payload::is_framework_effect_event_type;
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::types::SeqNo;
use obzenflow_core::event::vector_clock::CausalOrderingService;
use obzenflow_core::event::{ChainEvent, ChainEventContent, EventEnvelope, JournalEvent};
use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_core::{StageId, WriterId};
use std::any::Any;
use std::io;
use tokio::time::Instant;

/// Outcome of one read-side step on a single reader (FLOWIP-095d D8 split).
///
/// Both reader-selection policies consume journals through exactly this step,
/// so a read-side rule (baseline clearing, transport filtering, EOF/drain
/// classification) can never drift between them.
enum ReadStep<T: JournalEvent> {
    /// A deliverable transport head, classified at read time.
    Head { head: HeldHead<T>, is_data: bool },
    /// A row consumed from the journal and filtered read-side: it never
    /// becomes a head, takes no ordinal, and is not delivered.
    Filtered,
    /// Nothing available from this reader right now.
    Empty,
}

impl<T> UpstreamSubscription<T>
where
    T: JournalEvent + 'static,
{
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

    /// Backwards-compatible wrapper when FSM state is not provided
    pub async fn poll_next(&mut self) -> PollResult<T> {
        self.poll_next_with_state("unknown", None).await
    }

    fn no_readers_error(&self) -> PollResult<T> {
        tracing::error!("poll_next() called with no upstream readers");
        PollResult::Error(Box::new(io::Error::other("No upstream readers configured")))
    }

    // ------------------------------------------------------------------
    // Read side (FLOWIP-095d D8 split)
    // ------------------------------------------------------------------

    /// One read-side step for a single reader: read the next journal record,
    /// clear the tail-start baseline, apply the transport filter, and
    /// classify EOF/drain authorship.
    async fn read_transport_step(
        &mut self,
        index: usize,
    ) -> std::result::Result<ReadStep<T>, JournalError> {
        let stage_id = self.readers[index].stage_id;
        let next = self.readers[index].reader.next().await?;
        let Some(envelope) = next else {
            return Ok(ReadStep::Empty);
        };

        // The reader has observed post-baseline data; it is no longer
        // logically at EOF due to a tail-start baseline.
        self.state.clear_reader_baseline_at_tail(index);

        let chain_event = (&envelope.event as &dyn Any).downcast_ref::<ChainEvent>();
        if let Some(chain_event) = chain_event {
            if self.transport_filter_skips(chain_event, stage_id) {
                return Ok(ReadStep::Filtered);
            }
        }

        let (is_authored_eof, is_drain) = chain_event
            .map(|chain_event| Self::classify_eof_drain(chain_event, stage_id))
            .unwrap_or((false, false));
        let catch_up = match chain_event.map(|chain_event| &chain_event.content) {
            Some(ChainEventContent::FlowControl(FlowControlPayload::CatchUpComplete {
                generation,
                stage_key,
            })) => {
                // FLOWIP-120n F8: the watermark is re-admitted on replay with
                // its original-run StageId preserved, so authorship is matched
                // by the arrival edge's stable stage_key, never by StageId. A
                // forwarded marker fails loud.
                if stage_key.as_str() != self.readers[index].stage_key.as_str() {
                    return Err(JournalError::Implementation {
                        message: format!(
                            "catch-up watermark authored by '{}' arrived on edge '{}'; \
                             markers are authored per edge and never forwarded (FLOWIP-120n F8)",
                            stage_key.as_str(),
                            self.readers[index].stage_key.as_str()
                        ),
                        source: "forwarded CatchUpComplete marker".into(),
                    });
                }
                Some(*generation)
            }
            _ => None,
        };
        let is_data = chain_event.map(ChainEvent::is_data).unwrap_or(false);
        // FLOWIP-120n F18: re-admittable rows own a cross-run-stable sequence;
        // re-authored control rows order by journal position instead.
        let orders_by_own_seq = chain_event
            .map(ChainEvent::is_source_replayable)
            .unwrap_or(true);
        Ok(ReadStep::Head {
            head: HeldHead {
                envelope,
                is_authored_eof,
                is_drain,
                catch_up,
                orders_by_own_seq,
            },
            is_data,
        })
    }

    /// Whether an EOF row was authored by this reader's upstream stage.
    ///
    /// Important subtlety (FLOWIP-080o): journals can contain EOFs authored by
    /// *other* stages that were merely forwarded (e.g., a join forwarding
    /// source EOFs). Contracts and EOF accounting must only treat EOFs
    /// authored by the upstream associated with this reader as terminal,
    /// otherwise downstream stages observe "early EOF" and stop consuming
    /// before the real writer has finished. Some historical or forwarded EOF
    /// events omit the payload writer_id; fall back to the ChainEvent's
    /// writer_id, which still distinguishes "authored here" from "forwarded
    /// from elsewhere".
    fn eof_authored_by_upstream(chain_event: &ChainEvent, stage_id: StageId) -> bool {
        let ChainEventContent::FlowControl(FlowControlPayload::Eof { writer_id, .. }) =
            &chain_event.content
        else {
            return false;
        };
        match writer_id {
            Some(WriterId::Stage(eof_stage)) => *eof_stage == stage_id,
            Some(_) => false,
            None => {
                matches!(&chain_event.writer_id, WriterId::Stage(eof_stage) if *eof_stage == stage_id)
            }
        }
    }

    /// Detect terminal EOF and drain signals for one upstream reader.
    fn classify_eof_drain(chain_event: &ChainEvent, stage_id: StageId) -> (bool, bool) {
        match &chain_event.content {
            ChainEventContent::FlowControl(FlowControlPayload::Eof { .. }) => {
                (Self::eof_authored_by_upstream(chain_event, stage_id), false)
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
    ///
    /// Reader-telemetry flow control (consumption progress, gaps, finals,
    /// stalls, at-least-once violations) is filtered here for the same reason
    /// observability events are: those rows are emitted at wall-clock-gated
    /// times, so their journal positions are not a function of stream content.
    /// Admitting them to delivery would make per-reader delivered ordinals
    /// timing-dependent and break the canonical merge's determinism. Only
    /// behavioural flow control (EOF, drain, watermarks, checkpoints,
    /// pipeline abort, source contracts) participates in transport order.
    fn transport_filter_skips(&self, chain_event: &ChainEvent, stage_id: StageId) -> bool {
        if !matches!(self.delivery_filter, DeliveryFilter::TransportOnly) {
            return false;
        }
        if matches!(chain_event.content, ChainEventContent::Observability(_)) {
            return true;
        }
        if let ChainEventContent::FlowControl(payload) = &chain_event.content {
            if payload.is_reader_telemetry() {
                return true;
            }
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

    // ------------------------------------------------------------------
    // Availability-driven round-robin (the default policy)
    // ------------------------------------------------------------------

    /// Availability-driven round-robin polling (the historical behaviour).
    ///
    /// Cycles through readers and delivers whatever is available first, so
    /// delivery order depends on arrival timing.
    async fn poll_round_robin(
        &mut self,
        fsm_state: &str,
        reader_progress: Option<&mut [ReaderProgress]>,
    ) -> PollResult<T> {
        if self.readers.is_empty() {
            return self.no_readers_error();
        }
        let starting_index = self.state.current_reader_index;

        tracing::debug!(
            target: "flowip-080o",
            owner = %self.owner_label,
            starting_index = starting_index,
            eof_status = ?self.state.eof_received,
            fsm_state = fsm_state,
            "subscription: poll_next() starting round-robin"
        );

        loop {
            let index = self.state.current_reader_index;

            if self.state.is_reader_eof(index) {
                if self.advance_cycle(starting_index) {
                    break;
                }
                continue;
            }

            match self.read_transport_step(index).await {
                Err(e) => {
                    tracing::error!(
                        target: "flowip-080o",
                        owner = %self.owner_label,
                        reader_index = index,
                        error = %e,
                        "subscription: reader.next() returned Error"
                    );
                    return PollResult::Error(Box::new(e));
                }
                Ok(ReadStep::Filtered) => {
                    // A filtered row is consumed without ending the poll
                    // cycle, so a run of filtered rows cannot make a poll
                    // report NoEvents while deliverable events remain.
                    self.state.next_reader_index();
                }
                Ok(ReadStep::Empty) => {
                    if self.advance_cycle(starting_index) {
                        break;
                    }
                }
                Ok(ReadStep::Head { head, .. }) => {
                    // Advance for fairness, then run the shared delivery-side
                    // accounting (the FLOWIP-095d read/delivery split;
                    // round-robin reads and delivers in the same poll, so the
                    // read instant stamps at delivery).
                    self.state.next_reader_index();
                    return self.deliver_from_reader(index, head, fsm_state, reader_progress, true);
                }
            }
        }

        tracing::debug!(
            target: "flowip-080o",
            owner = %self.owner_label,
            eof_count = self.state.eof_count(),
            total_readers = self.readers.len(),
            "subscription: no events available in this poll cycle"
        );
        PollResult::NoEvents
    }

    /// Advance the round-robin fairness pointer; true when it has wrapped
    /// back to `starting_index`, completing one poll cycle.
    fn advance_cycle(&mut self, starting_index: usize) -> bool {
        self.state.next_reader_index();
        self.state.current_reader_index == starting_index
    }

    // ------------------------------------------------------------------
    // Delivery side (FLOWIP-095d D8 split), shared by both policies
    // ------------------------------------------------------------------

    /// Delivery-side accounting shared by both reader-selection policies.
    ///
    /// Everything that must observe exactly the delivered sequence in
    /// delivered order lives in this sequence of steps: selected-data
    /// counters, reader_seq and receipt tracking, advertised EOF evidence,
    /// contract chain on_read/on_write, EOF exhaustion accounting,
    /// `StageInputPosition` assignment, and the per-reader delivered ordinal.
    /// Exhaustion happens strictly at EOF delivery, never when an EOF head is
    /// merely held.
    fn deliver_from_reader(
        &mut self,
        reader_index: usize,
        head: HeldHead<T>,
        fsm_state: &str,
        reader_progress: Option<&mut [ReaderProgress]>,
        set_read_instant: bool,
    ) -> PollResult<T> {
        let HeldHead {
            envelope,
            is_authored_eof: is_eof,
            is_drain,
            catch_up,
            orders_by_own_seq,
        } = head;
        // FLOWIP-120n F18: a delivered positional row advances the inherited
        // key for this reader's later re-authored control heads.
        if orders_by_own_seq {
            if let Some(seq) = envelope.event.admission_seq() {
                if let Some(last) = self.last_positional_seq.get_mut(reader_index) {
                    *last = seq;
                }
            }
        }
        let (stage_id, stage_key) = {
            let slot = &self.readers[reader_index];
            (slot.stage_id, slot.stage_key.clone())
        };

        // FLOWIP-120n: a catch-up watermark delivers at the generation it
        // closes, then advances its reader to the announced value. The
        // advance is fail-closed (F11): exactly one, never a regression or a
        // skip, so a corrupted boundary aborts instead of mis-ordering.
        let reader_generation = self.generation_by_reader[reader_index];
        if let Some(announced) = catch_up {
            if announced.0 != reader_generation.0 + 1 {
                return PollResult::Error(Box::new(JournalError::Implementation {
                    message: format!(
                        "catch-up watermark on edge '{}' announces generation {} from {}; \
                         the boundary must advance by exactly one (FLOWIP-120n F11)",
                        stage_key.as_str(),
                        announced.0,
                        reader_generation.0
                    ),
                    source: "generation boundary regression or skip".into(),
                }));
            }
            self.generation_by_reader[reader_index] = announced;
        }
        self.last_delivered_generation = Some(reader_generation);
        let original_chain_event = (&envelope.event as &dyn Any).downcast_ref::<ChainEvent>();

        let normalized_contract_event =
            self.normalized_eof_for_contracts(reader_index, stage_id, original_chain_event, is_eof);
        let contract_chain_event = normalized_contract_event.as_ref().or(original_chain_event);

        self.bump_selected_data_counters(reader_index, original_chain_event);

        // Reader progress lives in FSM contexts; it is updated here so later
        // contract checks emit progress/final events from the same state
        // during replay.
        let mut reader_seq_for_contracts: Option<SeqNo> = None;
        if self.contract_tracker.is_some() {
            if let Some(progress) =
                reader_progress.and_then(|progress_slice| progress_slice.get_mut(reader_index))
            {
                reader_seq_for_contracts = Some(self.record_delivery_progress(
                    reader_index,
                    stage_id,
                    progress,
                    contract_chain_event,
                    &envelope,
                    set_read_instant,
                ));
            }
        }

        self.feed_contract_chains_on_delivery(
            reader_index,
            stage_id,
            contract_chain_event,
            reader_seq_for_contracts,
        );

        tracing::debug!(
            target: "flowip-080o",
            owner = %self.owner_label,
            stage_id = ?stage_id,
            stage_key = %stage_key,
            reader_index = reader_index,
            fsm_state = fsm_state,
            event_type = %envelope.event.event_type_name(),
            is_eof = is_eof,
            "subscription: received event"
        );

        if is_eof {
            self.record_eof_exhaustion(reader_index, stage_id, &stage_key);
        } else if is_drain {
            tracing::debug!(
                target: "flowip-080o",
                owner = %self.owner_label,
                stage_id = ?stage_id,
                stage_key = %stage_key,
                reader_index = reader_index,
                "Received drain from stage — continuing to consume until EOF"
            );
        }

        let delivered_stage_input_position = match original_chain_event {
            Some(chain_event) if chain_event.is_data() => {
                let position = StageInputPosition(self.next_stage_input_position);
                self.next_stage_input_position = self.next_stage_input_position.saturating_add(1);
                Some(position)
            }
            _ => None,
        };

        // FLOWIP-095d: every delivered transport event takes a per-reader
        // ordinal, post-filter, on the delivery side. This counter is the
        // canonical merge's tiebreak source and its checkpointable state.
        if let Some(delivered) = self.delivered_count_by_reader.get_mut(reader_index) {
            delivered.increment();
        }
        self.last_merge_wait = None;
        self.merge_candidate_index = None;

        self.last_delivered_upstream_stage = Some(stage_id);
        self.last_delivered_stage_input_position = delivered_stage_input_position;

        PollResult::Event(envelope)
    }

    /// Selected-feed EOF normalization for contract accounting: when this
    /// edge filters by selected event types, contracts must see the selected
    /// writer count rather than the upstream's raw count.
    fn normalized_eof_for_contracts(
        &self,
        reader_index: usize,
        stage_id: StageId,
        original: Option<&ChainEvent>,
        is_eof: bool,
    ) -> Option<ChainEvent> {
        if !is_eof || !self.has_selected_event_type_filter(stage_id) {
            return None;
        }
        let chain_event = original?;
        let selected_writer_seq = self.selected_writer_seq_for_reader(reader_index, stage_id);
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
        Some(normalized)
    }

    /// Per-reader selected-data counters (delivery side).
    fn bump_selected_data_counters(&mut self, reader_index: usize, original: Option<&ChainEvent>) {
        let Some(ChainEventContent::Data { event_type, .. }) = original.map(|event| &event.content)
        else {
            return;
        };
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

    /// Delivery-side `ReaderProgress` accounting: reader_seq and receipt
    /// tracking for data events, advertised positions from an authored EOF,
    /// and the last-seen identifiers. Returns the post-update reader_seq for
    /// contract chains.
    fn record_delivery_progress(
        &mut self,
        reader_index: usize,
        stage_id: StageId,
        progress: &mut ReaderProgress,
        contract_chain_event: Option<&ChainEvent>,
        envelope: &EventEnvelope<T>,
        set_read_instant: bool,
    ) -> SeqNo {
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
                    progress.last_receipted_vector_clock = Some(envelope.vector_clock.clone());
                }
            }

            // Capture advertised positions from an EOF authored by this
            // upstream (forwarded EOFs advertise nothing here).
            if let ChainEventContent::FlowControl(FlowControlPayload::Eof {
                writer_seq,
                writer_seq_by_event_type,
                vector_clock,
                ..
            }) = &chain_event.content
            {
                if Self::eof_authored_by_upstream(chain_event, stage_id) {
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

        progress.reader_seq
    }

    /// Feed the delivered event into this edge's `ContractChain` and any
    /// selected-feed chains, if contracts are configured.
    fn feed_contract_chains_on_delivery(
        &mut self,
        reader_index: usize,
        stage_id: StageId,
        contract_chain_event: Option<&ChainEvent>,
        reader_seq_for_contracts: Option<SeqNo>,
    ) {
        let (Some(chain_event), Some(reader_stage)) = (
            contract_chain_event,
            self.contract_tracker.as_ref().and_then(|t| t.reader_stage),
        ) else {
            return;
        };

        if let Some(chain) = self
            .contract_chains
            .get_mut(reader_index)
            .and_then(|slot| slot.as_mut())
        {
            let reader_seq = reader_seq_for_contracts.unwrap_or(SeqNo(0));
            // From the contract framework's perspective, the upstream writer
            // is `stage_id` and the downstream reader is `reader_stage`.
            chain.on_read(chain_event, reader_stage, reader_seq, stage_id);

            // Also feed the writer side of the transport contract: its
            // writer-side count is derived from EOF writer_seq, and
            // `TransportContract` ignores non-EOF events on the write side.
            chain.on_write(chain_event, stage_id, SeqNo(0));
        }

        self.feed_selected_contract_chains_on_event(
            reader_index,
            stage_id,
            chain_event,
            reader_stage,
        );
    }

    fn feed_selected_contract_chains_on_event(
        &mut self,
        reader_index: usize,
        upstream_stage: StageId,
        event: &ChainEvent,
        reader_stage: StageId,
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

    /// EOF exhaustion accounting (delivery side): a reader leaves the head
    /// set only when its authored EOF is delivered, never when merely held.
    fn record_eof_exhaustion(
        &mut self,
        reader_index: usize,
        stage_id: StageId,
        stage_key: &StageKey,
    ) {
        self.state.mark_reader_eof(reader_index);
        let total_readers = self.readers.len();
        let eof_count = self.state.eof_count();
        let is_final = eof_count == total_readers;
        self.last_eof_outcome = Some(EofOutcome {
            stage_id,
            stage_name: stage_key.to_string(),
            reader_index,
            eof_count,
            total_readers,
            is_final,
        });
        tracing::debug!(
            target: "flowip-080o",
            owner = %self.owner_label,
            stage_id = ?stage_id,
            stage_key = %stage_key,
            reader_index = reader_index,
            total_readers = total_readers,
            eof_status = ?self.state.eof_received,
            is_final = is_final,
            "Received EOF from stage"
        );
    }

    // ------------------------------------------------------------------
    // Canonical deterministic merge (FLOWIP-095d)
    // ------------------------------------------------------------------

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
            return self.no_readers_error();
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
        if self.seq_ordered {
            return self
                .ensure_seq_merge_candidate(fsm_state, reader_progress)
                .await;
        }
        for index in 0..self.readers.len() {
            if self.state.is_reader_eof(index) || self.held_heads[index].is_some() {
                continue;
            }
            self.acquire_head(index, fsm_state, reader_progress).await?;
        }

        let quiet_inputs: Vec<(StageId, String)> = (0..self.readers.len())
            .filter(|&index| !self.state.is_reader_eof(index) && self.held_heads[index].is_none())
            .map(|index| {
                let slot = &self.readers[index];
                (slot.stage_id, slot.stage_key.to_string())
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

        self.merge_candidate_index = self.select_merge_winner(&candidates);
        Ok(MergeCandidateStatus::Candidate)
    }

    /// Seq-ordered acquisition and selection (FLOWIP-120n F18).
    ///
    /// Acquisition repeats until a full round adds no head (bounded by reader
    /// count): every headless reader's last empty poll then postdates every
    /// held head's acquisition. The per-path journal lock makes an empty poll
    /// a stamp fence — any row committed after it was stamped after it — so a
    /// headless reader at or past `entered_generation` can never later
    /// present a sequence below a held head; its silence is proof and it is
    /// exempt from the quiet-input wait. A reader below the entered
    /// generation may still present re-admitted rows with recorded (smaller)
    /// sequences, so it keeps the Kahn wait until its F17 crossing.
    async fn ensure_seq_merge_candidate(
        &mut self,
        fsm_state: &str,
        reader_progress: &mut Option<&mut [ReaderProgress]>,
    ) -> std::result::Result<MergeCandidateStatus, Box<dyn std::error::Error + Send + Sync>> {
        loop {
            let held_before = self.held_head_count();
            for index in 0..self.readers.len() {
                if self.state.is_reader_eof(index) || self.held_heads[index].is_some() {
                    continue;
                }
                self.acquire_head(index, fsm_state, reader_progress).await?;
            }
            if self.held_head_count() == held_before {
                break;
            }
        }

        let waiting_inputs: Vec<(StageId, String)> = (0..self.readers.len())
            .filter(|&index| {
                !self.state.is_reader_eof(index)
                    && self.held_heads[index].is_none()
                    && self.generation_by_reader[index] < self.entered_generation
            })
            .map(|index| {
                let slot = &self.readers[index];
                (slot.stage_id, slot.stage_key.to_string())
            })
            .collect();
        if !waiting_inputs.is_empty() {
            tracing::debug!(
                target: "flowip-120n",
                owner = %self.owner_label,
                waiting = ?waiting_inputs.iter().map(|(_, name)| name).collect::<Vec<_>>(),
                fsm_state = fsm_state,
                "seq merge: waiting on pre-crossing quiet input(s)"
            );
            self.merge_candidate_index = None;
            self.last_merge_wait = Some(MergeWaitState {
                quiet_inputs: waiting_inputs,
            });
            return Ok(MergeCandidateStatus::Quiet);
        }
        self.last_merge_wait = None;

        let candidates: Vec<usize> = (0..self.held_heads.len())
            .filter(|&index| self.held_heads[index].is_some())
            .collect();
        if candidates.is_empty() {
            self.merge_candidate_index = None;
            // No heads and no waiters: every reader is EOF-exhausted or
            // exempt-quiet. Only full exhaustion ends the merge.
            return Ok(if self.state.eof_count() == self.readers.len() {
                MergeCandidateStatus::AllExhausted
            } else {
                MergeCandidateStatus::Quiet
            });
        }

        self.merge_candidate_index = Some(self.select_seq_winner(&candidates)?);
        Ok(MergeCandidateStatus::Candidate)
    }

    /// The head's effective sequence key (FLOWIP-120n F18). Re-admittable
    /// rows order by their own cross-run-stable sequence, failing closed when
    /// it is missing (an archive predating the field). Re-authored control
    /// rows (source contracts, EOFs) carry per-run sequences, so they inherit
    /// the reader's last positional sequence: their journal position, which
    /// is what replay reproduces.
    fn effective_seq(
        &self,
        index: usize,
    ) -> std::result::Result<obzenflow_core::AdmissionSeq, Box<dyn std::error::Error + Send + Sync>>
    {
        let head = self.held_heads[index].as_ref().expect("candidate has head");
        if !head.orders_by_own_seq {
            return Ok(self.last_positional_seq[index]);
        }
        head.envelope.event.admission_seq().ok_or_else(|| {
            Box::new(JournalError::Implementation {
                message: format!(
                    "seq-ordered merge on '{}' found a head from reader '{}' without \
                     an admission_seq; seq mode fails closed (FLOWIP-120n F18)",
                    self.owner_label,
                    self.readers[index].stage_key.as_str()
                ),
                source: "sequence-less head in seq mode".into(),
            }) as Box<dyn std::error::Error + Send + Sync>
        })
    }

    /// The seq-mode merge decision (FLOWIP-120n F18): the causality filter
    /// exactly as in Kahn mode, then min by `(generation, effective sequence,
    /// reader key)`. The reader key breaks the one reachable tie, two
    /// first-row control heads both inheriting sequence zero.
    fn select_seq_winner(
        &self,
        candidates: &[usize],
    ) -> std::result::Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        let head = |index: usize| self.held_heads[index].as_ref().expect("candidate has head");
        let mut keyed: Vec<(
            usize,
            (
                obzenflow_core::ReaderGeneration,
                obzenflow_core::AdmissionSeq,
                &ReaderTiebreakKey,
            ),
        )> = Vec::with_capacity(candidates.len());
        for &index in candidates {
            keyed.push((
                index,
                (
                    self.generation_by_reader[index],
                    self.effective_seq(index)?,
                    &self.reader_tiebreak_keys[index],
                ),
            ));
        }
        let causally_admissible = |index: usize| {
            head(index).is_authored_eof
                || !candidates.iter().any(|&other| {
                    other != index
                        && !head(other).is_authored_eof
                        && CausalOrderingService::happened_before(
                            &head(other).envelope.vector_clock,
                            &head(index).envelope.vector_clock,
                        )
                })
        };

        Ok(keyed
            .iter()
            .filter(|(index, _)| causally_admissible(*index))
            .min_by_key(|(_, key)| *key)
            .or_else(|| {
                tracing::error!(
                    target: "flowip-120n",
                    owner = %self.owner_label,
                    "seq merge: causality exclusion emptied the candidate set; \
                     falling back to sequence-only selection (still deterministic)"
                );
                keyed.iter().min_by_key(|(_, key)| *key)
            })
            .expect("candidates is non-empty")
            .0)
    }

    /// Drain one reader's journal until it presents a head or runs empty.
    async fn acquire_head(
        &mut self,
        index: usize,
        fsm_state: &str,
        reader_progress: &mut Option<&mut [ReaderProgress]>,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        loop {
            match self.read_transport_step(index).await {
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
                Ok(ReadStep::Filtered) => continue,
                Ok(ReadStep::Empty) => return Ok(()),
                Ok(ReadStep::Head { head, is_data }) => {
                    self.note_read_instant(reader_progress, index, is_data);
                    tracing::debug!(
                        target: "flowip-095d",
                        owner = %self.owner_label,
                        reader_index = index,
                        fsm_state = fsm_state,
                        event_type = %head.envelope.event.event_type_name(),
                        is_authored_eof = head.is_authored_eof,
                        "canonical merge: acquired head"
                    );
                    self.held_heads[index] = Some(head);
                    return Ok(());
                }
            }
        }
    }

    /// The canonical merge decision over the current heads.
    ///
    /// Causality among heads: a non-EOF head is excluded while another
    /// non-EOF head happened-before it. Authored EOFs are exempt and order by
    /// the tiebreak alone. Happened-before is acyclic, so the candidate set
    /// cannot empty; the fallback is defensive only and remains
    /// deterministic.
    fn select_merge_winner(&self, candidates: &[usize]) -> Option<usize> {
        let head = |index: usize| self.held_heads[index].as_ref().expect("candidate has head");
        // Compared as the ordinal the delivery would take, the same key shape
        // the join's cross-side rule uses; counts never enter a comparison.
        // Generation is the coarsest axis (FLOWIP-120n): every
        // recorded-generation head orders ahead of any live head, applied
        // after the causality filter, and same-generation order is
        // byte-for-byte the (ordinal, key) order it always was. A held
        // watermark sorts at its reader's current (pre-advance) generation.
        let tiebreak = |index: usize| {
            (
                self.generation_by_reader[index],
                self.delivered_count_by_reader[index].next_ordinal(),
                &self.reader_tiebreak_keys[index],
            )
        };
        let causally_admissible = |index: usize| {
            head(index).is_authored_eof
                || !candidates.iter().any(|&other| {
                    other != index
                        && !head(other).is_authored_eof
                        && CausalOrderingService::happened_before(
                            &head(other).envelope.vector_clock,
                            &head(index).envelope.vector_clock,
                        )
                })
        };

        candidates
            .iter()
            .copied()
            .filter(|&index| causally_admissible(index))
            .min_by_key(|&index| tiebreak(index))
            .or_else(|| {
                tracing::error!(
                    target: "flowip-095d",
                    owner = %self.owner_label,
                    "canonical merge: causality exclusion emptied the candidate set; \
                     falling back to tiebreak-only selection (still deterministic)"
                );
                candidates
                    .iter()
                    .copied()
                    .min_by_key(|&index| tiebreak(index))
            })
    }

    /// Comparison metadata for the currently selected merge candidate, if any.
    ///
    /// The join supervisor uses this to compose two subscriptions: each side
    /// selects its internal winner, and the cross-side choice applies the same
    /// (causality, then ordinal/stage-key/feed-identity) rule to the two metas.
    pub fn merge_candidate(&self) -> Option<MergeCandidateMeta<'_>> {
        let index = self.merge_candidate_index?;
        let head = self.held_heads.get(index)?.as_ref()?;
        let key = self.reader_tiebreak_keys.get(index)?;
        // The effective sequence is seq-mode-only: a Kahn join's cross-side
        // rule must stay on the (ordinal, key) tiebreak even though live rows
        // now carry sequences.
        let admission_seq = if self.seq_ordered {
            self.effective_seq(index).ok()
        } else {
            None
        };
        Some(MergeCandidateMeta {
            generation: self.generation_by_reader[index],
            ordinal: self.delivered_count_by_reader[index].next_ordinal(),
            key,
            vector_clock: &head.envelope.vector_clock,
            is_authored_eof: head.is_authored_eof,
            admission_seq,
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
        self.deliver_from_reader(index, head, fsm_state, reader_progress, false)
    }
}
