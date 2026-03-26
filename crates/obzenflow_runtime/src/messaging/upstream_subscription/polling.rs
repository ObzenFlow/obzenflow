// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{DeliveryFilter, EofOutcome, PollResult, ReaderProgress, UpstreamSubscription};
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::types::SeqNo;
use obzenflow_core::event::{ChainEvent, ChainEventContent, JournalEvent};
use obzenflow_core::WriterId;
use std::any::Any;
use std::io;
use tokio::time::Instant;

impl<T> UpstreamSubscription<T>
where
    T: JournalEvent + 'static,
{
    /// Poll for the next event without blocking
    ///
    /// This method tries once through all readers and returns immediately.
    /// The caller (FSM) decides whether to retry, sleep, or transition states.
    /// `fsm_state` is the caller's current FSM state (for diagnostics).
    pub async fn poll_next_with_state(
        &mut self,
        fsm_state: &str,
        mut reader_progress: Option<&mut [ReaderProgress]>,
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
                    if matches!(self.delivery_filter, DeliveryFilter::TransportOnly) {
                        if let Some(chain_event) =
                            (&envelope.event as &dyn Any).downcast_ref::<ChainEvent>()
                        {
                            if matches!(chain_event.content, ChainEventContent::Observability(_)) {
                                // Observability events are not part of the transport stream.
                                // Skip delivering to the caller, but keep progressing.
                                self.state.next_reader_index();
                                continue;
                            }
                        }
                    }

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
                    // Detect terminal EOF for this *upstream stage* (drain should NOT be treated as EOF).
                    //
                    // Important subtlety (FLOWIP-080o):
                    // - Journals can contain EOFs authored by *other* stages that were merely
                    //   forwarded (e.g., a join forwarding source EOFs).
                    // - Contracts and EOF accounting for this subscription must only treat
                    //   EOFs authored by the upstream stage associated with this reader as
                    //   terminal. Otherwise, downstream stages can observe "early EOF"
                    //   and stop consuming before the real writer has finished.
                    let (is_eof, is_drain) = if let Some(chain_event) =
                        (&envelope.event as &dyn Any).downcast_ref::<ChainEvent>()
                    {
                        match &chain_event.content {
                            ChainEventContent::FlowControl(FlowControlPayload::Eof {
                                writer_id,
                                ..
                            }) => {
                                // Only treat as terminal EOF when the EOF is authored by
                                // the upstream stage for this reader. If writer_id is
                                // missing, fall back to treating it as terminal.
                                let authored_by_upstream = match writer_id {
                                    Some(WriterId::Stage(eof_stage)) => *eof_stage == stage_id,
                                    Some(_) => false,
                                    None => true,
                                };
                                (authored_by_upstream, false)
                            }
                            ChainEventContent::FlowControl(FlowControlPayload::Drain) => {
                                (false, true)
                            }
                            _ => (false, false),
                        }
                    } else {
                        (false, false)
                    };

                    // Update legacy contract tracking state if enabled and
                    // contract progress has been provided by the owning FSM
                    // context. Reader progress lives in FSM contexts; we only
                    // update it here so that later contract checks can emit
                    // progress/final events based on the same state during
                    // replay.
                    let mut reader_seq_for_contracts: Option<SeqNo> = None;
                    if self.contract_tracker.is_some() {
                        if let Some(progress_slice) = reader_progress.as_deref_mut() {
                            if let Some(progress) = progress_slice.get_mut(current_index) {
                                // Update progress for data events
                                if let Some(chain_event) =
                                    (&envelope.event as &dyn Any).downcast_ref::<ChainEvent>()
                                {
                                    if chain_event.is_data() {
                                        progress.reader_seq.0 += 1;
                                        progress.last_read_instant = Some(Instant::now());
                                        if self.uses_receipt_watermark() {
                                            progress.track_pending_receipt(
                                                *envelope.event.id(),
                                                chain_event.clone(),
                                                envelope.vector_clock.clone(),
                                            );
                                        } else {
                                            progress.receipted_seq = progress.reader_seq;
                                            progress.last_receipted_event_id =
                                                Some(*envelope.event.id());
                                            progress.last_receipted_vector_clock =
                                                Some(envelope.vector_clock.clone());
                                        }
                                    }

                                    // Capture advertised positions from EOF authored by this upstream
                                    if let ChainEventContent::FlowControl(
                                        FlowControlPayload::Eof {
                                            writer_id,
                                            writer_seq,
                                            vector_clock,
                                            ..
                                        },
                                    ) = &chain_event.content
                                    {
                                        let authored_by_upstream = match writer_id {
                                            Some(WriterId::Stage(eof_stage)) => {
                                                *eof_stage == stage_id
                                            }
                                            Some(_) => false,
                                            None => true,
                                        };

                                        if authored_by_upstream {
                                            progress.advertised_writer_seq = *writer_seq;
                                            progress.last_vector_clock = vector_clock.clone();
                                        }
                                    }
                                }

                                progress.last_event_id = Some(*envelope.event.id());
                                if (&envelope.event as &dyn Any)
                                    .downcast_ref::<ChainEvent>()
                                    .is_some()
                                {
                                    progress.last_vector_clock =
                                        Some(envelope.vector_clock.clone());
                                }

                                // Capture the updated reader_seq for contract chains.
                                reader_seq_for_contracts = Some(progress.reader_seq);
                            }
                        }
                    }

                    // Feed the event into the ContractChain for this edge, if configured.
                    if let (Some(chain_event), Some(reader_stage)) = (
                        (&envelope.event as &dyn Any).downcast_ref::<ChainEvent>(),
                        self.contract_tracker.as_ref().and_then(|t| t.reader_stage),
                    ) {
                        if let Some(chain_slot) = self.contract_chains.get_mut(current_index) {
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
                    }

                    tracing::debug!(
                        target: "flowip-080o",
                        owner = %self.owner_label,
                        stage_id = ?stage_id,
                        stage_name = %stage_name,
                        reader_index = current_index,
                        fsm_state = fsm_state,
                        event_type = %envelope.event.event_type_name(),
                        is_eof = is_eof,
                        "subscription: received event"
                    );

                    if is_eof {
                        self.state.mark_reader_eof(current_index);
                        let eof_count = self.state.eof_count();
                        let is_final = eof_count == total_readers;
                        let outcome = EofOutcome {
                            stage_id,
                            stage_name: stage_name.clone(),
                            reader_index: current_index,
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
                            reader_index = current_index,
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
                            reader_index = current_index,
                            total_readers = total_readers,
                            "Received drain from stage — continuing to consume until EOF"
                        );
                    }

                    tracing::debug!(
                        "Received event from stage {:?} (reader {}/{})",
                        stage_id,
                        current_index + 1,
                        total_readers
                    );

                    // Advance to next reader for fairness
                    self.state.next_reader_index();
                    self.last_delivered_upstream_stage = Some(stage_id);

                    return PollResult::Event(envelope);
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

    /// Backwards-compatible wrapper when FSM state is not provided
    pub async fn poll_next(&mut self) -> PollResult<T> {
        self.poll_next_with_state("unknown", None).await
    }
}
