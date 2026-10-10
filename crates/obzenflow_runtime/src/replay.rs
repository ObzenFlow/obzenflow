// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Execution of replay through the Core archive and journal contracts.

use obzenflow_core::event::chain_event::ReplayDisposition;
use obzenflow_core::event::payloads::execution_payload::{
    ExecutionPayload, SourceContinuationRefusal, SourceInputPinnedFact,
};
use obzenflow_core::event::payloads::flow_control_payload::EofKind;
use obzenflow_core::event::provenance::FlowContext;
use obzenflow_core::event::ChainPayload;
use obzenflow_core::journal::archive::ReplayError;
use obzenflow_core::journal::JournalReader;
use obzenflow_core::source::EncodedSourcePosition;
use obzenflow_core::{ChainEvent, StageId, WriterId};
use std::path::PathBuf;

/// Where a positioned source continues on resume (FLOWIP-134i B1, B15).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceHandoff {
    /// Validate the pin, then continue strictly after `after`, or at the
    /// pinned beginning when nothing was admitted after it.
    Seek {
        pin: SourceInputPinnedFact,
        after: Option<EncodedSourcePosition>,
    },
    /// Never pinned and nothing admitted: pin and read as a fresh run (B15).
    Start,
}

/// The committed frontier a source's replay read (FLOWIP-134i B11): the latest
/// input pin and the position after the last admitted record bound to it.
#[derive(Debug, Default, Clone)]
pub struct SourceFrontier {
    pin: Option<SourceInputPinnedFact>,
    after: Option<EncodedSourcePosition>,
    admitted: u64,
    unpositioned: bool,
}

impl SourceFrontier {
    fn observe(&mut self, event: &ChainEvent) {
        match &event.payload {
            ChainPayload::Execution(ExecutionPayload::SourceInputPinned(pin)) => {
                self.pin = Some(pin.clone());
                self.after = None;
            }
            ChainPayload::Fact(_) => {
                self.admitted = self.admitted.saturating_add(1);
                match (&event.source_progress, &self.pin) {
                    (Some(progress), Some(_)) => self.after = Some(progress.after.clone()),
                    _ => self.unpositioned = true,
                }
            }
            _ => {}
        }
    }

    /// A positioned record with no preceding pin, or an admitted record
    /// without a position, is missing evidence and refuses.
    pub fn handoff(&self) -> Result<SourceHandoff, SourceContinuationRefusal> {
        if self.unpositioned {
            return Err(SourceContinuationRefusal::MissingEvidence);
        }
        match &self.pin {
            Some(pin) => Ok(SourceHandoff::Seek {
                pin: pin.clone(),
                after: self.after.clone(),
            }),
            None if self.admitted == 0 => Ok(SourceHandoff::Start),
            None => Err(SourceContinuationRefusal::MissingEvidence),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReplayContextTemplate {
    pub original_flow_id: String,
    pub original_stage_id: StageId,
}

pub struct ReplayedEvent {
    pub event: ChainEvent,
    pub origin: obzenflow_core::event::CausalFrontier,
}

impl ReplayedEvent {
    pub(crate) fn admit(self) -> Result<ChainEvent, obzenflow_core::journal::JournalError> {
        crate::supervised_base::publication::incorporate(&self.origin)?;
        Ok(self.event)
    }
}

pub struct ReplayDriver {
    archive_reader: Box<dyn JournalReader<ChainEvent>>,
    journal_path: PathBuf,
    replay_context: ReplayContextTemplate,
    replayed_events: u64,
    skipped_events: u64,
    archived_eof_kind: Option<EofKind>,
    frontier: SourceFrontier,
}

impl ReplayDriver {
    pub fn new(
        archive_reader: Box<dyn JournalReader<ChainEvent>>,
        journal_path: PathBuf,
        replay_context: ReplayContextTemplate,
    ) -> Self {
        Self {
            archive_reader,
            journal_path,
            replay_context,
            replayed_events: 0,
            skipped_events: 0,
            archived_eof_kind: None,
            frontier: SourceFrontier::default(),
        }
    }

    /// The committed frontier read so far; complete once the driver is exhausted.
    pub fn frontier(&self) -> &SourceFrontier {
        &self.frontier
    }

    pub fn replayed_events(&self) -> u64 {
        self.replayed_events
    }

    pub fn skipped_events(&self) -> u64 {
        self.skipped_events
    }

    /// The archive's recorded completion kind, captured while skipping the
    /// archived EOF (FLOWIP-095k). `None`: the archive committed no EOF.
    pub fn archived_eof_kind(&self) -> Option<EofKind> {
        self.archived_eof_kind
    }

    pub async fn next_replayed_event(
        &mut self,
        _writer_id: WriterId,
        _stage_name: &str,
        flow_context: FlowContext,
    ) -> Result<Option<ReplayedEvent>, ReplayError> {
        loop {
            let next =
                self.archive_reader
                    .next()
                    .await
                    .map_err(|e| ReplayError::CorruptedArchive {
                        path: self.journal_path.clone(),
                        record_position: self.archive_reader.position(),
                        message: e.to_string(),
                    })?;

            let Some(envelope) = next else {
                // FLOWIP-120q: the reader applies the archive's status-derived
                // torn-tail policy, so `None` is always a clean end (true EOF or
                // a tolerated final torn tail) and corruption arrives as the
                // `Err` mapped above. The reader owns finality; the driver no
                // longer second-guesses it via `is_at_end`.
                return Ok(None);
            };

            let original_event = envelope.authored();
            let disposition = original_event.replay_disposition();
            if disposition == ReplayDisposition::ReAuthor {
                if let ChainPayload::FlowControl(fc) = &original_event.payload {
                    if let Some(kind) = fc.eof_kind() {
                        crate::supervised_base::publication::observe_record(&envelope).map_err(
                            |error| ReplayError::CorruptedArchive {
                                path: self.journal_path.clone(),
                                record_position: self.archive_reader.position(),
                                message: error.to_string(),
                            },
                        )?;
                        self.archived_eof_kind = Some(kind);
                    }
                }
                self.skipped_events = self.skipped_events.saturating_add(1);
                continue;
            }

            self.frontier.observe(&original_event);
            let original_event_id = original_event.id;
            let mut new_event = original_event;
            new_event.flow_context = flow_context;
            new_event.replay_context = Some(obzenflow_core::event::provenance::ReplayContext {
                original_event_id,
                original_flow_id: self.replay_context.original_flow_id.clone(),
                original_stage_id: self.replay_context.original_stage_id,
            });

            // A preserved pin is copied without counting as delivered input.
            if disposition == ReplayDisposition::ReAdmit {
                self.replayed_events = self.replayed_events.saturating_add(1);
            }
            let origin =
                obzenflow_core::event::CausalFrontier::from_record(&envelope).map_err(|error| {
                    ReplayError::CorruptedArchive {
                        path: self.journal_path.clone(),
                        record_position: self.archive_reader.position(),
                        message: error.to_string(),
                    }
                })?;
            return Ok(Some(ReplayedEvent {
                event: new_event,
                origin,
            }));
        }
    }
}
