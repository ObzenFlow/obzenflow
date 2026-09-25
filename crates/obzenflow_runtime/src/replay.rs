// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Execution of replay through the Core archive and journal contracts.

use obzenflow_core::event::payloads::flow_control_payload::EofKind;
use obzenflow_core::event::provenance::FlowContext;
use obzenflow_core::event::ChainPayload;
use obzenflow_core::journal::archive::ReplayError;
use obzenflow_core::journal::JournalReader;
use obzenflow_core::{ChainEvent, StageId, WriterId};
use std::path::PathBuf;

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
        }
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
            if !original_event.is_source_replayable() {
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

            let original_event_id = original_event.id;
            let mut new_event = original_event;
            new_event.flow_context = flow_context;
            new_event.replay_context = Some(obzenflow_core::event::provenance::ReplayContext {
                original_event_id,
                original_flow_id: self.replay_context.original_flow_id.clone(),
                original_stage_id: self.replay_context.original_stage_id,
            });

            self.replayed_events = self.replayed_events.saturating_add(1);
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
