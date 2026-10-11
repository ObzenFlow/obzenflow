// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Execution of replay through the Core archive and journal contracts.

use obzenflow_core::event::chain_event::ReplayDisposition;
use obzenflow_core::event::payloads::execution_payload::{
    ExecutionPayload, SourceContinuationRefusal, SourceInputPinnedFact,
};
use obzenflow_core::event::payloads::flow_control_payload::{EofKind, FlowControlPayload};
use obzenflow_core::event::provenance::FlowContext;
use obzenflow_core::event::ChainPayload;
use obzenflow_core::journal::archive::ReplayError;
use obzenflow_core::journal::JournalReader;
use obzenflow_core::source::EncodedSourcePosition;
use obzenflow_core::{ChainEvent, ReaderGeneration, StageId, WriterId};
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
    crossed: ReaderGeneration,
}

impl SourceFrontier {
    fn observe(&mut self, event: &ChainEvent) {
        match &event.payload {
            ChainPayload::Execution(ExecutionPayload::SourceInputPinned(pin)) => {
                self.pin = Some(pin.clone());
                self.after = None;
            }
            ChainPayload::FlowControl(FlowControlPayload::CatchUpComplete {
                generation, ..
            }) => {
                self.crossed = self.crossed.max(*generation);
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

    /// The last generation the source's recorded markers crossed.
    pub fn crossed_generation(&self) -> ReaderGeneration {
        self.crossed
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

/// One archive's journal of a replayed source.
pub struct ReplayMember {
    pub reader: Box<dyn JournalReader<ChainEvent>>,
    pub journal_path: PathBuf,
    pub replay_context: ReplayContextTemplate,
}

struct MemberCursor {
    member: ReplayMember,
    eof_kind: Option<EofKind>,
    exhausted: bool,
}

/// A record a replay re-admits or preserves, as its member committed it.
struct ReplayableRecord {
    envelope: obzenflow_core::event::JournalRecord<ChainPayload>,
    authored: ChainEvent,
}

impl MemberCursor {
    fn corrupted(&self, message: String) -> ReplayError {
        ReplayError::CorruptedArchive {
            path: self.member.journal_path.clone(),
            record_position: self.member.reader.position(),
            message,
        }
    }

    /// Skips re-authored records, capturing the member's committed EOF kind.
    async fn next_replayable(
        &mut self,
        skipped_events: &mut u64,
    ) -> Result<Option<ReplayableRecord>, ReplayError> {
        loop {
            // FLOWIP-120q: the reader applies the archive's status-derived
            // torn-tail policy, so `None` is always a clean end and corruption
            // arrives as an `Err`.
            let next = self
                .member
                .reader
                .next()
                .await
                .map_err(|error| self.corrupted(error.to_string()))?;
            let Some(envelope) = next else {
                return Ok(None);
            };
            let authored = envelope.authored();
            if authored.replay_disposition() == ReplayDisposition::ReAuthor {
                if let ChainPayload::FlowControl(control) = &authored.payload {
                    if let Some(kind) = control.eof_kind() {
                        crate::supervised_base::publication::observe_record(&envelope)
                            .map_err(|error| self.corrupted(error.to_string()))?;
                        self.eof_kind = Some(kind);
                    }
                }
                *skipped_events = skipped_events.saturating_add(1);
                continue;
            }
            return Ok(Some(ReplayableRecord { envelope, authored }));
        }
    }
}

pub struct ReplayDriver {
    /// The replayed archive's journal first, then the ancestors a resume
    /// merges with, nearest first (FLOWIP-134i B6).
    members: Vec<MemberCursor>,
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
        Self::merged(
            ReplayMember {
                reader: archive_reader,
                journal_path,
                replay_context,
            },
            Vec::new(),
        )
    }

    /// Splice a source's records across a merged lineage (FLOWIP-134i B6).
    /// Every member's records must open the longest member's by recorded
    /// event id. The newest member still holding records supplies each copy;
    /// the members ending the merged sequence supply the EOF, the oldest on a
    /// tie, so a torn descendant's own EOF is dropped.
    pub fn merged(head: ReplayMember, ancestors: Vec<ReplayMember>) -> Self {
        Self {
            members: std::iter::once(head)
                .chain(ancestors)
                .map(|member| MemberCursor {
                    member,
                    eof_kind: None,
                    exhausted: false,
                })
                .collect(),
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
        let mut copies = Vec::new();
        let mut ending = Vec::new();
        for (index, cursor) in self.members.iter_mut().enumerate() {
            if cursor.exhausted {
                continue;
            }
            match cursor.next_replayable(&mut self.skipped_events).await? {
                Some(record) => copies.push((index, record)),
                None => {
                    cursor.exhausted = true;
                    ending.push(index);
                }
            }
        }

        let mut copies = copies.into_iter();
        let Some((newest, record)) = copies.next() else {
            if let Some(oldest) = ending.iter().max() {
                self.archived_eof_kind = self.members[*oldest].eof_kind;
            }
            return Ok(None);
        };
        for (index, copy) in copies {
            if copy.authored.id != record.authored.id {
                return Err(self.members[index].corrupted(format!(
                    "its source records disagree with {} by recorded event id",
                    self.members[newest].member.journal_path.display()
                )));
            }
        }

        let ReplayableRecord { envelope, authored } = record;
        let disposition = authored.replay_disposition();
        self.frontier.observe(&authored);
        let original_event_id = authored.id;
        let template = &self.members[newest].member.replay_context;
        let mut new_event = authored;
        new_event.flow_context = flow_context;
        new_event.replay_context = Some(obzenflow_core::event::provenance::ReplayContext {
            original_event_id,
            original_flow_id: template.original_flow_id.clone(),
            original_stage_id: template.original_stage_id,
        });

        // A preserved pin is copied without counting as delivered input.
        if disposition == ReplayDisposition::ReAdmit {
            self.replayed_events = self.replayed_events.saturating_add(1);
        }
        let origin = obzenflow_core::event::CausalFrontier::from_record(&envelope)
            .map_err(|error| self.members[newest].corrupted(error.to_string()))?;
        Ok(Some(ReplayedEvent {
            event: new_event,
            origin,
        }))
    }
}
