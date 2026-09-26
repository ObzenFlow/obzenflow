// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Owner-bound supervisor publication. Stage facts use the stage's existing
//! protected execution lane; system supervisors use their own system history.

use obzenflow_core::event::chain_event::ChainEventFactory;
use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
use obzenflow_core::event::provenance::FlowContext;
use obzenflow_core::event::{ChainEvent, ChainPayload, SystemEvent};
use obzenflow_core::journal::{AppendOptions, Journal, JournalError};
use obzenflow_core::{JournalId, JournalOwner, WriterId};
use std::sync::Arc;

#[derive(Clone)]
pub enum SupervisorJournal {
    Stage {
        journal: Arc<dyn Journal<ChainEvent>>,
        context: FlowContext,
    },
    System(Arc<dyn Journal<SystemEvent>>),
}

impl SupervisorJournal {
    pub fn stage(journal: Arc<dyn Journal<ChainEvent>>, context: FlowContext) -> Self {
        Self::Stage { journal, context }
    }

    #[cfg(test)]
    pub(crate) async fn read_all_unordered(&self) -> Result<Vec<SupervisorRecord>, JournalError> {
        Ok(match self {
            Self::System(journal) => journal
                .read_all_unordered()
                .await?
                .into_iter()
                .map(Into::into)
                .collect(),
            Self::Stage { journal, .. } => journal
                .read_all_unordered()
                .await?
                .into_iter()
                .filter_map(SupervisorRecord::from_chain)
                .collect(),
        })
    }

    pub fn id(&self) -> &JournalId {
        match self {
            Self::Stage { journal, .. } => journal.id(),
            Self::System(journal) => journal.id(),
        }
    }

    pub(crate) async fn append_inline(
        &self,
        event: SystemEvent,
        mut options: AppendOptions<SystemEvent>,
    ) -> Result<SupervisorRecord, JournalError> {
        match self {
            Self::System(journal) => {
                if !match journal.owner() {
                    Some(JournalOwner::System { system_id }) => {
                        event.writer_id == WriterId::from(*system_id)
                    }
                    Some(JournalOwner::Stage { stage_id }) => {
                        event.writer_id == WriterId::from(*stage_id)
                    }
                    None => false,
                } {
                    return Err(owner_error());
                }
                super::publication::append_inline(journal, event, options)
                    .await
                    .map(Into::into)
            }
            Self::Stage { journal, context } => {
                if event.writer_id != WriterId::from(context.stage_id)
                    || !matches!(journal.owner(), Some(JournalOwner::Stage { stage_id })
                        if *stage_id == context.stage_id)
                {
                    return Err(owner_error());
                }
                // Authoring conversion precedes commitment. No committed
                // envelope is rewritten or mirrored.
                let event = options.capture.prepare(0, event);
                let id = event.id;
                let writer = event.writer_id;
                let timestamp = event.timestamp;
                let payload =
                    ExecutionPayload::from_supervision_report(event.payload).map_err(|_| {
                        JournalError::Implementation {
                            message: "Report does not belong to a stage journal".into(),
                            source: "invalid stage supervision report".into(),
                        }
                    })?;
                let mut authored = ChainEventFactory::create_with_context(
                    writer,
                    ChainPayload::Execution(payload),
                    context.clone(),
                );
                authored.id = id;
                authored.processing.event_time = timestamp;
                authored.envelope.observability = event.envelope.observability;
                let record = super::publication::append_inline(
                    journal,
                    authored,
                    AppendOptions::new(options.frontier),
                )
                .await?;
                SupervisorRecord::from_chain(record).ok_or_else(owner_error)
            }
        }
    }
}

impl From<Arc<dyn Journal<SystemEvent>>> for SupervisorJournal {
    fn from(journal: Arc<dyn Journal<SystemEvent>>) -> Self {
        Self::System(journal)
    }
}

fn owner_error() -> JournalError {
    JournalError::Implementation {
        message: "Supervisor report publication does not match journal ownership".into(),
        source: "foreign supervisor report".into(),
    }
}

pub use obzenflow_core::event::SupervisorRecord;

impl<J: Journal<SystemEvent> + 'static> From<Arc<J>> for SupervisorJournal {
    fn from(journal: Arc<J>) -> Self {
        Self::System(journal)
    }
}
