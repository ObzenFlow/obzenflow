// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed host commands admitted by the pipeline's existing publication owner.
//! Hosts never receive an independent system-journal writer identity.

use crate::supervised_base::publication::{self, BoxError, PublicationScope};
use obzenflow_core::event::{SystemEvent, SystemPayload, WriterId};
use obzenflow_core::Journal;
use std::sync::Arc;

#[derive(Clone)]
pub struct PipelineReports {
    pub(crate) journal: Arc<dyn Journal<SystemEvent>>,
    pub(crate) owner: Arc<PublicationScope>,
    pub(crate) writer: WriterId,
}

impl PipelineReports {
    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn for_test(journal: Arc<dyn Journal<SystemEvent>>) -> Self {
        let Some(obzenflow_core::JournalOwner::System { system_id }) = journal.owner() else {
            panic!("pipeline test journal requires a system owner");
        };
        let writer = (*system_id).into();
        Self {
            journal,
            owner: PublicationScope::new(),
            writer,
        }
    }

    pub async fn record_ingress_refusal(&self, payload: SystemPayload) -> Result<(), BoxError> {
        if !matches!(payload, SystemPayload::IngressRefusal { .. }) {
            return Err(std::io::Error::other("Expected an ingress refusal command").into());
        }
        let journal = self.journal.clone();
        let event = SystemEvent::new(self.writer, payload);
        self.owner
            .accept_host(async move {
                publication::append_inline(&journal, event, Default::default()).await?;
                Ok(())
            })
            .await
    }
}
