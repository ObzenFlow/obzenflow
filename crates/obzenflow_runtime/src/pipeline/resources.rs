// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Concrete pipeline resources. These capabilities do not select lifecycle
//! work, publish semantic completion, or run an FSM.

use crate::metrics::MetricsHandle;
use crate::stages::common::stage_handle::{StageError, StageHandle};
use crate::supervised_base::{BuilderError, HandleError, SupervisorHandle};
use futures::{future::BoxFuture, FutureExt};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use crate::supervised_base::publication::{
    BoxError, PublicationScope, PublicationSettlement, SharedError,
};
use futures::stream::FuturesUnordered;
use obzenflow_core::EventId;
use std::sync::OnceLock;

pub(crate) type OperationalFailure = Arc<OnceLock<SharedError>>;
type StageJoins = FuturesUnordered<BoxFuture<'static, Result<(), StageError>>>;

#[derive(Default)]
pub(super) enum ProducerTail {
    #[default]
    Uncaptured,
    Reading(Mutex<BoxFuture<'static, Result<Option<EventId>, BoxError>>>),
    Through(EventId),
    Reached,
}

/// Observations of concrete owners. No task is spawned to perform a join.
pub(crate) struct PipelineResources {
    pub(super) publications: Arc<PublicationScope>,
    pub(super) publication_settlement: Option<PublicationSettlement>,
    pub(super) delivery: StageDelivery,
    pub(super) stage_joins: Option<Mutex<StageJoins>>,
    pub(super) stages_joined: bool,
    pub(crate) metrics: Arc<MetricsOwner>,
    pub(super) prepared_metrics: Option<crate::metrics::builder::PreparedMetricsAggregator>,
    pub(super) metrics_join: Option<Mutex<BoxFuture<'static, Result<(), HandleError>>>>,
    pub(super) metrics_joined: bool,
    pub(super) producer_tail: ProducerTail,
    pub(super) terminal_ack: Arc<OnceLock<std::time::Instant>>,
    pub(crate) failure: OperationalFailure,
}

impl Default for PipelineResources {
    fn default() -> Self {
        Self {
            publications: PublicationScope::pipeline(),
            publication_settlement: None,
            delivery: StageDelivery::default(),
            stage_joins: None,
            stages_joined: false,
            metrics: Arc::new(MetricsOwner::default()),
            prepared_metrics: None,
            metrics_join: None,
            metrics_joined: false,
            producer_tail: ProducerTail::Uncaptured,
            terminal_ack: Arc::new(OnceLock::new()),
            failure: Arc::new(OnceLock::new()),
        }
    }
}

impl PipelineResources {
    pub(super) fn retain_failure(&self, error: BoxError) {
        let _ = self.failure.set(SharedError::from(error));
    }

    pub(super) fn refresh_publications(&mut self) {
        self.publication_settlement = Some(self.publications.observe_accepted());
    }
}

#[derive(Default)]
struct MetricsSlot {
    handle: Option<Arc<MetricsHandle>>,
    writer: Option<obzenflow_core::event::WriterId>,
    cancelled: bool,
}

/// One child slot shared by the context, flow handle and lifetime guard.
#[derive(Default)]
pub(crate) struct MetricsOwner(Mutex<MetricsSlot>);

impl MetricsOwner {
    pub(crate) fn start(
        &self,
        prepared: crate::metrics::builder::PreparedMetricsAggregator,
    ) -> Result<(), BuilderError> {
        let mut slot = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if slot.cancelled || slot.handle.is_some() {
            return Err(BuilderError::Other(
                "metrics child cannot be started twice or after cancellation".into(),
            ));
        }
        // Hold the slot through the synchronous transfer. Emergency abort
        // either precedes spawning or sees the installed child.
        let writer = prepared.writer_id();
        slot.handle = Some(Arc::new(prepared.start()?));
        slot.writer = Some(writer);
        Ok(())
    }

    pub(crate) fn handle(&self) -> Option<Arc<MetricsHandle>> {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .handle
            .clone()
    }

    pub(super) fn writer_id(&self) -> Option<obzenflow_core::event::WriterId> {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).writer
    }

    pub(crate) fn request_abort(&self) {
        let mut slot = self.0.lock().unwrap_or_else(|e| e.into_inner());
        slot.cancelled = true;
        if let Some(handle) = &slot.handle {
            handle.request_abort();
        }
    }

    pub(crate) async fn abort_and_join(&self) -> Result<(), HandleError> {
        self.request_abort();
        match self.handle() {
            Some(handle) => handle.abort_and_wait().await,
            None => Ok(()),
        }
    }

    #[cfg(test)]
    pub(crate) fn install_for_test(&self, handle: MetricsHandle) {
        let mut slot = self.0.lock().unwrap();
        assert!(slot.handle.is_none());
        if slot.cancelled {
            handle.request_abort();
        }
        slot.handle = Some(Arc::new(handle));
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) enum StageCommand {
    Initialize,
    Ready,
    Start,
    Drain,
}

/// A bounded sequence of commands already authorised by FSM actions.
/// Dropping an unaccepted send cancels delivery, never the receiving task.
#[derive(Default)]
pub(super) struct StageDelivery {
    commands: VecDeque<(Arc<dyn StageHandle>, StageCommand)>,
    pending: Mutex<Option<BoxFuture<'static, Result<(), StageError>>>>,
}

impl StageDelivery {
    pub(super) fn enqueue(
        &mut self,
        handles: Vec<Arc<dyn StageHandle>>,
        commands: &[StageCommand],
        stage_count: usize,
    ) -> Result<(), StageError> {
        let count = handles.len().saturating_mul(commands.len());
        if self.commands.len() + count > stage_count.saturating_mul(4) {
            return Err(StageError::InvalidState(
                "pipeline command delivery capacity exceeded".into(),
            ));
        }
        for handle in handles {
            for command in commands {
                self.commands.push_back((handle.clone(), *command));
            }
        }
        Ok(())
    }

    pub(super) fn cancel(&mut self) {
        self.commands.clear();
        *self.pending.get_mut().unwrap_or_else(|e| e.into_inner()) = None;
    }

    pub(super) fn is_empty(&mut self) -> bool {
        self.commands.is_empty()
            && self
                .pending
                .get_mut()
                .unwrap_or_else(|e| e.into_inner())
                .is_none()
    }

    pub(super) fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<(), StageError>>> {
        let pending = self.pending.get_mut().unwrap_or_else(|e| e.into_inner());
        if pending.is_none() {
            let Some((handle, command)) = self.commands.pop_front() else {
                return Poll::Ready(None);
            };
            *pending = Some(
                async move {
                    match command {
                        StageCommand::Initialize => handle.initialize().await,
                        StageCommand::Ready => handle.ready().await,
                        StageCommand::Start => handle.start().await,
                        StageCommand::Drain => {
                            if handle.is_drained() {
                                return Ok(());
                            }
                            match handle.begin_drain().await {
                                Err(_) if handle.is_drained() => Ok(()),
                                result => result,
                            }
                        }
                    }
                }
                .boxed(),
            );
        }
        match pending.as_mut().expect("pending command").as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                *pending = None;
                Poll::Ready(Some(result))
            }
        }
    }
}
