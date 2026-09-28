// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Concrete child observations, mailbox delivery and owned publications.

use crate::metrics::MetricsHandle;
use crate::pipeline::fsm::PipelineFsmEvent;
use crate::stages::common::stage_handle::{StageError, StageHandle};
use crate::supervised_base::publication::{BoxError, PublicationScope, SharedError};
use crate::supervised_base::{BuilderError, HandleError, SupervisorHandle};
use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll};

pub(crate) type OperationalFailure = Arc<OnceLock<SharedError>>;
pub(super) type Observations = Mutex<FuturesUnordered<BoxFuture<'static, PipelineFsmEvent>>>;

pub(crate) struct PipelineResources {
    pub(super) publications: Arc<PublicationScope>,
    pub(super) publication_results: Observations,
    pub(super) acknowledgements: Observations,
    pub(super) failures: Observations,
    pub(super) exits: Observations,
    pub(super) delivery: StageDelivery,
    pub(crate) metrics: Arc<MetricsOwner>,
    pub(super) prepared_metrics: Option<crate::metrics::builder::PreparedMetricsAggregator>,
    pub(super) metrics_join: Option<Mutex<BoxFuture<'static, Result<(), HandleError>>>>,
    pub(crate) failure: OperationalFailure,
}

impl Default for PipelineResources {
    fn default() -> Self {
        Self {
            publications: PublicationScope::pipeline(),
            publication_results: Default::default(),
            acknowledgements: Default::default(),
            failures: Default::default(),
            exits: Default::default(),
            delivery: Default::default(),
            metrics: Arc::new(MetricsOwner::default()),
            prepared_metrics: None,
            metrics_join: None,
            failure: Arc::new(OnceLock::new()),
        }
    }
}

impl PipelineResources {
    pub(super) fn retain_failure(&self, error: BoxError) {
        let _ = self.failure.set(SharedError::from(error));
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

    #[cfg(any(test, feature = "test-support"))]
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
    Cancel,
}

type CommandDelivery = BoxFuture<'static, (obzenflow_core::StageId, Result<(), StageError>)>;

/// A bounded sequence of commands already authorised by FSM actions.
/// Dropping an unaccepted send cancels delivery, never the receiving task.
#[derive(Default)]
pub(super) struct StageDelivery {
    commands: VecDeque<(
        Arc<dyn StageHandle>,
        StageCommand,
        obzenflow_core::event::CausalFrontier,
    )>,
    pending: Mutex<Option<CommandDelivery>>,
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
                self.commands.push_back((
                    handle.clone(),
                    *command,
                    crate::supervised_base::publication::capture(),
                ));
            }
        }
        Ok(())
    }

    pub(super) fn cancel(&mut self) {
        self.commands.clear();
        *self.pending.get_mut().unwrap_or_else(|e| e.into_inner()) = None;
    }

    pub(super) fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<(obzenflow_core::StageId, Result<(), StageError>)>> {
        let pending = self.pending.get_mut().unwrap_or_else(|e| e.into_inner());
        if pending.is_none() {
            let Some((handle, command, frontier)) = self.commands.pop_front() else {
                return Poll::Ready(None);
            };
            *pending = Some(
                crate::supervised_base::publication::with_snapshot(frontier, async move {
                    let stage_id = handle.stage_id();
                    let result = match command {
                        StageCommand::Cancel => handle.force_shutdown().await,
                        StageCommand::Initialize => handle.initialize().await,
                        StageCommand::Ready => handle.ready().await,
                        StageCommand::Start => handle.start().await,
                        StageCommand::Drain => {
                            if handle.is_drained() {
                                return (stage_id, Ok(()));
                            }
                            match handle.begin_drain().await {
                                Err(_) if handle.is_drained() => Ok(()),
                                result => result,
                            }
                        }
                    };
                    (stage_id, result)
                })
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
