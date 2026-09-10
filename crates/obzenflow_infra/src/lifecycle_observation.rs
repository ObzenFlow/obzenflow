// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Infra's lifecycle read model. Folding is synchronous and has no Runtime
//! handles, clocks, controls or permission to classify unpublished execution.

use obzenflow_core::event::{
    PipelineLifecycleEvent as Lifecycle, PipelineStopAdmission, SystemEvent, SystemEventType,
    WriterId,
};
use obzenflow_core::journal::{Journal, JournalReader};
use obzenflow_core::{EventEnvelope, EventId};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Health {
    Reading,
    Healthy,
    Failed(String),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum Progress {
    #[default]
    Unknown,
    Starting,
    ReadyForRun,
    Running,
    Draining,
    Settled,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
    Completed,
    Cancelled,
    Failed(String),
    NotStarted,
}

#[derive(Clone, Debug)]
pub(crate) struct Projection {
    writer: WriterId,
    pub(crate) health: Health,
    pub(crate) admission: Option<PipelineStopAdmission>,
    pub(crate) progress: Progress,
    pub(crate) outcome: Option<Outcome>,
    pub(crate) terminal_id: Option<EventId>,
}

impl Projection {
    pub(crate) fn new(writer: WriterId) -> Self {
        Self {
            writer,
            health: Health::Reading,
            admission: None,
            progress: Progress::Unknown,
            outcome: None,
            terminal_id: None,
        }
    }

    pub(crate) fn fail(&mut self, error: impl ToString) {
        self.health = Health::Failed(error.to_string());
    }

    pub(crate) fn fold(&mut self, envelope: &EventEnvelope<SystemEvent>) {
        if envelope.event.writer_id != self.writer || matches!(self.health, Health::Failed(_)) {
            return;
        }
        let SystemEventType::PipelineLifecycle(event) = &envelope.event.event else {
            return;
        };
        let outcome = match event {
            Lifecycle::Completed { .. } => Some(Outcome::Completed),
            Lifecycle::Cancelled { .. } => Some(Outcome::Cancelled),
            Lifecycle::Failed { reason, .. } => Some(Outcome::Failed(reason.clone())),
            Lifecycle::NotStarted => Some(Outcome::NotStarted),
            _ => None,
        };
        if let Some(outcome) = outcome {
            if self.terminal_id.is_some_and(|id| id != envelope.event.id) {
                self.fail("pipeline lifecycle integrity error: multiple terminal facts");
            } else {
                self.terminal_id = Some(envelope.event.id);
                self.outcome = Some(outcome);
            }
            return;
        }
        match event {
            Lifecycle::StopAdmitted { admission } if self.outcome.is_none() => {
                if !matches!(self.admission, Some(PipelineStopAdmission::Cancel { .. })) {
                    self.admission = Some(admission.clone());
                }
            }
            Lifecycle::Drained => self.progress = Progress::Settled,
            _ if self.outcome.is_some() => {}
            Lifecycle::Starting => self.progress = Progress::Starting,
            Lifecycle::ReadyForRun { .. } => self.progress = Progress::ReadyForRun,
            Lifecycle::Running { .. } => self.progress = Progress::Running,
            Lifecycle::Draining { .. } | Lifecycle::AllStagesCompleted { .. } => {
                self.progress = Progress::Draining
            }
            _ => {}
        }
    }
}

pub(crate) struct Reader {
    journal: Option<Arc<dyn Journal<SystemEvent>>>,
    reader: Option<Box<dyn JournalReader<SystemEvent>>>,
    pub(crate) projection: Projection,
}

impl Reader {
    pub(crate) fn new(journal: Arc<dyn Journal<SystemEvent>>, writer: WriterId) -> Self {
        Self {
            journal: Some(journal),
            reader: None,
            projection: Projection::new(writer),
        }
    }

    #[cfg(test)]
    pub(crate) fn from_reader(
        reader: Box<dyn JournalReader<SystemEvent>>,
        writer: WriterId,
    ) -> Self {
        Self {
            journal: None,
            reader: Some(reader),
            projection: Projection::new(writer),
        }
    }

    /// True means temporary physical EOF or an observation failure. The cursor
    /// remains attached at EOF, so later commits are still observable.
    pub(crate) async fn catch_up(&mut self) -> bool {
        if matches!(self.projection.health, Health::Failed(_)) {
            return true;
        }
        if self.reader.is_none() {
            match self.journal.as_ref().expect("reader source").reader().await {
                Ok(reader) => self.reader = Some(reader),
                Err(error) => {
                    self.projection.fail(error);
                    return true;
                }
            }
        }
        for _ in 0..256 {
            match self.reader.as_mut().expect("opened").next().await {
                Ok(Some(envelope)) => self.projection.fold(&envelope),
                Ok(None) => {
                    if !matches!(self.projection.health, Health::Failed(_)) {
                        self.projection.health = Health::Healthy;
                    }
                    return true;
                }
                Err(error) => {
                    self.projection.fail(error);
                    return true;
                }
            }
        }
        false
    }
}

/// Read transport shared by application waiters. It carries observed facts,
/// never a stop deadline or cached Runtime execution result.
#[derive(Clone)]
pub(crate) struct Feed {
    snapshot: tokio::sync::watch::Receiver<Projection>,
    barriers: tokio::sync::mpsc::Sender<tokio::sync::oneshot::Sender<Projection>>,
}

impl Feed {
    pub(crate) fn spawn(
        journal: Arc<dyn Journal<SystemEvent>>,
        writer: WriterId,
    ) -> (Self, tokio::task::JoinHandle<()>) {
        let mut reader = Reader::new(journal, writer);
        let (updates, snapshot) = tokio::sync::watch::channel(reader.projection.clone());
        let (barriers, mut requests) =
            tokio::sync::mpsc::channel::<tokio::sync::oneshot::Sender<Projection>>(8);
        let task = tokio::spawn(async move {
            let mut barrier = None;
            loop {
                if barrier.is_none() {
                    barrier = requests.try_recv().ok();
                }
                let at_end = reader.catch_up().await;
                updates.send_replace(reader.projection.clone());
                if at_end {
                    if let Some(barrier) = barrier.take() {
                        let _ = barrier.send(reader.projection.clone());
                    }
                    tokio::select! {
                        request = requests.recv() => match request {
                            Some(request) => barrier = Some(request),
                            None => break,
                        },
                        _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {},
                    }
                } else {
                    tokio::task::yield_now().await;
                }
            }
        });
        (Self { snapshot, barriers }, task)
    }

    pub(crate) fn snapshot(&self) -> Projection {
        self.snapshot.borrow().clone()
    }

    /// Called after resource joining. This always asks for another catch-up,
    /// so an older temporary EOF cannot establish an absent-terminal error.
    pub(crate) async fn settled(&self) -> Projection {
        let (send, receive) = tokio::sync::oneshot::channel();
        if self.barriers.send(send).await.is_ok() {
            if let Ok(projection) = receive.await {
                return projection;
            }
        }
        let mut projection = self.snapshot();
        projection.fail("pipeline lifecycle observation reader stopped");
        projection
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::types::DurationMs;
    use obzenflow_core::event::{PipelineCancellationCause, SystemEventFactory};
    use obzenflow_core::id::SystemId;

    #[tokio::test]
    async fn admission_progress_and_terminal_integrity_are_independent() {
        let writer = SystemId::new();
        let journal =
            crate::journal::MemoryJournal::with_owner(obzenflow_core::JournalOwner::system(writer));
        let factory = SystemEventFactory::new(writer);
        let mut projection = Projection::new(writer.into());
        projection.fold(
            &journal
                .append(
                    SystemEventFactory::new(SystemId::new()).pipeline_not_started(),
                    None,
                )
                .await
                .unwrap(),
        );
        assert_eq!(projection.outcome, None);
        projection.fold(
            &journal
                .append(factory.pipeline_running(), None)
                .await
                .unwrap(),
        );
        let cancel = PipelineStopAdmission::Cancel {
            cause: PipelineCancellationCause::Requested,
        };
        for admission in [
            cancel.clone(),
            PipelineStopAdmission::Graceful {
                timeout_ms: DurationMs(100),
            },
        ] {
            projection.fold(
                &journal
                    .append(factory.pipeline_stop_admitted(admission), None)
                    .await
                    .unwrap(),
            );
        }
        assert_eq!(projection.progress, Progress::Running);
        assert_eq!(projection.admission, Some(cancel));
        let terminal = journal
            .append(factory.pipeline_not_started(), None)
            .await
            .unwrap();
        projection.fold(&terminal);
        projection.fold(&terminal);
        assert!(!matches!(projection.health, Health::Failed(_)));
        projection.fold(
            &journal
                .append(factory.pipeline_drained(), None)
                .await
                .unwrap(),
        );
        assert_eq!(projection.progress, Progress::Settled);
        projection.fold(
            &journal
                .append(factory.pipeline_not_started(), None)
                .await
                .unwrap(),
        );
        assert!(
            matches!(projection.health, Health::Failed(ref error) if error.contains("multiple terminal facts"))
        );
        assert_eq!(projection.terminal_id, Some(terminal.event.id));
        assert_eq!(projection.outcome, Some(Outcome::NotStarted));
    }

    #[tokio::test]
    async fn bounded_reader_retains_its_cursor_across_temporary_eof_and_join_barriers() {
        let writer = SystemId::new();
        let journal: Arc<dyn Journal<SystemEvent>> = Arc::new(
            crate::journal::MemoryJournal::with_owner(obzenflow_core::JournalOwner::system(writer)),
        );
        let factory = SystemEventFactory::new(writer);
        for _ in 0..300 {
            journal
                .append(factory.pipeline_running(), None)
                .await
                .unwrap();
        }
        let mut reader = Reader::new(journal.clone(), writer.into());
        assert!(!reader.catch_up().await);
        assert!(reader.catch_up().await);
        assert_eq!(reader.reader.as_ref().unwrap().position(), 300);
        assert_eq!(reader.projection.health, Health::Healthy);
        let (feed, task) = Feed::spawn(journal.clone(), writer.into());
        assert_eq!(feed.settled().await.outcome, None);
        journal
            .append(factory.pipeline_not_started(), None)
            .await
            .unwrap();
        assert!(reader.catch_up().await);
        assert_eq!(reader.projection.outcome, Some(Outcome::NotStarted));
        assert_eq!(feed.settled().await.outcome, Some(Outcome::NotStarted));
        drop(feed);
        task.await.unwrap();
    }

    struct FailingReader(Option<EventEnvelope<SystemEvent>>);
    #[async_trait::async_trait]
    impl JournalReader<SystemEvent> for FailingReader {
        async fn next(
            &mut self,
        ) -> Result<Option<EventEnvelope<SystemEvent>>, obzenflow_core::journal::JournalError>
        {
            match self.0.take() {
                Some(event) => Ok(Some(event)),
                None => Err(obzenflow_core::journal::JournalError::Full),
            }
        }
        fn position(&self) -> u64 {
            u64::from(self.0.is_none())
        }
    }

    #[tokio::test]
    async fn reader_failure_preserves_the_last_acknowledged_fact() {
        let writer = SystemId::new();
        let journal =
            crate::journal::MemoryJournal::with_owner(obzenflow_core::JournalOwner::system(writer));
        let terminal = journal
            .append(SystemEventFactory::new(writer).pipeline_not_started(), None)
            .await
            .unwrap();
        let mut reader = Reader::from_reader(
            Box::new(FailingReader(Some(terminal.clone()))),
            writer.into(),
        );
        assert!(reader.catch_up().await);
        assert!(matches!(reader.projection.health, Health::Failed(_)));
        assert_eq!(reader.projection.outcome, Some(Outcome::NotStarted));
        assert_eq!(reader.projection.terminal_id, Some(terminal.event.id));
    }
}
