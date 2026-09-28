// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Trusted storage implementation port. Runtime consumers use [`Journal`].

use super::journal_owner::JournalOwner;
use super::{AppendOptions, Journal, JournalConfig, JournalError, JournalReader};
use crate::event::{CausalError, JournalEvent, JournalRecord};
use crate::{EventId, JournalId};
use async_trait::async_trait;

#[cfg(test)]
mod tests;

/// Storage adapters implement this port to receive the core-owned [`Journal`]
/// facade. Its successful operations are the admission boundary for causal
/// evidence. Constructing, preparing or decoding a record is not admission.
///
/// The commit, cancellation, group atomicity and read contracts of [`Journal`]
/// apply here. In particular, returning `Ok` asserts actual commitment, and read
/// operations must establish membership in the admitted journal. Implementing a
/// storage adapter is a trust boundary; core cannot prove a dishonest provider's
/// I/O. There is deliberately no public operation that promotes an arbitrary
/// record into evidence.
#[async_trait]
pub trait JournalStorage<T: JournalEvent>: Send + Sync {
    fn storage_id(&self) -> &JournalId;
    fn storage_owner(&self) -> Option<&JournalOwner>;

    async fn storage_append(
        &self,
        event: T,
        options: AppendOptions<T>,
    ) -> Result<JournalRecord<T::Payload>, JournalError>;

    async fn storage_append_group(
        &self,
        group_id: &str,
        events: Vec<T>,
        options: AppendOptions<T>,
    ) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
        match events.len() {
            0 => Ok(Vec::new()),
            1 => Ok(vec![
                self.storage_append(events.into_iter().next().expect("one event"), options)
                    .await?,
            ]),
            count => Err(JournalError::AtomicAppendUnsupported {
                group_id: group_id.to_owned(),
                member_count: count,
            }),
        }
    }

    async fn storage_read_all_unordered(
        &self,
    ) -> Result<Vec<JournalRecord<T::Payload>>, JournalError>;
    async fn storage_read_event(
        &self,
        event_id: &EventId,
    ) -> Result<Option<JournalRecord<T::Payload>>, JournalError>;
    async fn storage_reader_from(
        &self,
        position: u64,
    ) -> Result<Box<dyn JournalReader<T>>, JournalError>;
    async fn storage_read_last_n(
        &self,
        count: usize,
    ) -> Result<Vec<JournalRecord<T::Payload>>, JournalError>;

    async fn storage_committed_position(&self) -> Result<u64, JournalError> {
        Ok(self
            .storage_read_last_n(1)
            .await?
            .first()
            .map_or(0, JournalRecord::local_sequence))
    }

    fn storage_observation_reader(&self) -> Option<&dyn super::JournalObservationReader> {
        None
    }

    async fn storage_read_metrics_tail(
        &self,
    ) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
        Ok(Vec::new())
    }

    fn storage_configure(&self, config: JournalConfig) -> Result<(), JournalError> {
        match config.observability {
            super::ObservabilityPolicy::EveryRecord => Ok(()),
            super::ObservabilityPolicy::Periodic { .. } => Err(JournalError::Implementation {
                message: "This journal does not support sparse observability".into(),
                source: "unsupported observability policy".into(),
            }),
        }
    }
}

fn admit<T: JournalEvent>(
    record: JournalRecord<T::Payload>,
    journal: &JournalId,
) -> Result<JournalRecord<T::Payload>, CausalError> {
    if record.causal_coordinate().journal_writer_id.as_journal_id() != journal {
        return Err(CausalError::ConflictingCommitment);
    }
    record.admit()
}

#[async_trait]
impl<T: JournalEvent, S: JournalStorage<T> + ?Sized> Journal<T> for S {
    fn id(&self) -> &JournalId {
        self.storage_id()
    }
    fn owner(&self) -> Option<&JournalOwner> {
        self.storage_owner()
    }
    fn observation_reader(&self) -> Option<&dyn super::JournalObservationReader> {
        self.storage_observation_reader()
    }
    fn configure(&self, config: JournalConfig) -> Result<(), JournalError> {
        self.storage_configure(config)
    }
    async fn committed_position(&self) -> Result<u64, JournalError> {
        self.storage_committed_position().await
    }
    async fn append(
        &self,
        event: T,
        options: AppendOptions<T>,
    ) -> Result<JournalRecord<T::Payload>, JournalError> {
        let record = self.storage_append(event, options).await?;
        // Storage already reported commitment. Admission failure must never
        // certify rollback or invite an automatic retry.
        admit::<T>(record, self.storage_id()).map_err(|source| JournalError::CommitIndeterminate {
            source: Box::new(source),
        })
    }
    async fn append_group(
        &self,
        group_id: &str,
        events: Vec<T>,
        options: AppendOptions<T>,
    ) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
        let records = self.storage_append_group(group_id, events, options).await?;
        records
            .into_iter()
            .map(|record| admit::<T>(record, self.storage_id()))
            .collect::<Result<_, _>>()
            .map_err(|source| JournalError::CommitIndeterminate {
                source: Box::new(source),
            })
    }
    async fn read_all_unordered(&self) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
        self.storage_read_all_unordered()
            .await?
            .into_iter()
            .map(|record| Ok(admit::<T>(record, self.storage_id())?))
            .collect()
    }
    async fn read_event(
        &self,
        event_id: &EventId,
    ) -> Result<Option<JournalRecord<T::Payload>>, JournalError> {
        self.storage_read_event(event_id)
            .await?
            .map(|record| Ok(admit::<T>(record, self.storage_id())?))
            .transpose()
    }
    async fn reader_from(&self, position: u64) -> Result<Box<dyn JournalReader<T>>, JournalError> {
        self.storage_reader_from(position).await
    }
    async fn read_last_n(
        &self,
        count: usize,
    ) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
        self.storage_read_last_n(count)
            .await?
            .into_iter()
            .map(|record| Ok(admit::<T>(record, self.storage_id())?))
            .collect()
    }
    async fn read_metrics_tail(&self) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
        self.storage_read_metrics_tail()
            .await?
            .into_iter()
            .map(|record| Ok(admit::<T>(record, self.storage_id())?))
            .collect()
    }
}
