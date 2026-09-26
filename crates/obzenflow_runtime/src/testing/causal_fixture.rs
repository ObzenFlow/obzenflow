// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Shared commitment preparation for the existing in-memory test doubles.
use obzenflow_core::event::provenance::JournalProvenance;
use obzenflow_core::event::{CausalCoordinate, JournalEvent, JournalRecord, PreparedCausalCommit};
use obzenflow_core::journal::{AppendOptions, JournalError};
use obzenflow_core::{FlowId, JournalId};

/// Model a stored root input crossing a reader, for unit tests that start after
/// upstream commitment. This helper is absent from production/test-support APIs.
#[cfg(test)]
pub(crate) fn committed_input<T: JournalEvent>(
    writer: obzenflow_core::JournalWriterId,
    event: T,
) -> JournalRecord<T::Payload> {
    use obzenflow_core::journal::{JournalReader, JournalStorageReader};

    struct Input<T: JournalEvent>(Option<JournalRecord<T::Payload>>);

    #[async_trait::async_trait]
    impl<T: JournalEvent> JournalStorageReader<T> for Input<T> {
        async fn storage_next(
            &mut self,
        ) -> Result<Option<JournalRecord<T::Payload>>, JournalError> {
            Ok(self.0.take())
        }
        fn storage_position(&self) -> u64 {
            u64::from(self.0.is_none())
        }
    }

    futures::executor::block_on(Input::<T>(Some(JournalRecord::new(writer, event))).next())
        .expect("admit stored fixture input")
        .expect("one input")
}

pub(crate) fn coordinate(label: &str) -> CausalCoordinate {
    let value = label.bytes().fold(1_u128, |value, byte| {
        value.wrapping_mul(257).wrapping_add(u128::from(byte))
    });
    CausalCoordinate::new(JournalId::from(ulid::Ulid::from(value)).into())
}

pub(crate) fn commit<T: JournalEvent>(
    id: JournalId,
    event: T,
    options: &AppendOptions<T>,
    records: &[JournalRecord<T::Payload>],
) -> Result<JournalRecord<T::Payload>, JournalError> {
    let run_id = records.first().map_or_else(
        || FlowId::from(*id.as_ulid()),
        |record| record.envelope.provenance.journal.run_id,
    );
    let previous = records
        .last()
        .map(PreparedCausalCommit::from_record)
        .transpose()?;
    let (commitment, causal) = PreparedCausalCommit::prepare(
        run_id,
        CausalCoordinate::new(id.into()),
        *event.id(),
        previous.as_ref(),
        &options.frontier,
    )?;
    JournalRecord::commit_event(
        event,
        JournalProvenance {
            run_id,
            journal_writer_id: id.into(),
            vector_clock: commitment.clock,
            causal,
            timestamp: chrono::Utc::now(),
            journal_group_id: None,
            journal_group_member: None,
        },
    )
    .map_err(|error| JournalError::Implementation {
        message: "commit test fixture".into(),
        source: error.into(),
    })
}
