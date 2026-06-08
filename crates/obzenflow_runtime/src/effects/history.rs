// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;

#[async_trait]
pub trait EffectHistoryReader: Send {
    async fn read_effect(
        &mut self,
        cursor: &EffectCursor,
    ) -> Result<Option<EffectRecord>, EffectError>;
}

#[async_trait]
pub trait EffectHistoryStore: Send + Sync {
    async fn open_effect_history(
        &self,
        stage_key: &str,
    ) -> Result<Box<dyn EffectHistoryReader>, EffectError>;
}

#[derive(Clone, Debug)]
pub struct EffectHistory {
    recorded_flow_id: String,
    records: Arc<Vec<EffectRecord>>,
    index: Arc<HashMap<EffectCursor, Vec<usize>>>,
}

impl EffectHistory {
    pub async fn load(
        archive: &Arc<dyn ReplayArchive>,
        stage_key: &str,
    ) -> Result<Self, EffectError> {
        let mut reader = archive.open_effect_history(stage_key).await?;
        let mut records = Vec::new();

        while let Some(envelope) = reader
            .next()
            .await
            .map_err(|e| EffectError::ReplayArchive(e.to_string()))?
        {
            if let Some(record) = effect_record_from_event(&envelope.event)? {
                records.push(record);
            }
        }

        Self::from_records(archive.archive_flow_id().to_string(), records)
    }

    pub fn from_records(
        recorded_flow_id: String,
        records: Vec<EffectRecord>,
    ) -> Result<Self, EffectError> {
        let mut index = HashMap::new();
        for (position, record) in records.iter().enumerate() {
            index
                .entry(record.cursor.clone())
                .or_insert_with(Vec::new)
                .push(position);
        }

        for positions in index.values() {
            let group = positions
                .iter()
                .filter_map(|position| records.get(*position))
                .collect::<Vec<_>>();
            validate_effect_outcome_group(&group)?;
        }

        Ok(Self {
            recorded_flow_id,
            records: Arc::new(records),
            index: Arc::new(index),
        })
    }

    // FLOWIP-120a: the cursor IS the index key (populated in `from_records`), so a
    // found record's stored cursor equals the lookup cursor by construction; a
    // "found under a different cursor" state is unrepresentable and needs no explicit
    // cursor-mismatch error. The two real divergence axes are handled elsewhere: an
    // absent cursor yields `None` (a `MissingRecordedEffect` under strict replay), and
    // a present-but-diverged record is caught by the independent `descriptor_hash`
    // check (`DescriptorMismatch`).
    fn find(&self, cursor: &EffectCursor) -> Option<&EffectRecord> {
        self.index
            .get(cursor)
            .and_then(|positions| positions.first())
            .and_then(|position| self.records.get(*position))
    }

    pub(super) fn find_group(&self, cursor: &EffectCursor) -> Option<Vec<&EffectRecord>> {
        self.index.get(cursor).map(|positions| {
            positions
                .iter()
                .filter_map(|position| self.records.get(*position))
                .collect()
        })
    }

    pub fn recorded_flow_id(&self) -> &str {
        &self.recorded_flow_id
    }
}

#[async_trait]
impl EffectHistoryReader for EffectHistory {
    async fn read_effect(
        &mut self,
        cursor: &EffectCursor,
    ) -> Result<Option<EffectRecord>, EffectError> {
        Ok(self.find(cursor).cloned())
    }
}
