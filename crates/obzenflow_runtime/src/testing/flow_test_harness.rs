// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Test-only harness that composes with a production [`FlowHandle`] while
//! carrying additional test-only state (FLOWIP-114h).

use crate::pipeline::FlowHandle;
use crate::testing::stage_journal::StageJournalLookupError;
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::journal::Journal;
use obzenflow_core::StageId;
use std::collections::HashMap;
use std::sync::Arc;

/// Opaque stage-journal map owned by the test harness.
///
/// The implementation detail is intentionally private so the backing store can
/// change without forcing migrations at call sites.
struct StageJournalMap {
    stage_data_journals: HashMap<StageId, Arc<dyn Journal<ChainEvent>>>,
}

impl StageJournalMap {
    fn from_stage_list(
        stage_data_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,
    ) -> Result<Self, StageJournalLookupError> {
        let mut map: HashMap<StageId, Arc<dyn Journal<ChainEvent>>> = HashMap::new();
        for (stage_id, journal) in stage_data_journals {
            if map.insert(stage_id, journal).is_some() {
                return Err(StageJournalLookupError::DuplicateStageJournal(stage_id));
            }
        }
        Ok(Self {
            stage_data_journals: map,
        })
    }

    fn get(&self, stage_id: StageId) -> Option<&Arc<dyn Journal<ChainEvent>>> {
        self.stage_data_journals.get(&stage_id)
    }
}

/// Test-only wrapper around a production [`FlowHandle`].
///
/// This type exists to keep `FlowHandle` and its construction path free of
/// test-only fields and cfg-gated methods.
pub struct FlowTestHarness {
    inner: FlowHandle,
    stage_journals: StageJournalMap,
}

impl FlowTestHarness {
    /// Build a harness from a production `FlowHandle` and the stage data-journal
    /// list produced during flow construction.
    ///
    /// Integration tests should obtain a harness through the `test_flow!` factory
    /// or builder. This lower-level constructor exists for the factory itself and
    /// focused unit tests.
    pub fn from_parts(
        inner: FlowHandle,
        stage_data_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,
    ) -> Result<Self, StageJournalLookupError> {
        let stage_journals = StageJournalMap::from_stage_list(stage_data_journals)?;
        Ok(Self {
            inner,
            stage_journals,
        })
    }

    /// Resolve a stage data journal by stage name through the flow topology.
    pub fn stage_journal_for_test(
        &self,
        stage_name: &str,
    ) -> Result<(StageId, Arc<dyn Journal<ChainEvent>>), StageJournalLookupError> {
        use crate::id_conversions::StageIdExt;

        let topology = self
            .inner
            .topology()
            .ok_or(StageJournalLookupError::MissingTopology)?;

        let mut matches: Vec<StageId> = topology
            .stages()
            .filter(|s| s.name == stage_name)
            .map(|s| StageId::from_topology_id(s.id))
            .collect();

        let stage_id = match matches.len() {
            0 => return Err(StageJournalLookupError::UnknownStage(stage_name.to_string())),
            1 => matches.remove(0),
            _ => return Err(StageJournalLookupError::AmbiguousStage(stage_name.to_string())),
        };

        let journal = self
            .stage_journals
            .get(stage_id)
            .ok_or_else(|| StageJournalLookupError::MissingStageJournal(stage_name.to_string()))?;

        Ok((stage_id, journal.clone()))
    }

    /// Unwrap and return the inner production handle.
    pub fn into_inner(self) -> FlowHandle {
        self.inner
    }
}

impl std::ops::Deref for FlowTestHarness {
    type Target = FlowHandle;

    fn deref(&self) -> &FlowHandle {
        &self.inner
    }
}
