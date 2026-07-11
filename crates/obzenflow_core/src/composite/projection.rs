// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Ordered, rebuildable lifecycle view for first-class composites.

use crate::event::StageLifecycleEvent;
use crate::id::{CompositeId, RoleId, StageId};
use std::collections::{BTreeMap, BTreeSet};

/// One composite's manifest-derived lifecycle definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompositeDefinition {
    composite_id: CompositeId,
    members: Vec<(StageId, RoleId)>,
}

impl CompositeDefinition {
    /// Build a definition from the composite identity and its member-role map.
    pub fn new(composite_id: CompositeId, members: Vec<(StageId, RoleId)>) -> Self {
        Self {
            composite_id,
            members,
        }
    }

    /// The manifest identity this definition describes.
    pub fn composite_id(&self) -> &CompositeId {
        &self.composite_id
    }
}

/// State-derived lifecycle output for a composite binding.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum CompositeStatus {
    Waiting,
    Running,
    Completed,
    Cancelled { reason: String },
    Failed { at: RoleId, error: String },
    Invalid { error: String },
}

/// A malformed definition or contradictory member lifecycle tape.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum CompositeProjectionError {
    #[error("composite {composite} has no lifecycle members")]
    EmptyComposite { composite: CompositeId },

    #[error("composite {composite} declares member {stage} more than once")]
    DuplicateMember {
        composite: CompositeId,
        stage: StageId,
    },

    #[error("composite {composite} declares role {role} more than once")]
    DuplicateRole {
        composite: CompositeId,
        role: RoleId,
    },

    #[error("composite {composite} is defined more than once")]
    DuplicateComposite { composite: CompositeId },

    #[error("stage {stage} belongs to both composite {first} and composite {second}")]
    StageInMultipleComposites {
        stage: StageId,
        first: CompositeId,
        second: CompositeId,
    },

    #[error(
        "composite {composite} member {stage} reported conflicting terminal states: {previous} then {incoming}"
    )]
    ConflictingTerminal {
        composite: CompositeId,
        stage: StageId,
        previous: &'static str,
        incoming: &'static str,
    },
}

impl CompositeProjectionError {
    /// Composite whose definition or history is invalid, when one is known.
    pub fn composite_id(&self) -> Option<&CompositeId> {
        match self {
            Self::EmptyComposite { composite }
            | Self::DuplicateMember { composite, .. }
            | Self::DuplicateRole { composite, .. }
            | Self::DuplicateComposite { composite }
            | Self::ConflictingTerminal { composite, .. } => Some(composite),
            Self::StageInMultipleComposites { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AttributedFailure {
    at: RoleId,
    error: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MemberTerminal {
    Completed,
    Cancelled { reason: String },
    Failed { error: String },
}

impl MemberTerminal {
    const fn name(&self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Cancelled { .. } => "cancelled",
            Self::Failed { .. } => "failed",
        }
    }

    fn semantically_matches(&self, other: &Self) -> bool {
        self == other
    }
}

#[derive(Debug, Clone)]
struct CompositeState {
    composite_id: CompositeId,
    members: BTreeSet<StageId>,
    role_of: BTreeMap<StageId, RoleId>,
    terminal_of: BTreeMap<StageId, MemberTerminal>,
    first_failure: Option<AttributedFailure>,
    first_cancellation: Option<String>,
    started: bool,
    integrity_error: Option<CompositeProjectionError>,
}

impl CompositeState {
    fn from_definition(definition: CompositeDefinition) -> Result<Self, CompositeProjectionError> {
        let CompositeDefinition {
            composite_id,
            members,
        } = definition;

        if members.is_empty() {
            return Err(CompositeProjectionError::EmptyComposite {
                composite: composite_id,
            });
        }

        let mut member_set = BTreeSet::new();
        let mut role_set = BTreeSet::new();
        let mut role_of = BTreeMap::new();
        for (stage, role) in members {
            if !member_set.insert(stage) {
                return Err(CompositeProjectionError::DuplicateMember {
                    composite: composite_id,
                    stage,
                });
            }
            if !role_set.insert(role.clone()) {
                return Err(CompositeProjectionError::DuplicateRole {
                    composite: composite_id,
                    role,
                });
            }
            role_of.insert(stage, role);
        }

        Ok(Self {
            composite_id,
            members: member_set,
            role_of,
            terminal_of: BTreeMap::new(),
            first_failure: None,
            first_cancellation: None,
            started: false,
            integrity_error: None,
        })
    }

    fn status(&self) -> CompositeStatus {
        if let Some(error) = &self.integrity_error {
            return CompositeStatus::Invalid {
                error: error.to_string(),
            };
        }

        if let Some(failure) = &self.first_failure {
            return CompositeStatus::Failed {
                at: failure.at.clone(),
                error: failure.error.clone(),
            };
        }

        if self.terminal_of.len() == self.members.len() {
            if let Some(reason) = &self.first_cancellation {
                return CompositeStatus::Cancelled {
                    reason: reason.clone(),
                };
            }
            return CompositeStatus::Completed;
        }

        if self.started {
            CompositeStatus::Running
        } else {
            CompositeStatus::Waiting
        }
    }

    fn apply(
        &mut self,
        stage: StageId,
        event: &StageLifecycleEvent,
    ) -> Result<(), CompositeProjectionError> {
        if self.integrity_error.is_some() || !self.members.contains(&stage) {
            return Ok(());
        }

        match event {
            StageLifecycleEvent::Running => {
                self.started = true;
                Ok(())
            }
            StageLifecycleEvent::Draining { .. } => Ok(()),
            StageLifecycleEvent::Drained | StageLifecycleEvent::Completed { .. } => {
                self.record_terminal(stage, MemberTerminal::Completed)
            }
            StageLifecycleEvent::Cancelled { reason, .. } => {
                let terminal = MemberTerminal::Cancelled {
                    reason: reason.clone(),
                };
                let was_new = !self.terminal_of.contains_key(&stage);
                self.record_terminal(stage, terminal)?;
                if was_new && self.first_cancellation.is_none() {
                    self.first_cancellation = Some(reason.clone());
                }
                Ok(())
            }
            StageLifecycleEvent::Failed { error, .. } => {
                let terminal = MemberTerminal::Failed {
                    error: error.clone(),
                };
                let was_new = !self.terminal_of.contains_key(&stage);
                self.record_terminal(stage, terminal)?;
                if was_new && self.first_failure.is_none() {
                    let at = self
                        .role_of
                        .get(&stage)
                        .expect("validated composite member always has a role")
                        .clone();
                    self.first_failure = Some(AttributedFailure {
                        at,
                        error: error.clone(),
                    });
                }
                Ok(())
            }
        }
    }

    fn record_terminal(
        &mut self,
        stage: StageId,
        incoming: MemberTerminal,
    ) -> Result<(), CompositeProjectionError> {
        if let Some(previous) = self.terminal_of.get(&stage) {
            if previous.semantically_matches(&incoming) {
                return Ok(());
            }

            let error = CompositeProjectionError::ConflictingTerminal {
                composite: self.composite_id.clone(),
                stage,
                previous: previous.name(),
                incoming: incoming.name(),
            };
            self.integrity_error = Some(error.clone());
            return Err(error);
        }

        self.terminal_of.insert(stage, incoming);
        Ok(())
    }
}

/// Pure lifecycle read model over all composites in one topology.
#[derive(Debug, Clone)]
pub struct CompositeLifecycleProjection {
    states: BTreeMap<CompositeId, CompositeState>,
    composite_by_stage: BTreeMap<StageId, CompositeId>,
}

impl CompositeLifecycleProjection {
    /// Build and validate the manifest-derived definitions.
    pub fn new(
        definitions: impl IntoIterator<Item = CompositeDefinition>,
    ) -> Result<Self, CompositeProjectionError> {
        let mut states = BTreeMap::new();
        let mut composite_by_stage = BTreeMap::new();

        for definition in definitions {
            let state = CompositeState::from_definition(definition)?;
            let composite_id = state.composite_id.clone();
            if states.contains_key(&composite_id) {
                return Err(CompositeProjectionError::DuplicateComposite {
                    composite: composite_id,
                });
            }

            for stage in &state.members {
                if let Some(first) = composite_by_stage.insert(*stage, composite_id.clone()) {
                    return Err(CompositeProjectionError::StageInMultipleComposites {
                        stage: *stage,
                        first,
                        second: composite_id,
                    });
                }
            }

            states.insert(composite_id, state);
        }

        Ok(Self {
            states,
            composite_by_stage,
        })
    }

    /// Composite containing `stage`, if the topology declares one.
    pub fn composite_for_stage(&self, stage: StageId) -> Option<&CompositeId> {
        self.composite_by_stage.get(&stage)
    }

    /// Fold one member lifecycle fact in caller-supplied append order.
    pub fn apply(
        &mut self,
        stage: StageId,
        event: &StageLifecycleEvent,
    ) -> Result<(), CompositeProjectionError> {
        let Some(composite_id) = self.composite_by_stage.get(&stage).cloned() else {
            return Ok(());
        };
        self.states
            .get_mut(&composite_id)
            .expect("validated stage index references an existing composite")
            .apply(stage, event)
    }

    /// Current state-derived status for one composite.
    pub fn status(&self, composite_id: &CompositeId) -> Option<CompositeStatus> {
        self.states.get(composite_id).map(CompositeState::status)
    }

    /// Deterministically ordered snapshot of every composite status.
    pub fn statuses(&self) -> Vec<(CompositeId, CompositeStatus)> {
        self.states
            .iter()
            .map(|(id, state)| (id.clone(), state.status()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn definition(composite: &str, members: &[(StageId, &str)]) -> CompositeDefinition {
        CompositeDefinition::new(
            CompositeId::new(composite),
            members
                .iter()
                .map(|(stage, role)| (*stage, RoleId::new(*role)))
                .collect(),
        )
    }

    fn projection() -> (CompositeLifecycleProjection, StageId, StageId) {
        let map = StageId::new();
        let finish = StageId::new();
        let projection = CompositeLifecycleProjection::new([definition(
            "ai_map_reduce:digest",
            &[(map, "map"), (finish, "finalize")],
        )])
        .unwrap();
        (projection, map, finish)
    }

    fn completed() -> StageLifecycleEvent {
        StageLifecycleEvent::Completed { metrics: None }
    }

    fn cancelled(reason: &str) -> StageLifecycleEvent {
        StageLifecycleEvent::Cancelled {
            reason: reason.to_string(),
            metrics: None,
        }
    }

    fn failed(error: &str) -> StageLifecycleEvent {
        StageLifecycleEvent::Failed {
            error: error.to_string(),
            recoverable: None,
            metrics: None,
        }
    }

    fn id() -> CompositeId {
        CompositeId::new("ai_map_reduce:digest")
    }

    #[test]
    fn waiting_running_and_all_completed_are_state_derived() {
        let (mut projection, map, finish) = projection();
        assert_eq!(projection.status(&id()), Some(CompositeStatus::Waiting));

        projection
            .apply(map, &StageLifecycleEvent::Running)
            .unwrap();
        assert_eq!(projection.status(&id()), Some(CompositeStatus::Running));

        projection.apply(map, &completed()).unwrap();
        assert_eq!(projection.status(&id()), Some(CompositeStatus::Running));
        projection
            .apply(finish, &StageLifecycleEvent::Drained)
            .unwrap();
        assert_eq!(projection.status(&id()), Some(CompositeStatus::Completed));
    }

    #[test]
    fn first_failure_is_fail_fast_and_append_order_attributed() {
        let (mut first_map, map, finish) = projection();
        first_map.apply(map, &failed("map failed")).unwrap();
        first_map.apply(finish, &failed("finalize failed")).unwrap();
        assert_eq!(
            first_map.status(&id()),
            Some(CompositeStatus::Failed {
                at: RoleId::new("map"),
                error: "map failed".to_string(),
            })
        );

        let (mut first_finish, map, finish) = projection();
        first_finish
            .apply(finish, &failed("finalize failed"))
            .unwrap();
        first_finish.apply(map, &failed("map failed")).unwrap();
        assert_eq!(
            first_finish.status(&id()),
            Some(CompositeStatus::Failed {
                at: RoleId::new("finalize"),
                error: "finalize failed".to_string(),
            })
        );
    }

    #[test]
    fn cancellation_waits_for_all_members_and_keeps_first_reason() {
        let (mut projection, map, finish) = projection();
        projection.apply(map, &cancelled("operator stop")).unwrap();
        assert_eq!(projection.status(&id()), Some(CompositeStatus::Waiting));
        projection.apply(finish, &cancelled("timeout")).unwrap();
        assert_eq!(
            projection.status(&id()),
            Some(CompositeStatus::Cancelled {
                reason: "operator stop".to_string(),
            })
        );
    }

    #[test]
    fn failure_overrides_an_unresolved_cancellation() {
        let (mut projection, map, finish) = projection();
        projection.apply(map, &cancelled("operator stop")).unwrap();
        projection.apply(finish, &failed("boom")).unwrap();
        assert!(matches!(
            projection.status(&id()),
            Some(CompositeStatus::Failed { at, error })
                if at.as_str() == "finalize" && error == "boom"
        ));
    }

    #[test]
    fn exact_duplicate_terminal_is_idempotent() {
        let (mut projection, map, _finish) = projection();
        projection.apply(map, &completed()).unwrap();
        projection.apply(map, &completed()).unwrap();
        assert_eq!(projection.status(&id()), Some(CompositeStatus::Waiting));
    }

    #[test]
    fn conflicting_terminal_makes_the_view_invalid() {
        let (mut projection, map, _finish) = projection();
        projection.apply(map, &completed()).unwrap();
        let error = projection
            .apply(map, &cancelled("late stop"))
            .expect_err("contradictory tape must fail integrity");
        assert!(matches!(
            error,
            CompositeProjectionError::ConflictingTerminal {
                previous: "completed",
                incoming: "cancelled",
                ..
            }
        ));
        assert!(matches!(
            projection.status(&id()),
            Some(CompositeStatus::Invalid { error })
                if error.contains("conflicting terminal states")
        ));
    }

    #[test]
    fn same_ordered_history_rebuilds_identical_statuses() {
        let (first, map, finish) = projection();
        let history = [
            (map, StageLifecycleEvent::Running),
            (finish, StageLifecycleEvent::Running),
            (map, cancelled("operator stop")),
            (finish, completed()),
        ];

        let replay = |mut projection: CompositeLifecycleProjection| {
            for (stage, event) in &history {
                projection.apply(*stage, event).unwrap();
            }
            projection.statuses()
        };

        assert_eq!(replay(first.clone()), replay(first));
    }

    #[test]
    fn non_member_lifecycle_is_ignored() {
        let (mut projection, _map, _finish) = projection();
        projection
            .apply(StageId::new(), &StageLifecycleEvent::Running)
            .unwrap();
        assert_eq!(projection.status(&id()), Some(CompositeStatus::Waiting));
    }

    #[test]
    fn one_stage_cannot_belong_to_two_composites() {
        let stage = StageId::new();
        let error = CompositeLifecycleProjection::new([
            definition("first", &[(stage, "member")]),
            definition("second", &[(stage, "member")]),
        ])
        .expect_err("ambiguous membership must fail");
        assert!(matches!(
            error,
            CompositeProjectionError::StageInMultipleComposites { .. }
        ));
    }
}
