// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Runtime feed-plan metadata for logical edge selection.
//!
//! The feed plan is the runtime-owned projection of DSL typing metadata. It is
//! intentionally separate from the published topology schema so routing and
//! contract gating can evolve without requiring an `obzenflow-topology` version
//! bump for each internal contract change.

use obzenflow_core::StageId;
use obzenflow_topology::TypeHintInfo;
use std::collections::HashMap;
use std::fmt;

/// Downstream input position selected by a logical feed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FeedRole {
    Input,
    Reference,
    Stream,
}

impl FeedRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Input => "input",
            Self::Reference => "reference",
            Self::Stream => "stream",
        }
    }
}

impl fmt::Display for FeedRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Whether an output contract member is selected by at least one downstream
/// feed in the current topology.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FactVisibility {
    Routable,
    Unrouted,
}

/// Runtime description of one output-contract member.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PayloadTypeDescriptor {
    pub type_hint: TypeHintInfo,
    pub event_type: Option<String>,
    pub schema_version: Option<u32>,
    pub visibility: FactVisibility,
}

impl PayloadTypeDescriptor {
    pub fn from_type_hint(type_hint: TypeHintInfo, visibility: FactVisibility) -> Self {
        Self {
            type_hint,
            event_type: None,
            schema_version: None,
            visibility,
        }
    }

    pub fn payload_key(&self) -> String {
        self.event_type
            .clone()
            .unwrap_or_else(|| payload_key_from_type_hint(&self.type_hint))
    }

    pub fn as_routable(mut self) -> Self {
        self.visibility = FactVisibility::Routable;
        self
    }
}

/// Stage output contract known to the runtime.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StageOutputContract {
    pub outputs: Vec<PayloadTypeDescriptor>,
}

impl StageOutputContract {
    pub fn empty() -> Self {
        Self {
            outputs: Vec::new(),
        }
    }

    pub fn single(output: PayloadTypeDescriptor) -> Self {
        Self {
            outputs: vec![output],
        }
    }

    pub fn mark_routable(&mut self, payload_key: &str) {
        if let Some(output) = self
            .outputs
            .iter_mut()
            .find(|output| output.payload_key() == payload_key)
        {
            output.visibility = FactVisibility::Routable;
        }
    }

    pub fn output_by_key(&self, payload_key: &str) -> Option<&PayloadTypeDescriptor> {
        self.outputs
            .iter()
            .find(|output| output.payload_key() == payload_key)
    }

    pub fn is_empty(&self) -> bool {
        self.outputs.is_empty()
    }
}

/// Stable key for one logical feed between two stages.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct FeedKey {
    pub upstream_stage: StageId,
    pub downstream_stage: StageId,
    pub selected_payload_key: String,
    pub role: FeedRole,
}

impl FeedKey {
    pub fn new(
        upstream_stage: StageId,
        downstream_stage: StageId,
        selected_payload_key: impl Into<String>,
        role: FeedRole,
    ) -> Self {
        Self {
            upstream_stage,
            downstream_stage,
            selected_payload_key: selected_payload_key.into(),
            role,
        }
    }

    /// Fallback key for legacy callers that have only stage-pair contract
    /// evidence. DSL-built flows should use real selected payload keys.
    pub fn legacy_stage_pair(upstream_stage: StageId, downstream_stage: StageId) -> Self {
        Self::new(
            upstream_stage,
            downstream_stage,
            payload_key_from_type_hint(&TypeHintInfo::Unspecified),
            FeedRole::Input,
        )
    }

    pub fn matches_stage_pair(&self, upstream_stage: StageId, downstream_stage: StageId) -> bool {
        self.upstream_stage == upstream_stage && self.downstream_stage == downstream_stage
    }
}

/// One selected payload type flowing across a concrete edge.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalFeed {
    pub key: FeedKey,
    pub selected_payload: PayloadTypeDescriptor,
}

/// Flow-scoped output contracts and logical feeds.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FeedPlan {
    pub stage_output_contracts: HashMap<StageId, StageOutputContract>,
    pub feeds: Vec<LogicalFeed>,
}

impl FeedPlan {
    pub fn new(
        mut stage_output_contracts: HashMap<StageId, StageOutputContract>,
        mut feeds: Vec<LogicalFeed>,
    ) -> Self {
        feeds.sort_by(|left, right| {
            left.key
                .upstream_stage
                .cmp(&right.key.upstream_stage)
                .then_with(|| left.key.downstream_stage.cmp(&right.key.downstream_stage))
                .then_with(|| left.key.role.as_str().cmp(right.key.role.as_str()))
                .then_with(|| {
                    left.key
                        .selected_payload_key
                        .cmp(&right.key.selected_payload_key)
                })
        });

        for feed in &feeds {
            if let Some(contract) = stage_output_contracts.get_mut(&feed.key.upstream_stage) {
                contract.mark_routable(&feed.key.selected_payload_key);
            }
        }

        Self {
            stage_output_contracts,
            feeds,
        }
    }

    pub fn output_contract(&self, stage_id: StageId) -> Option<&StageOutputContract> {
        self.stage_output_contracts.get(&stage_id)
    }

    pub fn input_feeds(&self, downstream_stage: StageId) -> Vec<LogicalFeed> {
        self.feeds
            .iter()
            .filter(|feed| feed.key.downstream_stage == downstream_stage)
            .cloned()
            .collect()
    }

    pub fn all_feeds(&self) -> &[LogicalFeed] {
        &self.feeds
    }
}

pub fn payload_key_from_type_hint(type_hint: &TypeHintInfo) -> String {
    match type_hint {
        TypeHintInfo::Exact { name } => format!("exact:{name}"),
        TypeHintInfo::Mixed => "mixed".to_string(),
        TypeHintInfo::Unspecified => "unspecified".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feed_plan_marks_selected_output_as_routable() {
        let upstream = StageId::new();
        let downstream = StageId::new();
        let type_hint = TypeHintInfo::exact("crate::Fact");
        let selected_payload_key = payload_key_from_type_hint(&type_hint);
        let mut contracts = HashMap::new();
        contracts.insert(
            upstream,
            StageOutputContract::single(PayloadTypeDescriptor::from_type_hint(
                type_hint.clone(),
                FactVisibility::Unrouted,
            )),
        );

        let plan = FeedPlan::new(
            contracts,
            vec![LogicalFeed {
                key: FeedKey {
                    upstream_stage: upstream,
                    downstream_stage: downstream,
                    selected_payload_key: selected_payload_key.clone(),
                    role: FeedRole::Input,
                },
                selected_payload: PayloadTypeDescriptor::from_type_hint(
                    type_hint,
                    FactVisibility::Routable,
                ),
            }],
        );

        let output = plan
            .output_contract(upstream)
            .and_then(|contract| contract.output_by_key(&selected_payload_key))
            .expect("selected output contract member");
        assert_eq!(output.visibility, FactVisibility::Routable);
        assert_eq!(plan.input_feeds(downstream).len(), 1);
    }
}
