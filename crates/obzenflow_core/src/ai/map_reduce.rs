// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{AiUnavailable, GroupClosedBy, GroupOrdinal, ReduceTrigger, WindowOrdinal};
use crate::event::payloads::composite_data_payload::CompositeDataPayload;
use crate::event::status::processing_status::ErrorKind;
use crate::event::ChainPayload;
use crate::TypedPayload;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;

macro_rules! composite_data_payload {
    ($ty:ty, [$($generic:ident),*], $event_type:literal, $version:literal, $variant:ident) => {
        impl<$($generic),*> TypedPayload for $ty
        where
            $($generic: Serialize + DeserializeOwned,)*
        {
            const EVENT_TYPE: &'static str = $event_type;
            const SCHEMA_VERSION: u32 = $version;

            const EVENT_KIND: crate::event::payloads::chain_payload::EventKind =
                crate::event::payloads::chain_payload::EventKind::CompositeData;

            fn into_chain_payload(self) -> Result<ChainPayload, serde_json::Error> {
                CompositeDataPayload::decode(&Self::event_type_name(), serde_json::to_value(self)?)
                    .map(ChainPayload::CompositeData)
            }

            fn accepts_payload(payload: &ChainPayload) -> bool {
                matches!(
                    payload,
                    ChainPayload::CompositeData(CompositeDataPayload::$variant(_))
                )
            }
        }
    };
}

/// One closed group, the map member's input.
#[doc(hidden)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AiMapReduceMapInput<T> {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<Value>,
    pub group: GroupOrdinal,
    /// Ordinal of the group's first item among its key's items.
    pub first_item: u64,
    /// Citation number of the group's first item within its reduce window.
    pub first_number: u64,
    pub closed_by: GroupClosedBy,
    pub items: Vec<T>,
}

composite_data_payload!(
    AiMapReduceMapInput<T>,
    [T],
    "ai.map_reduce.map_input",
    2,
    MapInput
);

/// End of input for one key, carrying the key's group count.
#[doc(hidden)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AiMapReduceGroupingEnded {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<Value>,
    pub group_count: u64,
}

composite_data_payload!(
    AiMapReduceGroupingEnded,
    [],
    "ai.map_reduce.grouping_ended",
    1,
    GroupingEnded
);

/// A group's partial, carried with the group's items to the collector.
#[doc(hidden)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AiMapReduceTaggedPartial<T, P> {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<Value>,
    pub group: GroupOrdinal,
    pub items: Vec<T>,
    pub partial: P,
}

composite_data_payload!(
    AiMapReduceTaggedPartial<T, P>,
    [T, P],
    "ai.map_reduce.tagged_partial",
    2,
    TaggedPartial
);

/// Why a role's call produced no output.
#[doc(hidden)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "failure_type", rename_all = "snake_case")]
pub enum AiMapReduceRoleFailure {
    /// The role failed with its own error.
    Role { kind: ErrorKind, message: String },
    /// The call was unavailable and the role propagated it.
    Unavailable { unavailable: AiUnavailable },
    /// The role's request could not become an effect.
    Request { message: String },
}

/// A group whose map role failed.
#[doc(hidden)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AiMapReduceChunkFailed {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<Value>,
    pub group: GroupOrdinal,
    pub cause: AiMapReduceRoleFailure,
}

composite_data_payload!(
    AiMapReduceChunkFailed,
    [],
    "ai.map_reduce.chunk_failed",
    3,
    ChunkFailed
);

/// One closed reduce window, the reduce member's input.
#[doc(hidden)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AiMapReduceReduceInput<T, P> {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<Value>,
    pub window: WindowOrdinal,
    pub trigger: ReduceTrigger,
    pub items: Vec<T>,
    pub partials: Vec<P>,
}

composite_data_payload!(
    AiMapReduceReduceInput<T, P>,
    [T, P],
    "ai.map_reduce.reduce_input",
    3,
    ReduceInput
);

/// One failed group of a window.
#[doc(hidden)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AiMapReduceGroupFailure {
    pub group: GroupOrdinal,
    pub cause: AiMapReduceRoleFailure,
}

/// A window that closed with a failed group and never reduces.
#[doc(hidden)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AiMapReduceWindowFailed {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<Value>,
    pub window: WindowOrdinal,
    pub failures: Vec<AiMapReduceGroupFailure>,
}

composite_data_payload!(
    AiMapReduceWindowFailed,
    [],
    "ai.map_reduce.window_failed",
    1,
    WindowFailed
);

/// A window whose reduce role failed.
#[doc(hidden)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AiMapReduceReduceFailed {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<Value>,
    pub window: WindowOrdinal,
    pub cause: AiMapReduceRoleFailure,
}

composite_data_payload!(
    AiMapReduceReduceFailed,
    [],
    "ai.map_reduce.reduce_failed",
    1,
    ReduceFailed
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ai::{AiProviderFailureKind, AiUnavailableCause};
    use serde_json::json;

    #[test]
    fn role_failure_wire_shape_has_one_outer_tag() {
        let cases = [
            (
                AiMapReduceRoleFailure::Role {
                    kind: ErrorKind::Domain,
                    message: "no summary".to_string(),
                },
                json!({"failure_type": "role", "kind": "Domain", "message": "no summary"}),
            ),
            (
                AiMapReduceRoleFailure::Unavailable {
                    unavailable: AiUnavailable::new(
                        AiUnavailableCause::Provider {
                            failure: AiProviderFailureKind::Timeout,
                        },
                        "slow",
                    ),
                },
                json!({
                    "failure_type": "unavailable",
                    "unavailable": {
                        "cause": {"kind": "provider", "failure": "timeout"},
                        "reason": "slow"
                    }
                }),
            ),
            (
                AiMapReduceRoleFailure::Request {
                    message: "bad schema".to_string(),
                },
                json!({"failure_type": "request", "message": "bad schema"}),
            ),
        ];

        for (failure, expected) in cases {
            let value = serde_json::to_value(&failure).expect("role failure serializes");
            assert_eq!(value, expected);
            assert_eq!(
                serde_json::from_value::<AiMapReduceRoleFailure>(value).unwrap(),
                failure
            );
        }
    }

    #[test]
    fn an_unkeyed_group_omits_its_key() {
        let group = AiMapReduceMapInput {
            key: None,
            group: GroupOrdinal::new(2),
            first_item: 20,
            first_number: 21,
            closed_by: GroupClosedBy::ChunkSize,
            items: vec!["story"],
        };
        assert_eq!(
            serde_json::to_value(&group).unwrap(),
            json!({
                "group": 2,
                "first_item": 20,
                "first_number": 21,
                "closed_by": "chunk_size",
                "items": ["story"]
            })
        );
    }
}
