// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Stage output carriers (FLOWIP-120z).
//!
//! A pure typed transform returns its real domain outcome as a carrier value
//! that lowers to flat named facts; an effectful stateful handler folds one
//! committed fact per `apply` call. Both facades sit over the shared
//! [`TypedFactSet`] marshalling and [`StageFactSet`] membership machinery,
//! exactly as `EffectOutcomeFacts` does; there is no parallel marshalling
//! stack, and a carrier is never a persisted wrapper event.

use super::stage_fact_set::StageFactSet;
use super::typed_fact_set::TypedFactSet;
use super::typed_payload::TypedPayload;

/// Inhabited stage output carrier: a fact set that also lowers to facts.
///
/// Define one with `#[derive(StageOutputFacts)]` on an enum whose variants
/// are single facts, products of facts (named fields, field order is commit
/// order), or an explicit `#[stage_output(empty)]` filter arm; a scalar
/// `TypedPayload` is a one-member carrier through the blanket. A derived
/// carrier can never also implement `TypedPayload`: the blanket
/// implementations would conflict, which is deliberate, because a carrier
/// must never double as a persisted wrapper event.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a stage output carrier",
    note = "a typed stage output is a closed set of named facts: use \
            #[derive(StageOutputFacts)] on a sum-of-products enum or a named-field \
            struct, or use a single `TypedPayload` fact type directly (FLOWIP-120z)"
)]
pub trait StageOutputFacts: TypedFactSet + StageFactSet {}

impl<T: TypedFactSet + StageFactSet> StageOutputFacts for T {}

/// Sum-only refinement of [`StageOutputFacts`]: every value lowers to
/// exactly one committed fact.
///
/// The effectful stateful `Output` bound: `apply` folds one committed fact
/// per call, so a product carrier (which commits several facts at once)
/// must fail this bound at compile time rather than failing
/// `try_from_facts` at runtime. Deliberately not a blanket over all fact
/// sets; the derive implements it for per-fact sums, and scalars qualify
/// through this blanket.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be an effectful stateful `Output`: it does not commit \
               exactly one fact per value",
    note = "`apply` folds one committed fact per call, so the stateful `Output` must be \
            a per-fact sum (an enum with one fact per variant) or a single \
            `TypedPayload`; a product carrier commits several facts at once and cannot \
            fold one at a time (FLOWIP-120z)"
)]
pub trait OneFactStageOutput: StageOutputFacts {}

impl<T: TypedPayload + Send + Sync + 'static> OneFactStageOutput for T {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::schema::{StageOutputFacts, TypedFact, TypedFactSetError};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Solo {
        value: u32,
    }

    impl TypedPayload for Solo {
        const EVENT_TYPE: &'static str = "stage_output.solo";
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Valid {
        value: u32,
    }

    impl TypedPayload for Valid {
        const EVENT_TYPE: &'static str = "stage_output.valid";
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Invalid {
        reason: String,
    }

    impl TypedPayload for Invalid {
        const EVENT_TYPE: &'static str = "stage_output.invalid";
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Cancelled {
        reason: String,
    }

    impl TypedPayload for Cancelled {
        const EVENT_TYPE: &'static str = "stage_output.cancelled";
    }

    /// The FLOWIP-120z sum-of-products shape: one leaf on the valid path,
    /// a product on the invalid path, and a deliberate filter arm.
    #[derive(Debug, Clone, PartialEq, Eq, StageOutputFacts)]
    enum ClassificationOutcome {
        Validated(Valid),
        Rejected {
            invalid: Invalid,
            cancelled: Cancelled,
        },
        #[stage_output(empty)]
        Skipped,
    }

    /// Per-fact sum: qualifies as `OneFactStageOutput` via the derive.
    #[derive(Debug, Clone, PartialEq, Eq, StageOutputFacts)]
    enum PerFactSum {
        Valid(Valid),
        Invalid(Invalid),
    }

    /// Structural sharing across variants is legitimate: `Cancelled`
    /// appears in two variants and the member set deduplicates it.
    #[derive(Debug, Clone, PartialEq, Eq, StageOutputFacts)]
    enum SharedLeafOutcome {
        InvalidPath {
            invalid: Invalid,
            cancelled: Cancelled,
        },
        ValidThenCancelled {
            valid: Valid,
            cancelled: Cancelled,
        },
    }

    fn is_stage_output<T: StageOutputFacts>() {}
    fn is_one_fact_output<T: OneFactStageOutput>() {}

    #[test]
    fn scalar_payload_is_a_one_fact_stage_output() {
        is_stage_output::<Solo>();
        is_one_fact_output::<Solo>();
    }

    #[test]
    fn per_fact_sum_derive_emits_one_fact_stage_output() {
        is_stage_output::<PerFactSum>();
        is_one_fact_output::<PerFactSum>();
        is_stage_output::<ClassificationOutcome>();
        // ClassificationOutcome has a product and an empty variant, so it
        // deliberately does NOT implement OneFactStageOutput.
    }

    #[test]
    fn sum_of_products_round_trips_each_variant() {
        for carrier in [
            ClassificationOutcome::Validated(Valid { value: 4 }),
            ClassificationOutcome::Rejected {
                invalid: Invalid {
                    reason: "zero".to_string(),
                },
                cancelled: Cancelled {
                    reason: "invalid".to_string(),
                },
            },
            ClassificationOutcome::Skipped,
        ] {
            let facts = carrier.clone().into_facts().expect("variant serializes");
            let reconstructed =
                ClassificationOutcome::try_from_facts(&facts).expect("variant reconstructs");
            assert_eq!(reconstructed, carrier);
        }
    }

    #[test]
    fn product_variant_lowers_leaves_in_field_order() {
        let carrier = ClassificationOutcome::Rejected {
            invalid: Invalid {
                reason: "zero".to_string(),
            },
            cancelled: Cancelled {
                reason: "invalid".to_string(),
            },
        };
        let facts = carrier.into_facts().expect("variant serializes");
        assert_eq!(facts.len(), 2);
        assert_eq!(facts[0].event_type.as_str(), "stage_output.invalid.v1");
        assert_eq!(facts[1].event_type.as_str(), "stage_output.cancelled.v1");
    }

    #[test]
    fn empty_variant_lowers_to_zero_facts() {
        let facts = ClassificationOutcome::Skipped
            .into_facts()
            .expect("empty variant serializes");
        assert!(facts.is_empty());
    }

    #[test]
    fn leaf_set_dispatch_is_order_insensitive() {
        let carrier = ClassificationOutcome::Rejected {
            invalid: Invalid {
                reason: "zero".to_string(),
            },
            cancelled: Cancelled {
                reason: "invalid".to_string(),
            },
        };
        let facts = carrier.clone().into_facts().expect("variant serializes");
        let reversed = vec![facts[1].clone(), facts[0].clone()];
        assert_eq!(
            ClassificationOutcome::try_from_facts(&reversed).expect("variant reconstructs"),
            carrier
        );
    }

    #[test]
    fn leaf_set_dispatch_selects_among_shared_leaves() {
        let invalid_path = SharedLeafOutcome::InvalidPath {
            invalid: Invalid {
                reason: "zero".to_string(),
            },
            cancelled: Cancelled {
                reason: "invalid".to_string(),
            },
        };
        let valid_path = SharedLeafOutcome::ValidThenCancelled {
            valid: Valid { value: 1 },
            cancelled: Cancelled {
                reason: "late".to_string(),
            },
        };
        for carrier in [invalid_path, valid_path] {
            let facts = carrier.clone().into_facts().expect("variant serializes");
            assert_eq!(
                SharedLeafOutcome::try_from_facts(&facts).expect("variant reconstructs"),
                carrier
            );
        }

        let members: Vec<String> = <SharedLeafOutcome as StageFactSet>::member_fact_types()
            .iter()
            .map(|t| t.event_type.to_string())
            .collect();
        assert_eq!(
            members,
            vec![
                "stage_output.invalid.v1",
                "stage_output.cancelled.v1",
                "stage_output.valid.v1"
            ],
            "the member set deduplicates shared leaves in first-occurrence order"
        );
    }

    #[test]
    fn sum_of_products_fails_closed_on_bad_groups() {
        let foreign = vec![TypedFact {
            event_type: crate::event::types::EventType::from("stage_output.unknown.v1"),
            payload: serde_json::json!({}),
        }];
        assert!(matches!(
            ClassificationOutcome::try_from_facts(&foreign),
            Err(TypedFactSetError::UnexpectedFact { .. })
        ));

        let duplicate = vec![
            TypedFact::from_payload(Valid { value: 1 }).expect("valid"),
            TypedFact::from_payload(Valid { value: 2 }).expect("valid again"),
        ];
        assert!(matches!(
            ClassificationOutcome::try_from_facts(&duplicate),
            Err(TypedFactSetError::DuplicateFact { .. })
        ));

        // A declared-but-mismatched combination matches no variant's set.
        let mismatched = vec![
            TypedFact::from_payload(Valid { value: 1 }).expect("valid"),
            TypedFact::from_payload(Invalid {
                reason: "zero".to_string(),
            })
            .expect("invalid"),
        ];
        assert!(matches!(
            ClassificationOutcome::try_from_facts(&mismatched),
            Err(TypedFactSetError::MissingFact { .. })
        ));

        // An empty group only matches an explicit empty variant.
        let empty: Vec<TypedFact> = Vec::new();
        assert!(matches!(
            PerFactSum::try_from_facts(&empty),
            Err(TypedFactSetError::MissingFact { .. })
        ));
    }
}
