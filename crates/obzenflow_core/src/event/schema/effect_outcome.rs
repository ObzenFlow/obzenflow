// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Effect outcome carriers (FLOWIP-120m).
//!
//! An effect's outcome is a closed set of named facts. The transient value an
//! effect returns through `fx.perform` is an outcome carrier: an
//! effect-specific enum for sum outcomes or a named struct for genuine product
//! outcomes, lowered to flat named `Data` facts for the journal and
//! reconstructed from the recorded fact group on replay. The carrier is
//! control-flow machinery only; it is never a persisted wrapper event and
//! never the stage output contract.

use super::stage_fact_set::StageFactSet;
use super::typed_fact_set::TypedFactSet;

/// Public name for an effect outcome carrier.
///
/// This is a supertrait alias over [`TypedFactSet`] and [`StageFactSet`]: every
/// fact set is an outcome carrier, its conversion methods (`fact_types`,
/// `into_facts`, `try_from_facts`, `synthesized_fact_types`) live on
/// [`TypedFactSet`], and its declared member contract lives on
/// [`StageFactSet`].
/// The `Effect::Outcome` bound and user-facing docs name this trait;
/// [`TypedFactSet`] remains the low-level marshalling bridge for non-effect
/// uses such as scalar stateful outputs.
///
/// Define a carrier with `#[derive(EffectOutcomeFacts)]` on an enum (closed
/// sum, one persisted fact per variant) or a named-field struct (product, one
/// fact per field):
///
/// ```
/// use obzenflow_core::{EffectOutcomeFacts, TypedPayload};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// pub struct PaymentAuthorized { pub authorization_id: String }
/// impl TypedPayload for PaymentAuthorized {
///     const EVENT_TYPE: &'static str = "payment.authorized";
/// }
///
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// pub struct PaymentDeclined { pub reason: String }
/// impl TypedPayload for PaymentDeclined {
///     const EVENT_TYPE: &'static str = "payment.declined";
/// }
///
/// /// Closed set of successful gateway authorization outcomes. The variants
/// /// are the facts the journal records; the derive writes the marshalling.
/// #[derive(Debug, Clone, EffectOutcomeFacts)]
/// pub enum AuthorizePaymentOutcome {
///     Authorized(PaymentAuthorized),
///     Declined(PaymentDeclined),
/// }
/// ```
///
/// A scalar `TypedPayload` is a valid carrier through the blanket
/// `TypedPayload -> TypedFactSet` implementation, so single-fact effects keep
/// their bare payload type as `Outcome`. A derived carrier can never also
/// implement `TypedPayload`: the blanket implementations would conflict, which
/// is deliberate, because a carrier must never double as a persisted wrapper
/// event.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not an effect outcome carrier",
    note = "an effect outcome is a closed set of named facts: add \
            #[derive(EffectOutcomeFacts)] so each variant or field lowers to its persisted \
            fact (FLOWIP-120m)"
)]
pub trait EffectOutcomeFacts: TypedFactSet + StageFactSet {}

impl<T: TypedFactSet + StageFactSet> EffectOutcomeFacts for T {}

#[cfg(test)]
mod tests {
    use crate::event::schema::{EffectOutcomeFacts, TypedFact, TypedFactSet, TypedFactSetError};
    use crate::event::types::EventType;
    use crate::TypedPayload;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct Approved {
        value: u32,
    }

    impl TypedPayload for Approved {
        const EVENT_TYPE: &'static str = "carrier.approved";
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct Refused {
        reason: String,
    }

    impl TypedPayload for Refused {
        const EVENT_TYPE: &'static str = "carrier.refused";
    }

    /// Doc comments and derives pass through; the carrier is plain source.
    #[derive(Debug, Clone, PartialEq, Eq, EffectOutcomeFacts)]
    pub enum DecisionOutcome {
        Approved(Approved),
        Refused(Refused),
    }

    #[derive(Debug, Clone, PartialEq, Eq, EffectOutcomeFacts)]
    pub struct DecisionWithAudit {
        pub decision: Approved,
        pub audit: Refused,
    }

    /// The path escape hatch (FLOWIP-120m): generated code resolves through
    /// the named path instead of the extern-prelude `::obzenflow_core`. The
    /// self alias makes the unprefixed name stand in for a re-export path.
    #[derive(Debug, Clone, PartialEq, Eq, EffectOutcomeFacts)]
    #[effect_outcome(crate = obzenflow_core)]
    pub enum HatchOutcome {
        Approved(Approved),
        Refused(Refused),
    }

    fn unknown_fact() -> TypedFact {
        TypedFact {
            event_type: EventType::from("carrier.unknown.v1"),
            payload: serde_json::json!({}),
        }
    }

    #[test]
    fn effect_outcome_enum_round_trips_each_variant() {
        for carrier in [
            DecisionOutcome::Approved(Approved { value: 4 }),
            DecisionOutcome::Refused(Refused {
                reason: "limit".to_string(),
            }),
        ] {
            let facts = carrier.clone().into_facts().expect("variant serializes");
            assert_eq!(facts.len(), 1);
            let reconstructed =
                DecisionOutcome::try_from_facts(&facts).expect("variant reconstructs");
            assert_eq!(reconstructed, carrier);
        }
    }

    #[test]
    fn effect_outcome_enum_fails_closed_on_bad_groups() {
        let empty: Vec<TypedFact> = Vec::new();
        assert!(matches!(
            DecisionOutcome::try_from_facts(&empty),
            Err(TypedFactSetError::MissingFact { .. })
        ));

        let duplicate = vec![
            TypedFact::from_payload(Approved { value: 1 }).expect("approved"),
            TypedFact::from_payload(Approved { value: 2 }).expect("approved again"),
        ];
        assert!(matches!(
            DecisionOutcome::try_from_facts(&duplicate),
            Err(TypedFactSetError::DuplicateFact { .. })
        ));

        let mixed = vec![
            TypedFact::from_payload(Approved { value: 1 }).expect("approved"),
            TypedFact::from_payload(Refused {
                reason: "limit".to_string(),
            })
            .expect("refused"),
        ];
        assert!(matches!(
            DecisionOutcome::try_from_facts(&mixed),
            Err(TypedFactSetError::UnexpectedFact { .. })
        ));

        let unknown = vec![unknown_fact()];
        assert!(matches!(
            DecisionOutcome::try_from_facts(&unknown),
            Err(TypedFactSetError::UnexpectedFact { .. })
        ));
    }

    #[test]
    fn effect_outcome_struct_round_trips_in_field_order() {
        let carrier = DecisionWithAudit {
            decision: Approved { value: 7 },
            audit: Refused {
                reason: "recorded".to_string(),
            },
        };

        let facts = carrier.clone().into_facts().expect("product serializes");
        assert_eq!(facts.len(), 2);
        assert_eq!(facts[0].event_type.as_str(), "carrier.approved.v1");
        assert_eq!(facts[1].event_type.as_str(), "carrier.refused.v1");

        // Reconstruction is order-insensitive; the ordinal regime owns order.
        let reversed = vec![facts[1].clone(), facts[0].clone()];
        assert_eq!(
            DecisionWithAudit::try_from_facts(&reversed).expect("product reconstructs"),
            carrier
        );
    }

    #[test]
    fn effect_outcome_struct_rejects_missing_duplicate_and_undeclared() {
        let missing = vec![TypedFact::from_payload(Approved { value: 7 }).expect("approved")];
        assert!(matches!(
            DecisionWithAudit::try_from_facts(&missing),
            Err(TypedFactSetError::MissingFact { .. })
        ));

        let duplicated = vec![
            TypedFact::from_payload(Approved { value: 7 }).expect("approved"),
            TypedFact::from_payload(Approved { value: 8 }).expect("approved again"),
            TypedFact::from_payload(Refused {
                reason: "recorded".to_string(),
            })
            .expect("refused"),
        ];
        assert!(matches!(
            DecisionWithAudit::try_from_facts(&duplicated),
            Err(TypedFactSetError::DuplicateFact { .. })
        ));

        let with_unknown = vec![
            TypedFact::from_payload(Approved { value: 7 }).expect("approved"),
            TypedFact::from_payload(Refused {
                reason: "recorded".to_string(),
            })
            .expect("refused"),
            unknown_fact(),
        ];
        assert!(matches!(
            DecisionWithAudit::try_from_facts(&with_unknown),
            Err(TypedFactSetError::UnexpectedFact { .. })
        ));
    }

    #[test]
    fn effect_outcome_crate_path_hatch_substitutes_generated_paths() {
        let carrier = HatchOutcome::Approved(Approved { value: 11 });
        let facts = carrier.clone().into_facts().expect("variant serializes");
        assert_eq!(
            HatchOutcome::try_from_facts(&facts).expect("variant reconstructs"),
            carrier
        );
    }

    #[test]
    fn effect_outcome_carriers_report_empty_synthesized_fact_types() {
        // Load-bearing for guarded coordination (FLOWIP-120h): plain carriers
        // must keep the default empty synthesized set.
        assert!(DecisionOutcome::synthesized_fact_types().is_empty());
        assert!(DecisionWithAudit::synthesized_fact_types().is_empty());
    }
}
