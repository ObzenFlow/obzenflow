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

use super::typed_fact_set::TypedFactSet;

/// Public name for an effect outcome carrier.
///
/// This is a supertrait alias over [`TypedFactSet`]: every fact set is an
/// outcome carrier, and the conversion methods (`fact_types`, `into_facts`,
/// `try_from_facts`, `synthesized_fact_types`) live on [`TypedFactSet`].
/// The `Effect::Outcome` bound and user-facing docs name this trait;
/// [`TypedFactSet`] remains the low-level marshalling bridge for non-effect
/// uses such as scalar stateful outputs.
///
/// A scalar `TypedPayload` is a valid carrier through the blanket
/// `TypedPayload -> TypedFactSet` implementation, so single-fact effects keep
/// their bare payload type as `Outcome`. A carrier type can never also
/// implement `TypedPayload`: the blanket implementations would conflict, which
/// is deliberate, because a carrier must never double as a persisted wrapper
/// event.
pub trait EffectOutcomeFacts: TypedFactSet {}

impl<T: TypedFactSet> EffectOutcomeFacts for T {}

/// Define an effect outcome carrier and generate its fact-set implementation
/// (FLOWIP-120m).
///
/// Wrap an enum for a closed sum outcome (one persisted fact per variant) or
/// a named-field struct for a genuine product outcome (one persisted fact per
/// field, recorded together). The macro emits the type definition verbatim
/// plus an exact, fail-closed `TypedFactSet` implementation, which the
/// blanket lift makes an `EffectOutcomeFacts` carrier usable as
/// `Effect::Outcome`.
///
/// ```
/// use obzenflow_core::{effect_outcome, TypedPayload};
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
/// effect_outcome! {
///     /// Closed set of successful gateway authorization outcomes.
///     #[derive(Debug, Clone)]
///     pub enum AuthorizePaymentOutcome {
///         Authorized(PaymentAuthorized),
///         Declined(PaymentDeclined),
///     }
/// }
/// ```
///
/// Reconstruction is exact: a recorded group containing a fact outside the
/// carrier's declared set fails with `TypedFactSetError::UnexpectedFact`,
/// sum groups must hold exactly one fact matching exactly one variant, and
/// product groups must hold exactly one fact per field. Each variant or
/// field holds exactly one `TypedPayload`; nested carriers, multi-field
/// variants, tuple structs, and generics are rejected at compile time.
///
/// A carrier must not also implement `TypedPayload`: the blanket
/// `TypedPayload -> TypedFactSet` implementation would conflict with the
/// generated one, which is deliberate, because a carrier is transient
/// control-flow machinery and never a persisted wrapper event. Two distinct
/// member types that collide on `EVENT_TYPE` cannot be seen by a macro;
/// flow build rejects them when declarations are collected.
#[macro_export]
macro_rules! effect_outcome {
    // ── Sum carrier: enum, exactly one TypedPayload per tuple variant ──
    (
        $(#[$meta:meta])*
        $vis:vis enum $name:ident {
            $( $(#[$variant_meta:meta])* $variant:ident($member:ty) ),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        $vis enum $name {
            $( $(#[$variant_meta])* $variant($member), )+
        }

        impl $crate::event::schema::TypedFactSet for $name {
            fn fact_types() -> ::std::vec::Vec<$crate::event::schema::TypedFactType> {
                ::std::vec![ $( $crate::event::schema::TypedFactType::of::<$member>() ),+ ]
            }

            fn into_facts(
                self,
            ) -> ::std::result::Result<
                ::std::vec::Vec<$crate::event::schema::TypedFact>,
                $crate::event::schema::TypedFactSetError,
            > {
                match self {
                    $( Self::$variant(member) => ::std::result::Result::Ok(::std::vec![
                        $crate::event::schema::TypedFact::from_payload(member)?,
                    ]), )+
                }
            }

            fn try_from_facts(
                facts: &[$crate::event::schema::TypedFact],
            ) -> ::std::result::Result<Self, $crate::event::schema::TypedFactSetError> {
                for fact in facts {
                    let declared = false
                        $( || <$member as $crate::event::schema::TypedPayload>::event_type_matches(
                            fact.event_type.as_str(),
                        ) )+;
                    if !declared {
                        return ::std::result::Result::Err(
                            $crate::event::schema::TypedFactSetError::UnexpectedFact {
                                event_type: fact.event_type.clone(),
                            },
                        );
                    }
                }
                match facts {
                    [single] => {
                        $(
                            if <$member as $crate::event::schema::TypedPayload>::event_type_matches(
                                single.event_type.as_str(),
                            ) {
                                return ::std::result::Result::Ok(Self::$variant(
                                    $crate::event::schema::decode_member_fact::<$member>(facts)?,
                                ));
                            }
                        )+
                        // Safe: the undeclared-fact scan above already
                        // rejected any fact no variant matches.
                        ::core::unreachable!(
                            "effect_outcome!: undeclared facts were rejected above"
                        )
                    }
                    [] => ::std::result::Result::Err(
                        $crate::event::schema::missing_fact_group_error(
                            &<Self as $crate::event::schema::TypedFactSet>::fact_types(),
                        ),
                    ),
                    [first, rest @ ..] => ::std::result::Result::Err(
                        $crate::event::schema::sum_group_arity_error(first, rest),
                    ),
                }
            }
        }
    };

    // ── Product carrier: struct, exactly one TypedPayload per named field ──
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident {
            $( $(#[$field_meta:meta])* $field_vis:vis $field:ident : $member:ty ),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        $vis struct $name {
            $( $(#[$field_meta])* $field_vis $field: $member, )+
        }

        impl $crate::event::schema::TypedFactSet for $name {
            fn fact_types() -> ::std::vec::Vec<$crate::event::schema::TypedFactType> {
                ::std::vec![ $( $crate::event::schema::TypedFactType::of::<$member>() ),+ ]
            }

            fn into_facts(
                self,
            ) -> ::std::result::Result<
                ::std::vec::Vec<$crate::event::schema::TypedFact>,
                $crate::event::schema::TypedFactSetError,
            > {
                // Field order is the committed fact order; the committer's
                // outcome_fact_ordinal preserves it deterministically.
                ::std::result::Result::Ok(::std::vec![
                    $( $crate::event::schema::TypedFact::from_payload(self.$field)?, )+
                ])
            }

            fn try_from_facts(
                facts: &[$crate::event::schema::TypedFact],
            ) -> ::std::result::Result<Self, $crate::event::schema::TypedFactSetError> {
                for fact in facts {
                    let declared = false
                        $( || <$member as $crate::event::schema::TypedPayload>::event_type_matches(
                            fact.event_type.as_str(),
                        ) )+;
                    if !declared {
                        return ::std::result::Result::Err(
                            $crate::event::schema::TypedFactSetError::UnexpectedFact {
                                event_type: fact.event_type.clone(),
                            },
                        );
                    }
                }
                // Multiset equality: the scan above rejects foreign facts,
                // and each member decode requires exactly one fact of its
                // type (MissingFact / DuplicateFact otherwise).
                ::std::result::Result::Ok(Self {
                    $( $field: $crate::event::schema::decode_member_fact::<$member>(facts)?, )+
                })
            }
        }
    };

    // ── Diagnostics: invalid shapes fail at compile time, in arm order ──
    ( $(#[$meta:meta])* $vis:vis enum $name:ident { $($body:tt)* } ) => {
        ::core::compile_error!(
            "effect_outcome! enum variants must each hold exactly one TypedPayload fact, \
             e.g. `Authorized(PaymentAuthorized)`; unit, struct-like, multi-field, and \
             empty enums are not valid carriers (FLOWIP-120m)"
        );
    };
    ( $(#[$meta:meta])* $vis:vis struct $name:ident ( $($body:tt)* ) $(;)? ) => {
        ::core::compile_error!(
            "effect_outcome! struct carriers use named fields, one TypedPayload fact per \
             field; tuple structs are not valid carriers (FLOWIP-120m)"
        );
    };
    ( $($tokens:tt)* ) => {
        ::core::compile_error!(
            "effect_outcome! accepts exactly one enum or struct carrier definition; \
             attributes, doc comments, and a visibility qualifier may precede it \
             (FLOWIP-120m)"
        );
    };
}

#[cfg(test)]
mod tests {
    use crate::event::schema::{TypedFact, TypedFactSet, TypedFactSetError, TypedPayload};
    use crate::event::types::EventType;
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

    effect_outcome! {
        /// Doc comments and derives pass through.
        #[derive(Debug, Clone, PartialEq, Eq)]
        pub enum DecisionOutcome {
            Approved(Approved),
            Refused(Refused),
        }
    }

    effect_outcome! {
        #[derive(Debug, Clone, PartialEq, Eq)]
        pub struct DecisionWithAudit {
            pub decision: Approved,
            pub audit: Refused,
        }
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
    fn effect_outcome_carriers_report_empty_synthesized_fact_types() {
        // Load-bearing for guarded coordination (FLOWIP-120h): plain carriers
        // must keep the default empty synthesized set.
        assert!(DecisionOutcome::synthesized_fact_types().is_empty());
        assert!(DecisionWithAudit::synthesized_fact_types().is_empty());
    }
}
