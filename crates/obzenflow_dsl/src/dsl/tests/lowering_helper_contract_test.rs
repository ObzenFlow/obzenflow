// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Lowering-contract tests: the only test file permitted to invoke
//! `__obzenflow_*` helpers directly (FLOWIP-133a helper boundary; enforced by
//! `tests/handler_path_source_gate_test.rs`).
//!
//! Each test pins an internal contract between lowering helpers that the
//! public macro surface composes and therefore cannot observe independently.
//! Public-surface tests must not be added here.

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use obzenflow_core::{BoundedBindingEvidence, TypedPayload};
    use obzenflow_runtime::effects::{
        Effect, EffectBinding, EffectBindingEvidence, EffectBindingUse, EffectContext,
        EffectDeclaration, EffectError, EffectPortResolutionError, EffectPortSlot,
        EffectPortSlotSet, EffectRegistrationBuilder, EffectSafety, IdempotencyKeyPolicy,
        LogicalEffectBindingName, Named, NamedEffect,
    };
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct Out;
    impl TypedPayload for Out {
        const EVENT_TYPE: &'static str = "test.lowering_contract.out";
        const SCHEMA_VERSION: u32 = 1;
    }

    trait ReplyPort: Send + Sync {}

    const REPLY: EffectPortSlot<dyn ReplyPort> = EffectPortSlot::new("reply");

    #[derive(Clone, Debug)]
    struct AffineEffect {
        binding: EffectBindingUse<Self>,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct AffineEvidence;

    impl EffectBindingEvidence for AffineEvidence {
        const SCHEMA_VERSION: u32 = 1;

        fn canonical_bytes(&self) -> BoundedBindingEvidence {
            BoundedBindingEvidence::try_new(b"affine-fixture".to_vec()).unwrap()
        }
    }

    #[async_trait]
    impl Effect for AffineEffect {
        const EFFECT_TYPE: &'static str = "test.lowering_contract.affine_effect";
        const SCHEMA_VERSION: u32 = 1;
        const SAFETY: EffectSafety = EffectSafety::NonIdempotentAtLeastOnce;
        type BindingMode = Named<AffineEvidence>;

        type Outcome = Out;
        type OutcomeSemantics = obzenflow_runtime::effects::DomainFacts;

        fn label(&self) -> &str {
            "affine_effect"
        }

        fn canonical_input(&self) -> serde_json::Value {
            serde_json::Value::Null
        }

        async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
            Ok(Out)
        }
    }

    impl NamedEffect for AffineEffect {
        type BindingEvidence = AffineEvidence;

        fn binding_use(&self) -> &EffectBindingUse<Self> {
            &self.binding
        }

        fn required_slots() -> EffectPortSlotSet {
            EffectPortSlotSet::single(REPLY)
        }
    }

    fn affine_binding() -> EffectBinding<AffineEffect> {
        EffectRegistrationBuilder::<AffineEffect>::new(
            LogicalEffectBindingName::new("affine").unwrap(),
            AffineEvidence,
        )
        .bind_deferred(
            REPLY,
            Arc::new(|| Err(EffectPortResolutionError::ClientConstructionFailed)),
        )
        .unwrap()
        .finish()
        .unwrap()
    }

    #[test]
    fn generated_entry_declares_one_acknowledged_effect_and_attaches_its_policies() {
        let binding = affine_binding();
        let mut declarations: Vec<EffectDeclaration> = Vec::new();
        let mut attachments: Vec<crate::dsl::stage_descriptor::EffectPolicyAttachment> = Vec::new();
        crate::__obzenflow_effect_entries!(
            @entry declarations,
            attachments,
            [],
            at_least_once(AffineEffect)
                via binding
                with {
                    obzenflow_adapters::middleware::rate_limit(2.0),
                    obzenflow_adapters::middleware::circuit_breaker()
                }
        );

        assert_eq!(
            declarations
                .iter()
                .map(EffectDeclaration::effect_type)
                .collect::<Vec<_>>(),
            [AffineEffect::EFFECT_TYPE]
        );
        assert!(matches!(
            declarations[0].idempotency_key_policy(),
            IdempotencyKeyPolicy::AtLeastOnceAcknowledged
        ));
        assert_eq!(attachments.len(), 2);
        assert!(attachments
            .iter()
            .all(|attachment| attachment.effect_type == AffineEffect::EFFECT_TYPE));
    }
}
