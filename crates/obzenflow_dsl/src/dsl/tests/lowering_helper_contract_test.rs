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
        transactional_effect_port_slot, Effect, EffectBinding, EffectBindingEvidence,
        EffectBindingUse, EffectContext, EffectDeclaration, EffectError, EffectPortResolutionError,
        EffectPortSlotSet, EffectRegistrationBuilder, EffectSafety, EffectSet,
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

    #[derive(Clone, Debug)]
    struct PlainEffect;

    #[async_trait]
    impl Effect for PlainEffect {
        const EFFECT_TYPE: &'static str = "test.lowering_contract.plain_effect";
        const SCHEMA_VERSION: u32 = 1;
        const SAFETY: EffectSafety = EffectSafety::Idempotent;
        type BindingMode = obzenflow_runtime::effects::Portless;

        type Outcome = Out;
        type OutcomeSemantics = obzenflow_runtime::effects::DomainFacts;

        fn label(&self) -> &str {
            "plain_effect"
        }

        fn canonical_input(&self) -> serde_json::Value {
            serde_json::Value::Null
        }

        async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
            Ok(Out)
        }
    }

    #[derive(Clone, Debug)]
    struct AffineEffect;

    #[async_trait]
    impl Effect for AffineEffect {
        const EFFECT_TYPE: &'static str = "test.lowering_contract.affine_effect";
        const SCHEMA_VERSION: u32 = 1;
        const SAFETY: EffectSafety = EffectSafety::NonIdempotentAtLeastOnce;
        type BindingMode = obzenflow_runtime::effects::Portless;

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

    #[derive(Clone, Debug)]
    struct TxEffect {
        binding: EffectBindingUse<Self>,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TxEvidence;

    impl EffectBindingEvidence for TxEvidence {
        const SCHEMA_VERSION: u32 = 1;

        fn canonical_bytes(&self) -> BoundedBindingEvidence {
            BoundedBindingEvidence::try_new(b"tx-fixture".to_vec()).unwrap()
        }
    }

    #[async_trait]
    impl Effect for TxEffect {
        const EFFECT_TYPE: &'static str = "test.lowering_contract.tx_effect";
        const SCHEMA_VERSION: u32 = 1;
        const SAFETY: EffectSafety = EffectSafety::Transactional;
        type BindingMode = Named<TxEvidence>;

        type Outcome = Out;
        type OutcomeSemantics = obzenflow_runtime::effects::DomainFacts;

        fn label(&self) -> &str {
            "tx_effect"
        }

        fn canonical_input(&self) -> serde_json::Value {
            serde_json::Value::Null
        }

        async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
            Ok(Out)
        }
    }

    impl NamedEffect for TxEffect {
        type BindingEvidence = TxEvidence;

        fn binding_use(&self) -> &EffectBindingUse<Self> {
            &self.binding
        }

        fn required_slots() -> EffectPortSlotSet {
            EffectPortSlotSet::single(transactional_effect_port_slot::<Self>())
        }
    }

    fn tx_binding() -> EffectBinding<TxEffect> {
        EffectRegistrationBuilder::<TxEffect>::new(
            LogicalEffectBindingName::new("tx").unwrap(),
            TxEvidence,
        )
        .bind_deferred(
            transactional_effect_port_slot::<TxEffect>(),
            Arc::new(|| Err(EffectPortResolutionError::ClientConstructionFailed)),
        )
        .unwrap()
        .finish()
        .unwrap()
        .0
    }

    /// FLOWIP-120z: the type-level manifest muncher and the value-level entry
    /// muncher must agree for every effect-declaration spelling.
    #[test]
    fn effect_manifest_type_muncher_matches_every_declaration_spelling() {
        type Plain = crate::__obzenflow_effect_manifest_types!(PlainEffect);
        let mut plain = Vec::new();
        let _plain_attachments: Vec<crate::dsl::stage_descriptor::EffectPolicyAttachment> =
            Vec::new();
        crate::__obzenflow_effect_entries!(@entry plain, _plain_attachments, [], PlainEffect);
        assert_eq!(
            <Plain as EffectSet>::effect_types(),
            plain
                .iter()
                .map(EffectDeclaration::effect_type)
                .collect::<Vec<_>>()
        );

        type WithPolicy = crate::__obzenflow_effect_manifest_types!(
            PlainEffect with obzenflow_adapters::middleware::RateLimiterBuilder::new(2.0).build()
        );
        let mut with_policy = Vec::new();
        let mut with_policy_attachments = Vec::new();
        crate::__obzenflow_effect_entries!(
            @entry with_policy,
            with_policy_attachments,
            [],
            PlainEffect with obzenflow_adapters::middleware::RateLimiterBuilder::new(2.0).build()
        );
        assert_eq!(
            <WithPolicy as EffectSet>::effect_types(),
            with_policy
                .iter()
                .map(EffectDeclaration::effect_type)
                .collect::<Vec<_>>()
        );
        assert_eq!(with_policy_attachments.len(), 1);

        let tx = tx_binding();
        type Transactional =
            crate::__obzenflow_effect_manifest_types!(transactional(TxEffect) via tx);
        let mut transactional = Vec::new();
        let transactional_attachments: Vec<crate::dsl::stage_descriptor::EffectPolicyAttachment> =
            Vec::new();
        crate::__obzenflow_effect_entries!(
            @entry transactional,
            transactional_attachments,
            [],
            transactional(TxEffect) via tx
        );
        assert_eq!(
            <Transactional as EffectSet>::effect_types(),
            transactional
                .iter()
                .map(EffectDeclaration::effect_type)
                .collect::<Vec<_>>()
        );
        assert!(transactional_attachments.is_empty());

        type AtLeastOnce = crate::__obzenflow_effect_manifest_types!(at_least_once(AffineEffect));
        let mut at_least_once = Vec::new();
        let _at_least_once_attachments: Vec<crate::dsl::stage_descriptor::EffectPolicyAttachment> =
            Vec::new();
        crate::__obzenflow_effect_entries!(
            @entry at_least_once,
            _at_least_once_attachments,
            [],
            at_least_once(AffineEffect)
        );
        assert_eq!(
            <AtLeastOnce as EffectSet>::effect_types(),
            at_least_once
                .iter()
                .map(EffectDeclaration::effect_type)
                .collect::<Vec<_>>()
        );
        assert!(matches!(
            at_least_once[0].idempotency_key_policy(),
            obzenflow_runtime::effects::IdempotencyKeyPolicy::AtLeastOnceAcknowledged
        ));
    }
}
