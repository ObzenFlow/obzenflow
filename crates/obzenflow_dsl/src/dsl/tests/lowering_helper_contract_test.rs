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
    use obzenflow_core::TypedPayload;
    use obzenflow_runtime::effects::{Effect, EffectContext, EffectError, EffectSafety, EffectSet};
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct Out;
    impl TypedPayload for Out {
        const EVENT_TYPE: &'static str = "test.lowering_contract.out";
        const SCHEMA_VERSION: u32 = 1;
    }

    #[derive(Clone, Debug)]
    struct TxEffect;

    #[async_trait]
    impl Effect for TxEffect {
        const EFFECT_TYPE: &'static str = "test.lowering_contract.tx_effect";
        const SCHEMA_VERSION: u32 = 1;
        const SAFETY: EffectSafety = EffectSafety::Transactional;

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

    /// FLOWIP-120z: the type-level manifest muncher and the value-level entry
    /// muncher must agree for every effect-declaration spelling.
    #[test]
    fn effect_manifest_type_muncher_matches_every_declaration_spelling() {
        type Plain = crate::__obzenflow_effect_manifest_types!(TxEffect);
        let mut plain = Vec::new();
        let _plain_attachments: Vec<crate::dsl::stage_descriptor::EffectPolicyAttachment> =
            Vec::new();
        crate::__obzenflow_effect_entries!(@entry plain, _plain_attachments, [], TxEffect);
        assert_eq!(
            <Plain as EffectSet>::effect_types(),
            plain
                .iter()
                .map(|entry| entry.effect_type)
                .collect::<Vec<_>>()
        );

        type WithPolicy = crate::__obzenflow_effect_manifest_types!(
            TxEffect with [obzenflow_adapters::middleware::RateLimiterBuilder::new(2.0).build()]
        );
        let mut with_policy = Vec::new();
        let mut with_policy_attachments = Vec::new();
        crate::__obzenflow_effect_entries!(
            @entry with_policy,
            with_policy_attachments,
            [],
            TxEffect with [obzenflow_adapters::middleware::RateLimiterBuilder::new(2.0).build()]
        );
        assert_eq!(
            <WithPolicy as EffectSet>::effect_types(),
            with_policy
                .iter()
                .map(|entry| entry.effect_type)
                .collect::<Vec<_>>()
        );
        assert_eq!(with_policy_attachments.len(), 1);

        type Transactional =
            crate::__obzenflow_effect_manifest_types!(transactional(TxEffect, "tx"));
        let mut transactional = Vec::new();
        let transactional_attachments: Vec<crate::dsl::stage_descriptor::EffectPolicyAttachment> =
            Vec::new();
        crate::__obzenflow_effect_entries!(
            @entry transactional,
            transactional_attachments,
            [],
            transactional(TxEffect, "tx")
        );
        assert_eq!(
            <Transactional as EffectSet>::effect_types(),
            transactional
                .iter()
                .map(|entry| entry.effect_type)
                .collect::<Vec<_>>()
        );
        assert!(transactional_attachments.is_empty());
    }
}
