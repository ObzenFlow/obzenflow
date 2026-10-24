// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

const STAGE_MACROS: &str = include_str!("../crates/obzenflow_dsl/src/dsl/stage_macros.rs");
const AI_STAGE: &str = include_str!("../crates/obzenflow_dsl/src/dsl/ai_stage.rs");

#[test]
fn ai_stages_declare_their_effect_only_through_the_configured_binding() {
    assert!(AI_STAGE.contains("EffectDeclaration::configured("));
    assert!(!AI_STAGE.contains("EffectPolicyAttachment {"));
    assert!(!AI_STAGE.contains("EffectDeclaration::named_at_least_once"));
    assert!(!STAGE_MACROS.contains("macro_rules! __obzenflow_effect_entries"));
    assert!(!STAGE_MACROS.contains("@generated_chat surface"));
}
