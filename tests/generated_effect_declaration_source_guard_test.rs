// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

const STAGE_MACROS: &str = include_str!("../crates/obzenflow_dsl/src/dsl/stage_macros.rs");
const INFERENCE: &str = include_str!("../crates/obzenflow_dsl/src/dsl/inference.rs");
const AI_MAP_REDUCE: &str =
    include_str!("../crates/obzenflow_dsl/src/dsl/composites/ai_map_reduce.rs");

#[test]
fn generated_ai_uses_clauses_use_the_canonical_effect_entry_lowerer() {
    assert!(STAGE_MACROS.contains("@generated_chat surface"));
    assert!(STAGE_MACROS.contains("@entry __chat_declarations"));
    assert!(!STAGE_MACROS.contains("macro_rules! __obzenflow_ai_chat_effect_row"));
    assert!(!STAGE_MACROS.contains("macro_rules! __obzenflow_ai_effect_row_syntax_then"));

    for generated_module in [INFERENCE, AI_MAP_REDUCE] {
        assert!(!generated_module.contains("EffectPolicyAttachment {"));
        assert!(!generated_module.contains("EffectDeclaration::named_at_least_once"));
    }
}
