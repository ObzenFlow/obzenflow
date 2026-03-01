// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;

#[test]
fn is_gating_edge_for_contract_behaves_as_expected() {
    // Non-source edges are always gating, regardless of mode.
    assert!(is_gating_edge_for_contract(
        false,
        SourceContractStrictMode::Abort
    ));
    assert!(is_gating_edge_for_contract(
        false,
        SourceContractStrictMode::Warn
    ));

    // Source edges are gating only when strict mode is Abort.
    assert!(is_gating_edge_for_contract(
        true,
        SourceContractStrictMode::Abort
    ));
    assert!(
        !is_gating_edge_for_contract(true, SourceContractStrictMode::Warn),
        "source edges should be non-gating when strict mode is Warn"
    );
}
