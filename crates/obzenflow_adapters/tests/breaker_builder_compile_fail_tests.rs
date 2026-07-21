// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[test]
fn unchecked_breaker_builders_are_not_middleware_factories() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/breaker_builder/checked_builder_not_factory.rs");
    cases.compile_fail("tests/ui/breaker_builder/raw_breaker_core_private.rs");
    cases.compile_fail(
        "tests/ui/breaker_builder/materialization_context_hides_control_authority.rs",
    );
}
