// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[test]
fn ordinary_observers_have_no_execution_or_publication_authority() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/observer_cannot_mutate_outputs.rs");
    t.compile_fail("tests/compile_fail/observer_cannot_return_report.rs");
    t.compile_fail("tests/compile_fail/observer_cannot_publish_framework_event.rs");
    t.compile_fail("tests/compile_fail/observer_context_constructor_is_private.rs");
    t.compile_fail("tests/compile_fail/output_commit_observer_is_not_public.rs");
    t.compile_fail("tests/compile_fail/observer_bundle_builder_is_not_public.rs");
    t.compile_fail("tests/compile_fail/observer_bundle_cannot_enter_config.rs");
}
