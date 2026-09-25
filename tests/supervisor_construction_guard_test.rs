// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Protect the runtime's typed supervisor construction boundary (FLOWIP-122e A1).

#[test]
fn raw_supervisor_spawning_remains_private() {
    // An external compile-fail check cannot distinguish private from pub(crate).
    // Inspect the actual declaration so runtime modules cannot regain the bypass.
    let syntax = syn::parse_file(include_str!(
        "../crates/obzenflow_runtime/src/supervised_base/handle.rs"
    ))
    .expect("parse supervisor task construction");
    let methods: Vec<_> = syntax
        .items
        .iter()
        .filter_map(|item| match item {
            syn::Item::Impl(implementation)
                if matches!(implementation.self_ty.as_ref(), syn::Type::Path(path)
                    if path.path.segments.last().is_some_and(|segment|
                        segment.ident == "SupervisorTaskBuilder")) =>
            {
                Some(implementation)
            }
            _ => None,
        })
        .flat_map(|implementation| &implementation.items)
        .filter_map(|item| match item {
            syn::ImplItem::Fn(method) if method.sig.ident == "spawn" => Some(method),
            _ => None,
        })
        .collect();
    assert_eq!(
        methods.len(),
        1,
        "locate the raw supervisor task constructor"
    );
    assert!(
        matches!(methods[0].vis, syn::Visibility::Inherited),
        "raw spawning must stay private behind typed supervisor construction"
    );
}

#[test]
fn borrowed_handler_runner_stays_inside_supervised_base() {
    let syntax = syn::parse_file(include_str!(
        "../crates/obzenflow_runtime/src/supervised_base/handler_supervised.rs"
    ))
    .expect("parse handler supervision");
    let runner = syntax
        .items
        .iter()
        .find_map(|item| match item {
            syn::Item::Fn(function) if function.sig.ident == "run_handler_supervised" => {
                Some(function)
            }
            _ => None,
        })
        .expect("locate the borrowed shared runner");
    assert!(
        matches!(&runner.vis, syn::Visibility::Restricted(visibility)
            if visibility.path.is_ident("super")),
        "borrowed execution must remain inside supervised_base's typed construction boundary"
    );
}
