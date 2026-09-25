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
fn handler_runner_remains_trait_owned_and_consuming() {
    let syntax = syn::parse_file(include_str!(
        "../crates/obzenflow_runtime/src/supervised_base/handler_supervised.rs"
    ))
    .expect("parse handler supervision");
    assert!(
        !syntax
            .items
            .iter()
            .any(|item| matches!(item, syn::Item::Fn(_))),
        "handler execution belongs to the consuming trait method, not a free function"
    );
    let extension = syntax
        .items
        .iter()
        .find_map(|item| match item {
            syn::Item::Trait(extension) if extension.ident == "HandlerSupervisedExt" => {
                Some(extension)
            }
            _ => None,
        })
        .expect("locate the canonical handler extension trait");
    let runner = extension
        .items
        .iter()
        .find_map(|item| match item {
            syn::TraitItem::Fn(method) if method.sig.ident == "run" => Some(method),
            _ => None,
        })
        .expect("locate the trait-owned runner");
    let receiver = runner.sig.receiver().expect("run owns its supervisor");
    assert!(
        receiver.reference.is_none() && receiver.colon_token.is_none(),
        "run must consume self; a borrowed runner permits repeated execution"
    );
    assert!(runner.sig.asyncness.is_some() && runner.default.is_some());
}

#[test]
fn handler_task_construction_has_one_entry_point() {
    let syntax = syn::parse_file(include_str!(
        "../crates/obzenflow_runtime/src/supervised_base/handle.rs"
    ))
    .expect("parse supervisor task construction");
    let constructors: Vec<_> = syntax
        .items
        .iter()
        .filter_map(|item| match item {
            syn::Item::Impl(implementation) => Some(implementation),
            _ => None,
        })
        .flat_map(|implementation| &implementation.items)
        .filter_map(|item| match item {
            syn::ImplItem::Fn(method)
                if method
                    .sig
                    .ident
                    .to_string()
                    .starts_with("spawn_handler_supervised") =>
            {
                Some(method.sig.ident.to_string())
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        constructors,
        ["spawn_handler_supervised"],
        "resource cleanup must be intrinsic to the runner, never an alternate constructor"
    );
}
