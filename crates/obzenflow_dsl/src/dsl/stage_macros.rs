// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Stage macros for building ObzenFlow pipeline descriptors.
//!
//! Public macros only accept the typed shape. Every authoring site declares
//! its input and output types so the topology API and Studio can label every
//! stage and every edge, and so the edge-compatibility validator (FLOWIP-114c)
//! can catch type-level fan-in mistakes at build time.
//!
//! ```ignore
//! source!(Out => handler)
//! transform!(In -> Out => handler)
//! effectful_transform!(In -> Out with policy => handler)
//! stateful!(In -> Out => handler)
//! effectful_stateful!(In -> Out => handler)
//! sink!(In => command)
//! join!(catalog CatalogStage: Catalog, Stream -> Out => handler)
//! ```
//!
//! Ordinary handler and role slots accept only a local name, unit value path,
//! or qualified value path made from identifier segments. Calls, builder chains,
//! closures, and struct literals are rejected with a diagnostic that teaches
//! the binding idiom. Builder-owned
//! values are constructed as ordinary Rust inside
//! [`FlowDefinition::materialize`](crate::FlowDefinition::materialize),
//! immediately above the inner `flow!`, and only their names appear here.
//! `placeholder!()` and `placeholder!("reason")` remain the sketching forms.
//!
//! The pre-FLOWIP-114c untyped forms (`source!(handler)`,
//! `transform!(handler)`, `sink!(handler)`, etc.) and the mixed-leg join arms
//! (`join!(catalog Ref: mixed, ...)`) were removed in the PR that operationalized
//! FLOWIP-114c. Authoring patterns that previously reached for an untyped
//! handler to demux events at runtime are now expressed as joins (two typed
//! inputs) or per-branch alignment transforms (homogeneous fan-in on an
//! envelope type). The public `obzenflow::flow` API documentation includes an
//! input-alignment recipe; FLOWIP-114c "How to handle heterogeneous fan-in"
//! records the rationale.
//!
//! Public typed arms dispatch into `#[doc(hidden)]` helper macros
//! (`__obzenflow_*_typed!`) that handle normalisation, metadata construction,
//! and descriptor wrapping. The doc-hidden helpers also expose
//! `__obzenflow_*_untyped!` arms used internally by typed expansions for
//! descriptor assembly. `#[macro_export]` makes those helpers technically
//! callable across crates so public expansions work, but direct calls are an
//! unsupported implementation surface rather than an alternate declaration
//! grammar.

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_output_contract_members {
    ($($member:ty),+ $(,)?) => {
        vec![
            $(
                $crate::dsl::typing::TypeHint::exact_payload::<$member>()
            ),+
        ]
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_first_output_contract_member {
    ($first:ty $(, $member:ty)* $(,)?) => {
        $crate::dsl::typing::TypeHint::exact_payload::<$first>()
    };
}

// ============================================================================
// placeholder!
// ============================================================================

/// `placeholder!()` is recognised directly by typed stage macros.
///
/// Used outside a typed stage macro, it is a compile error by design.
#[macro_export]
macro_rules! placeholder {
    () => {
        compile_error!("placeholder!() must be used directly inside a typed stage macro")
    };
    ($msg:expr) => {
        compile_error!("placeholder!(...) must be used directly inside a typed stage macro")
    };
}

/// Implementation helper for the supported public macros' path-only teaching fallback.
///
/// This macro is exported only because public macro expansion may cross crate
/// boundaries. It is not a supported author-facing declaration surface.
#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_handler_path_diagnostic {
    ($surface:literal, $example:literal) => {
        compile_error!(concat!(
            $surface,
            ": the handler slot takes a name, not an expression. ",
            "Bind the handler inside the materialiser immediately above the inner flow, ",
            "then pass the name. Example: ",
            $example,
        ))
    };
}

/// The facts a stage row lists, as a type.
#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_row_facts {
    () => { $crate::__private::EmptySet };
    ($head:ty $(, $rest:ty)* $(,)?) => {
        $crate::__private::WithMember<$head, $crate::__obzenflow_row_facts!($($rest),*)>
    };
}

/// Shared proof-carrying source admission used by all four source families.
#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_admit_typed_source {
    (factory = $factory:ident, output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = $handler:expr, source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*]) => {{
        const _: () = $crate::__private::assert_distinct_outcome_facts::<
            $crate::__obzenflow_row_facts![$($member),+],
        >();
        let __source_policies: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($policy)),*];
        let __ingress_policy: Option<Box<dyn $crate::__private::MiddlewareFactory>> =
            None $(.or_else(|| Some($crate::__private::capture_middleware($ingress))))?;
        let __observers: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($observer)),*];        $crate::dsl::typing::$factory(
            $name,
            $handler,
            $crate::dsl::typing::SourceRowArrow::<
                $out,
                $crate::__obzenflow_row_facts![$($member),+],
            >::new(),
            __source_policies,
            __ingress_policy,
            __observers,        )
    }};
    (factory = $factory:ident, output = $out:ty, name = $name:literal, handler = $handler:expr, source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*]) => {{
        let __source_policies: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($policy)),*];
        let __ingress_policy: Option<Box<dyn $crate::__private::MiddlewareFactory>> =
            None $(.or_else(|| Some($crate::__private::capture_middleware($ingress))))?;
        let __observers: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($observer)),*];        $crate::dsl::typing::$factory(
            $name,
            $handler,
            $crate::dsl::typing::SourceRowArrow::<$out, $out>::new(),
            __source_policies,
            __ingress_policy,
            __observers,        )
    }};
}

/// Shared structural admission for source placeholders. Placeholders never
/// poll a domain dependency or author a row, so they use the classified raw
/// substrate without exposing a raw-handler macro arm.
#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_admit_placeholder_source {
    (factory = $factory:ident, output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, message = $message:expr, source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*]) => {{
        const _: () = $crate::__private::assert_distinct_outcome_facts::<
            $crate::__obzenflow_row_facts![$($member),+],
        >();
        let __source_policies: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($policy)),*];
        let __ingress_policy: Option<Box<dyn $crate::__private::MiddlewareFactory>> =
            None $(.or_else(|| Some($crate::__private::capture_middleware($ingress))))?;
        let __observers: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($observer)),*];        $crate::dsl::typing::$factory::<
            $out,
            $crate::__obzenflow_row_facts![$($member),+],
        >(
            $name,
            $message,
            __source_policies,
            __ingress_policy,
            __observers,        )
    }};
    (factory = $factory:ident, output = $out:ty, name = $name:literal, message = $message:expr, source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*]) => {{
        let __source_policies: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($policy)),*];
        let __ingress_policy: Option<Box<dyn $crate::__private::MiddlewareFactory>> =
            None $(.or_else(|| Some($crate::__private::capture_middleware($ingress))))?;
        let __observers: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($observer)),*];        $crate::dsl::typing::$factory::<$out, $out>(
            $name,
            $message,
            __source_policies,
            __ingress_policy,
            __observers,        )
    }};
}

// ============================================================================
// source!  +  __obzenflow_source_typed!
// ============================================================================

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_source_typed {
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!(), source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*]) => {{
        $crate::__obzenflow_admit_placeholder_source!(factory = placeholder_finite_source_descriptor, output = $out, output_contract = [$($member),+], name = $name, message = None, source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*])
    }};
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!($msg:expr), source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*]) => {{
        $crate::__obzenflow_admit_placeholder_source!(factory = placeholder_finite_source_descriptor, output = $out, output_contract = [$($member),+], name = $name, message = Some($msg), source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*])
    }};
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = $handler:expr, source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*]) => {{
        $crate::__obzenflow_admit_typed_source!(factory = typed_source_descriptor, output = $out, output_contract = [$($member),+], name = $name, handler = $handler, source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*])
    }};
    (output = $out:ty, name = $name:literal, handler = placeholder!(), source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*]) => {{
        $crate::__obzenflow_admit_placeholder_source!(factory = placeholder_finite_source_descriptor, output = $out, name = $name, message = None, source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*])
    }};
    (output = $out:ty, name = $name:literal, handler = placeholder!($msg:expr), source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*]) => {{
        $crate::__obzenflow_admit_placeholder_source!(factory = placeholder_finite_source_descriptor, output = $out, name = $name, message = Some($msg), source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*])
    }};
    (output = $out:ty, name = $name:literal, handler = $handler:expr, source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*]) => {{
        $crate::__obzenflow_admit_typed_source!(factory = typed_source_descriptor, output = $out, name = $name, handler = $handler, source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*])
    }};
}

/// `__obzenflow_source_attachment_contract!` creates a source stage descriptor for a
/// handler of any source role.
#[macro_export]
macro_rules! __obzenflow_source_attachment_contract {












    // FLOWIP-115s: grammar positions retain authority structurally.
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!() $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => {
        $crate::__obzenflow_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = "__obzenflow_binding_derived_name__", handler = placeholder!(), source_policies = [$($($policy),*)?], ingress_policy = [], observers = [])
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr) $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => {
        $crate::__obzenflow_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = "__obzenflow_binding_derived_name__", handler = placeholder!($msg), source_policies = [$($($policy),*)?], ingress_policy = [], observers = [])
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => {
        $crate::__obzenflow_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = "__obzenflow_binding_derived_name__", handler = $handler_head $(:: $handler_tail)*, source_policies = [$($($policy),*)?], ingress_policy = [], observers = [])
    };
    ($out:ty => placeholder!() $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => {
        $crate::__obzenflow_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = placeholder!(), source_policies = [$($($policy),*)?], ingress_policy = [], observers = [])
    };
    ($out:ty => placeholder!($msg:expr) $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => {
        $crate::__obzenflow_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = placeholder!($msg), source_policies = [$($($policy),*)?], ingress_policy = [], observers = [])
    };
    ($out:ty => $handler_head:ident $(:: $handler_tail:ident)* $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => {
        $crate::__obzenflow_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = $handler_head $(:: $handler_tail)*, source_policies = [$($($policy),*)?], ingress_policy = [], observers = [])
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!() $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => {
        $crate::__obzenflow_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = placeholder!(), source_policies = [$($($policy),*)?], ingress_policy = [], observers = [])
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr) $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => {
        $crate::__obzenflow_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = placeholder!($msg), source_policies = [$($($policy),*)?], ingress_policy = [], observers = [])
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => {
        $crate::__obzenflow_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = $handler_head $(:: $handler_tail)*, source_policies = [$($($policy),*)?], ingress_policy = [], observers = [])
    };
    (name: $name:literal, $out:ty => placeholder!() $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => {
        $crate::__obzenflow_source_typed!(output = $out, name = $name, handler = placeholder!(), source_policies = [$($($policy),*)?], ingress_policy = [], observers = [])
    };
    (name: $name:literal, $out:ty => placeholder!($msg:expr) $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => {
        $crate::__obzenflow_source_typed!(output = $out, name = $name, handler = placeholder!($msg), source_policies = [$($($policy),*)?], ingress_policy = [], observers = [])
    };
    (name: $name:literal, $out:ty => $handler_head:ident $(:: $handler_tail:ident)* $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => {
        $crate::__obzenflow_source_typed!(output = $out, name = $name, handler = $handler_head $(:: $handler_tail)*, source_policies = [$($($policy),*)?], ingress_policy = [], observers = [])
    };
    // ── typed (binding-derived name) ──
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!()) => {
        $crate::__obzenflow_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!(),
            middleware = []        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr)) => {
        $crate::__obzenflow_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!($msg),
            middleware = []        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!(),
            middleware = [$($mw),*]        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!($msg),
            middleware = [$($mw),*]        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*) => {
        $crate::__obzenflow_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = "__obzenflow_binding_derived_name__",
            handler = $handler_head $(:: $handler_tail)*,
            middleware = []        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*]) => {
        $crate::__obzenflow_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = "__obzenflow_binding_derived_name__",
            handler = $handler_head $(:: $handler_tail)*,
            middleware = [$($mw),*]        )
    };
    ($out:ty => placeholder!()) => {
        $crate::__obzenflow_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!(),
            middleware = []        )
    };
    ($out:ty => placeholder!($msg:expr)) => {
        $crate::__obzenflow_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!($msg),
            middleware = []        )
    };
    ($out:ty => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!(),
            middleware = [$($mw),*]        )
    };
    ($out:ty => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!($msg),
            middleware = [$($mw),*]        )
    };
    ($out:ty => $handler_head:ident $(:: $handler_tail:ident)*) => {
        $crate::__obzenflow_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = $handler_head $(:: $handler_tail)*,
            middleware = []        )
    };
    ($out:ty => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*]) => {
        $crate::__obzenflow_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = $handler_head $(:: $handler_tail)*,
            middleware = [$($mw),*]        )
    };

    // ── typed (explicit name override) ──
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!()) => {
        $crate::__obzenflow_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = placeholder!(), middleware = [])
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr)) => {
        $crate::__obzenflow_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = placeholder!($msg), middleware = [])
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = placeholder!(), middleware = [$($mw),*])
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = placeholder!($msg), middleware = [$($mw),*])
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*) => {
        $crate::__obzenflow_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = $handler_head $(:: $handler_tail)*, middleware = [])
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*]) => {
        $crate::__obzenflow_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = $handler_head $(:: $handler_tail)*, middleware = [$($mw),*])
    };
    (name: $name:literal, $out:ty => placeholder!()) => {
        $crate::__obzenflow_source_typed!(output = $out, name = $name, handler = placeholder!(), middleware = [])
    };
    (name: $name:literal, $out:ty => placeholder!($msg:expr)) => {
        $crate::__obzenflow_source_typed!(output = $out, name = $name, handler = placeholder!($msg), middleware = [])
    };
    (name: $name:literal, $out:ty => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_source_typed!(output = $out, name = $name, handler = placeholder!(), middleware = [$($mw),*])
    };
    (name: $name:literal, $out:ty => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_source_typed!(output = $out, name = $name, handler = placeholder!($msg), middleware = [$($mw),*])
    };
    (name: $name:literal, $out:ty => $handler_head:ident $(:: $handler_tail:ident)*) => {
        $crate::__obzenflow_source_typed!(output = $out, name = $name, handler = $handler_head $(:: $handler_tail)*, middleware = [])
    };
    (name: $name:literal, $out:ty => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*]) => {
        $crate::__obzenflow_source_typed!(output = $out, name = $name, handler = $handler_head $(:: $handler_tail)*, middleware = [$($mw),*])
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => $handler:expr $(, [$($mw:expr),*])?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "source!",
            "let source = MySource::new(...); events = source!(Event => source);"
        )
    };
    ($out:ty => $handler:expr $(, [$($mw:expr),*])?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "source!",
            "let source = MySource::new(...); events = source!(Event => source);"
        )
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => $handler:expr $(, [$($mw:expr),*])?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "source!",
            "let source = MySource::new(...); events = source!(name: \"events\", Event => source);"
        )
    };
    (name: $name:literal, $out:ty => $handler:expr $(, [$($mw:expr),*])?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "source!",
            "let source = MySource::new(...); events = source!(name: \"events\", Event => source);"
        )
    };
}

// ============================================================================
// transform!  +  __obzenflow_transform_typed!
// ============================================================================

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_transform_typed {
    // -- exact input, placeholder, explicit output contract --
    (input = exact($in:ty), output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!(), middleware = [$($mw:expr),*]) => {{
        const _: () = $crate::__private::assert_distinct_outcome_facts::<
            $crate::__obzenflow_row_facts![$($member),+],
        >();
        let __observers: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($mw)),*];        $crate::dsl::typing::placeholder_transform_descriptor::<
            $in,
            $out,
            $crate::__obzenflow_row_facts![$($member),+],
            _,
        >($name, None, __observers)
    }};
    (input = exact($in:ty), output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!($msg:expr), middleware = [$($mw:expr),*]) => {{
        const _: () = $crate::__private::assert_distinct_outcome_facts::<
            $crate::__obzenflow_row_facts![$($member),+],
        >();
        let __observers: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($mw)),*];        $crate::dsl::typing::placeholder_transform_descriptor::<
            $in,
            $out,
            $crate::__obzenflow_row_facts![$($member),+],
            _,
        >($name, Some($msg), __observers)
    }};
    // -- exact input, real handler, explicit output contract --
    // FLOWIP-120b Option B keeps the flat output contract in the arrow's
    // type-signature position; the handler-side outcome is never named in the
    // flow.
    (input = exact($in:ty), output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*]) => {{
        let __handler = $handler;
        const _: () = $crate::__private::assert_distinct_outcome_facts::<
            $crate::__obzenflow_row_facts![$($member),+],
        >();
        let __observers: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($mw)),*];        $crate::dsl::typing::typed_transform_descriptor::<
            _,
            $in,
            $out,
            $crate::__obzenflow_row_facts![$($member),+],
            _,
            _,
            _,
        >($name, __handler, __observers)
    }};
    // ── exact input, placeholder ──
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = placeholder!(), middleware = [$($mw:expr),*]) => {{
        let __observers: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($mw)),*];        $crate::dsl::typing::placeholder_transform_descriptor::<$in, $out, $out, _>(
            $name,
            None,
            __observers,        )
    }};
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = placeholder!($msg:expr), middleware = [$($mw:expr),*]) => {{
        let __observers: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($mw)),*];        $crate::dsl::typing::placeholder_transform_descriptor::<$in, $out, $out, _>(
            $name,
            Some($msg),
            __observers,        )
    }};
    // ── exact input, real handler ──
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*]) => {{
        let __handler = $handler;
        let __observers: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($mw)),*];        $crate::dsl::typing::typed_transform_descriptor::<
            _,
            $in,
            $out,
            $out,
            _,
            _,
            _,
        >($name, __handler, __observers)
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_transform_exact_contract {
    (name = $name:literal, $($rest:tt)+) => {
        $crate::__obzenflow_transform_exact_contract!(@collect name = $name, in = (), $($rest)+)
    };



    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!()) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!(),
            middleware = []        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr)) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!($msg),
            middleware = []        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!() @attachments [$($mw:expr),*]) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!(),
            middleware = [$($mw),*]        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr) @attachments [$($mw:expr),*]) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!($msg),
            middleware = [$($mw),*]        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = []        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* @attachments [$($mw:expr),*]) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = [$($mw),*]        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty, outputs: [$($member:ty),+ $(,)?] => placeholder!()) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $out,
            output_contract = [$($member),+],
            name = $name,
            handler = placeholder!(),
            middleware = []        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty, outputs: [$($member:ty),+ $(,)?] => placeholder!($msg:expr)) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $out,
            output_contract = [$($member),+],
            name = $name,
            handler = placeholder!($msg),
            middleware = []        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty, outputs: [$($member:ty),+ $(,)?] => placeholder!() @attachments [$($mw:expr),*]) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $out,
            output_contract = [$($member),+],
            name = $name,
            handler = placeholder!(),
            middleware = [$($mw),*]        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty, outputs: [$($member:ty),+ $(,)?] => placeholder!($msg:expr) @attachments [$($mw:expr),*]) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $out,
            output_contract = [$($member),+],
            name = $name,
            handler = placeholder!($msg),
            middleware = [$($mw),*]        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty, outputs: [$($member:ty),+ $(,)?] => $handler_head:ident $(:: $handler_tail:ident)*) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $out,
            output_contract = [$($member),+],
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = []        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty, outputs: [$($member:ty),+ $(,)?] => $handler_head:ident $(:: $handler_tail:ident)* @attachments [$($mw:expr),*]) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $out,
            output_contract = [$($member),+],
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = [$($mw),*]        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!()) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!(),
            middleware = []        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!($msg:expr)) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!($msg),
            middleware = []        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!() @attachments [$($mw:expr),*]) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!(),
            middleware = [$($mw),*]        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!($msg:expr) @attachments [$($mw:expr),*]) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!($msg),
            middleware = [$($mw),*]        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler_head:ident $(:: $handler_tail:ident)*) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = []        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler_head:ident $(:: $handler_tail:ident)* @attachments [$($mw:expr),*]) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = [$($mw),*]        )
    };



    (@collect name = $name:literal, in = ($($in:tt)*), $tok:tt $($rest:tt)+) => {
        $crate::__obzenflow_transform_exact_contract!(
            @collect
            name = $name,
            in = ($($in)* $tok),
            $($rest)+
        )
    };
    (@collect name = $name:literal, in = (), -> $($rest:tt)*) => {
        compile_error!("transform!: expected `InputType -> OutputType => handler`");
    };
    (@collect name = $name:literal, in = ($($in:tt)+), $($rest:tt)*) => {
        compile_error!("transform!: expected `-> OutputType => handler` after input type");
    };
}

/// Create a transform stage descriptor.
#[macro_export]
macro_rules! __obzenflow_transform_attachment_contract {
    // ── typed (exact input) ──
    (name: $name:literal, $($rest:tt)+) => {
        $crate::__obzenflow_transform_exact_contract!(name = $name, $($rest)+)
    };
    ($($rest:tt)+) => {
        $crate::__obzenflow_transform_exact_contract!(
            name = "__obzenflow_binding_derived_name__",
            $($rest)+
        )
    };
}

// ============================================================================
// effectful_transform!  +  effectful_stateful!
// ============================================================================

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_assert_effectful_transform_contract {
    ($handler:ident, $in:ty, [$($member:ty),+]) => {{
        fn __obzenflow_assert_effectful_transform_contract<
            H,
            ArrowToHandlerProof,
            HandlerToArrowProof,
        >(_: &H)
        where
            H: $crate::__private::EffectfulTransformHandler,
            <H as $crate::__private::EffectfulTransformHandler>::Input:
                $crate::dsl::typing::EffectfulTransformInputMatchesArrow<$in>,
            <$crate::__obzenflow_row_facts![$($member),+] as $crate::__private::Outcome>::Members:
                $crate::dsl::typing::ArrowOutputsAreDeclaredByHandler<
                    <<H as $crate::__private::EffectfulTransformHandler>::Output as $crate::__private::Outcome>::Members,
                    ArrowToHandlerProof,
                >,
            <<H as $crate::__private::EffectfulTransformHandler>::Output as $crate::__private::Outcome>::Members:
                $crate::dsl::typing::HandlerOutputsAreDeclaredByArrow<
                    <$crate::__obzenflow_row_facts![$($member),+] as $crate::__private::Outcome>::Members,
                    HandlerToArrowProof,
                >,
        {}

        __obzenflow_assert_effectful_transform_contract::<_, _, _>(&$handler);
        const _: () = $crate::__private::assert_distinct_outcome_facts::<
            $crate::__obzenflow_row_facts![$($member),+],
        >();
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_assert_effectful_stateful_contract {
    ($handler:ident, $in:ty, [$($member:ty),+]) => {{
        fn __obzenflow_assert_effectful_stateful_contract<
            H,
            ArrowToHandlerProof,
            HandlerToArrowProof,
        >(_: &H)
        where
            H: $crate::__private::EffectfulStatefulHandler,
            <H as $crate::__private::EffectfulStatefulHandler>::Input:
                $crate::dsl::typing::EffectfulStatefulInputMatchesArrow<$in>,
            <$crate::__obzenflow_row_facts![$($member),+] as $crate::__private::Outcome>::Members:
                $crate::dsl::typing::ArrowOutputsAreDeclaredByHandler<
                    <<H as $crate::__private::EffectfulStatefulHandler>::Output as $crate::__private::Outcome>::Members,
                    ArrowToHandlerProof,
                >,
            <<H as $crate::__private::EffectfulStatefulHandler>::Output as $crate::__private::Outcome>::Members:
                $crate::dsl::typing::HandlerOutputsAreDeclaredByArrow<
                    <$crate::__obzenflow_row_facts![$($member),+] as $crate::__private::Outcome>::Members,
                    HandlerToArrowProof,
                >,
        {}

        __obzenflow_assert_effectful_stateful_contract::<_, _, _>(&$handler);
        const _: () = $crate::__private::assert_distinct_outcome_facts::<
            $crate::__obzenflow_row_facts![$($member),+],
        >();
    }};
}

/// Lower an effectful row. The command's effect set declares the stage's
/// effects, and its ports answer them.
#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_effectful_row {
    ($family:ident, name = $name:literal, in = [], -> $($rest:tt)*) => {
        compile_error!("expected an input type before `->`")
    };
    ($family:ident, name = $name:literal, in = [$($in:tt)+], -> $($rest:tt)+) => {
        $crate::__obzenflow_effectful_row!(@output $family, name = $name, in = [$($in)+], out = [], $($rest)+)
    };
    ($family:ident, name = $name:literal, in = [$($in:tt)*], $next:tt $($rest:tt)*) => {
        $crate::__obzenflow_effectful_row!($family, name = $name, in = [$($in)* $next], $($rest)*)
    };
    ($family:ident, name = $name:literal, in = [$($in:tt)*]) => {
        compile_error!("expected `Input -> Output with middleware => handler`")
    };
    (@output $family:ident, name = $name:literal, in = [$($in:tt)+], out = [$($out:tt)+], => $($rest:tt)+) => {
        $crate::__obzenflow_effectful_row!(@handler $family, name = $name, in = [$($in)+], out = [$($out)+], $($rest)+)
    };
    (@output $family:ident, name = $name:literal, in = [$($in:tt)+], out = [$($out:tt)*], $next:tt $($rest:tt)*) => {
        $crate::__obzenflow_effectful_row!(@output $family, name = $name, in = [$($in)+], out = [$($out)* $next], $($rest)*)
    };
    (@output $family:ident, name = $name:literal, in = [$($in:tt)+], out = [$($out:tt)*]) => {
        compile_error!("expected `Input -> Output with middleware => handler`")
    };
    (@handler $family:ident, name = $name:literal, in = [$($in:tt)+], out = [{ $first:ty $(, $member:ty)* $(,)? }], $handler_head:ident $(:: $handler_tail:ident)* @attachments [$($mw:expr),* $(,)?]) => {
        $crate::__obzenflow_effectful_lower!(
            $family,
            input = exact($($in)+),
            output = $first,
            members = [$first $(, $member)*],
            contract = [$first $(, $member)*],
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = [$($mw),*]
        )
    };
    (@handler $family:ident, name = $name:literal, in = [$($in:tt)+], out = [$out:ty], $handler_head:ident $(:: $handler_tail:ident)* @attachments [$($mw:expr),* $(,)?]) => {
        $crate::__obzenflow_effectful_lower!(
            $family,
            input = exact($($in)+),
            output = $out,
            members = [$out],
            contract = single,
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = [$($mw),*]
        )
    };
    (@handler $family:ident, name = $name:literal, in = [$($in:tt)+], out = [$($out:tt)+], placeholder!($($message:tt)*) $($rest:tt)*) => {
        compile_error!("an effectful stage takes a named handler, whose effect set declares the effects it performs")
    };
    (@handler $family:ident, name = $name:literal, in = [$($in:tt)+], out = [$($out:tt)+], $($rest:tt)*) => {
        compile_error!("expected `Input -> Output with middleware => handler`")
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_effectful_metadata {
    (transform, $in:ty, $out:ty, single) => {
        $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::exact_payload::<$in>(),
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            false,
            None,
        )
    };
    (transform, $in:ty, $out:ty, [$($member:ty),+]) => {
        $crate::__obzenflow_effectful_metadata!(transform, $in, $out, single)
            .with_additional_output_contract($crate::__obzenflow_output_contract_members!($($member),+))
    };
    (stateful, $in:ty, $out:ty, single) => {
        $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::exact_payload::<$in>(),
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            false,
            None,
        )
    };
    (stateful, $in:ty, $out:ty, [$($member:ty),+]) => {
        $crate::__obzenflow_effectful_metadata!(stateful, $in, $out, single)
            .with_additional_output_contract($crate::__obzenflow_output_contract_members!($($member),+))
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_effectful_lower {
    (transform, input = exact($in:ty), output = $out:ty, members = [$($member:ty),+], contract = $contract:tt, name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*]) => {{
        let __handler = $handler;
        $crate::__private::assert_command(&__handler);
        $crate::__private::assert_handler_performs_no_ai_effect(&__handler);
        $crate::__obzenflow_assert_effectful_transform_contract!(__handler, $in, [$($member),+]);
        let __effects =
            $crate::__private::effectful_transform_effects(&__handler);
        let __descriptor: Box<dyn $crate::dsl::stage_descriptor::StageDescriptor> = Box::new(
            $crate::dsl::stage_descriptor::EffectfulTransformDescriptor::new(
                $name,
                __handler,
                __effects,
                vec![$($crate::__private::capture_middleware($mw)),*],
            ),
        );
        $crate::dsl::typing::wrap_typed_descriptor(
            __descriptor,
            $crate::__obzenflow_effectful_metadata!(transform, $in, $out, $contract),
        )
    }};
    (stateful, input = exact($in:ty), output = $out:ty, members = [$($member:ty),+], contract = $contract:tt, name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*]) => {{
        let __handler = $handler;
        $crate::__private::assert_command(&__handler);
        $crate::__private::assert_stateful_handler_performs_no_ai_effect(&__handler);
        $crate::__obzenflow_assert_effectful_stateful_contract!(__handler, $in, [$($member),+]);
        let __effects =
            $crate::__private::effectful_stateful_effects(&__handler);
        let __descriptor: Box<dyn $crate::dsl::stage_descriptor::StageDescriptor> = Box::new(
            $crate::dsl::stage_descriptor::EffectfulStatefulDescriptor::new(
                $name,
                __handler,
                __effects,
                vec![$($crate::__private::capture_middleware($mw)),*],
            ),
        );
        $crate::dsl::typing::wrap_typed_descriptor(
            __descriptor,
            $crate::__obzenflow_effectful_metadata!(stateful, $in, $out, $contract),
        )
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_effectful_transform_attachment_contract {
    (name: $name:literal, $($rest:tt)+) => {
        $crate::__obzenflow_effectful_row!(transform, name = $name, in = [], $($rest)+)
    };
    ($($rest:tt)+) => {
        $crate::__obzenflow_effectful_row!(
            transform,
            name = "__obzenflow_binding_derived_name__",
            in = [],
            $($rest)+
        )
    };
}

// ============================================================================
// sink!  +  __obzenflow_sink_typed!
// ============================================================================

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_sink_typed {
    (input = exact($in:ty), name = $name:literal, handler = placeholder!(), middleware = [$($mw:expr),*]) => {{
        $crate::dsl::typing::placeholder_sink_descriptor::<$in>(
            $name,
            None,
            vec![$($crate::__private::capture_middleware($mw)),*],
        )
    }};
    (input = exact($in:ty), name = $name:literal, handler = placeholder!($msg:expr), middleware = [$($mw:expr),*]) => {{
        $crate::dsl::typing::placeholder_sink_descriptor::<$in>(
            $name,
            Some($msg),
            vec![$($crate::__private::capture_middleware($mw)),*],
        )
    }};
    (input = exact($in:ty), name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*]) => {{
        let __handler = $handler;
        $crate::dsl::typing::typed_sink_descriptor::<_, $in, _>(
            $name,
            __handler,
            vec![$($crate::__private::capture_middleware($mw)),*],
        )
    }};
}

/// Create a sink stage descriptor.
///
/// Canonical grammar: `InputType with middleware => command`, where
/// `with middleware` or `with { a, b }` is optional. The command is
/// `perform(...)` with its port, a ready-made helper such as `ConsoleSink`, or a
/// struct implementing `SinkRole`, built above `flow!` and passed by path. The
/// sink sends one request per input through the command's port, and the port
/// method's `#[effect]` states the guarantee.
#[macro_export]
macro_rules! __obzenflow_sink_attachment_contract {
    ($in:ty => select($selector:expr) {$($arms:tt)*} $($rest:tt)*) => {
        compile_error!("choose a sink's system in code, by building its port with `Port::to` or `Port::deferred` in a `match`")
    };
    (name: $name:literal, $in:ty => select($selector:expr) {$($arms:tt)*} $($rest:tt)*) => {
        compile_error!("choose a sink's system in code, by building its port with `Port::to` or `Port::deferred` in a `match`")
    };
    ($in:ty => placeholder!() $(@attachments [$($mw:expr),* $(,)?])? $(,)?) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = "__obzenflow_binding_derived_name__", handler = placeholder!(), middleware = [$($($mw),*)?])
    };
    ($in:ty => placeholder!($msg:expr) $(@attachments [$($mw:expr),* $(,)?])? $(,)?) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = "__obzenflow_binding_derived_name__", handler = placeholder!($msg), middleware = [$($($mw),*)?])
    };
    (|$($closure:tt)*) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "sink!",
            "let output = MySink::new(...); events = sink!(Event => output);"
        )
    };
    (move |$($closure:tt)*) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "sink!",
            "let output = MySink::new(...); events = sink!(Event => output);"
        )
    };
    ($in:ty => |$($closure:tt)*) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "sink!",
            "let output = MySink::new(...); events = sink!(Event => output);"
        )
    };
    ($in:ty => move |$($closure:tt)*) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "sink!",
            "let output = MySink::new(...); events = sink!(Event => output);"
        )
    };
    ($in:ty => sinks::$factory:ident($($args:tt)*) ) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "sink!",
            "let output = sinks::ConsoleSink::<Event, _>::new(sinks::JsonFormatter); events = sink!(Event => output);"
        )
    };
    ($in:ty => $handler_head:ident $(:: $handler_tail:ident)*
        $(@attachments [$($mw:expr),* $(,)?])?
    ) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = "__obzenflow_binding_derived_name__", handler = $handler_head $(:: $handler_tail)*, middleware = [$($($mw),*)?])
    };
    (name: $name:literal, $in:ty => placeholder!() $(@attachments [$($mw:expr),* $(,)?])? $(,)?) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = $name, handler = placeholder!(), middleware = [$($($mw),*)?])
    };
    (name: $name:literal, $in:ty => placeholder!($msg:expr) $(@attachments [$($mw:expr),* $(,)?])? $(,)?) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = $name, handler = placeholder!($msg), middleware = [$($($mw),*)?])
    };
    (name: $name:literal, |$($closure:tt)*) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "sink!",
            "let output = MySink::new(...); events = sink!(name: \"events\", Event => output);"
        )
    };
    (name: $name:literal, move |$($closure:tt)*) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "sink!",
            "let output = MySink::new(...); events = sink!(name: \"events\", Event => output);"
        )
    };
    (name: $name:literal, $in:ty => |$($closure:tt)*) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "sink!",
            "let output = MySink::new(...); events = sink!(name: \"events\", Event => output);"
        )
    };
    (name: $name:literal, $in:ty => move |$($closure:tt)*) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "sink!",
            "let output = MySink::new(...); events = sink!(name: \"events\", Event => output);"
        )
    };
    (name: $name:literal, $in:ty => sinks::$factory:ident($($args:tt)*) ) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "sink!",
            "let output = sinks::ConsoleSink::<Event, _>::new(sinks::JsonFormatter); events = sink!(name: \"events\", Event => output);"
        )
    };
    (name: $name:literal, $in:ty => $handler_head:ident $(:: $handler_tail:ident)*
        $(@attachments [$($mw:expr),* $(,)?])?
    ) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = $name, handler = $handler_head $(:: $handler_tail)*, middleware = [$($($mw),*)?])
    };
}

// ============================================================================
// stateful!  +  __obzenflow_stateful_typed!
// ============================================================================

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_stateful_emit_option {
    (none) => {
        None
    };
    (some($interval:expr)) => {
        Some($interval)
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_stateful_typed {
    (input = exact($in:ty), output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!(), emit = $emit:ident $(($interval:expr))?, middleware = [$($mw:expr),*]) => {{
        const _: () = $crate::__private::assert_distinct_outcome_facts::<
            $crate::__obzenflow_row_facts![$($member),+],
        >();
        let __observers: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($mw)),*];        $crate::dsl::typing::placeholder_stateful_descriptor::<
            $in,
            $out,
            $crate::__obzenflow_row_facts![$($member),+],
            _,
        >(
            $name,
            None,
            $crate::__obzenflow_stateful_emit_option!($emit $(($interval))?),
            __observers,        )
    }};
    (input = exact($in:ty), output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!($msg:expr), emit = $emit:ident $(($interval:expr))?, middleware = [$($mw:expr),*]) => {{
        const _: () = $crate::__private::assert_distinct_outcome_facts::<
            $crate::__obzenflow_row_facts![$($member),+],
        >();
        let __observers: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($mw)),*];        $crate::dsl::typing::placeholder_stateful_descriptor::<
            $in,
            $out,
            $crate::__obzenflow_row_facts![$($member),+],
            _,
        >(
            $name,
            Some($msg),
            $crate::__obzenflow_stateful_emit_option!($emit $(($interval))?),
            __observers,        )
    }};
    (input = exact($in:ty), output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = $handler:expr, emit = $emit:ident $(($interval:expr))?, middleware = [$($mw:expr),*]) => {{
        let __handler = $handler;
        const _: () = $crate::__private::assert_distinct_outcome_facts::<
            $crate::__obzenflow_row_facts![$($member),+],
        >();
        let __observers: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($mw)),*];        $crate::dsl::typing::typed_stateful_descriptor::<
            _,
            $in,
            $out,
            $crate::__obzenflow_row_facts![$($member),+],
            _,
            _,
            _,
        >(
            $name,
            __handler,
            $crate::__obzenflow_stateful_emit_option!($emit $(($interval))?),
            __observers,        )
    }};
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = placeholder!(), emit = $emit:ident $(($interval:expr))?, middleware = [$($mw:expr),*]) => {{
        let __observers: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($mw)),*];        $crate::dsl::typing::placeholder_stateful_descriptor::<$in, $out, $out, _>(
            $name,
            None,
            $crate::__obzenflow_stateful_emit_option!($emit $(($interval))?),
            __observers,        )
    }};
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = placeholder!($msg:expr), emit = $emit:ident $(($interval:expr))?, middleware = [$($mw:expr),*]) => {{
        let __observers: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($mw)),*];        $crate::dsl::typing::placeholder_stateful_descriptor::<$in, $out, $out, _>(
            $name,
            Some($msg),
            $crate::__obzenflow_stateful_emit_option!($emit $(($interval))?),
            __observers,        )
    }};
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = $handler:expr, emit = $emit:ident $(($interval:expr))?, middleware = [$($mw:expr),*]) => {{
        let __handler = $handler;
        let __observers: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($mw)),*];        $crate::dsl::typing::typed_stateful_descriptor::<_, $in, $out, $out, _, _, _>(
            $name,
            __handler,
            $crate::__obzenflow_stateful_emit_option!($emit $(($interval))?),
            __observers,        )
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_stateful_exact_contract {
    (name = $name:literal, $($rest:tt)+) => {
        $crate::__obzenflow_stateful_exact_contract!(@collect name = $name, in = (), $($rest)+)
    };


    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!()) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!(),
            emit = none,
            middleware = []        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr)) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!($msg),
            emit = none,
            middleware = []        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!() @attachments [$($mw:expr),*]) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!(),
            emit = none,
            middleware = [$($mw),*]        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr) @attachments [$($mw:expr),*]) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!($msg),
            emit = none,
            middleware = [$($mw),*]        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!(), emit_interval = $emit_interval:expr) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!(),
            emit = some($emit_interval),
            middleware = []        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr), emit_interval = $emit_interval:expr) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!($msg),
            emit = some($emit_interval),
            middleware = []        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!() @attachments [$($mw:expr),*], emit_interval = $emit_interval:expr) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!(),
            emit = some($emit_interval),
            middleware = [$($mw),*]        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr) @attachments [$($mw:expr),*], emit_interval = $emit_interval:expr) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!($msg),
            emit = some($emit_interval),
            middleware = [$($mw),*]        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            emit = none,
            middleware = []        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* @attachments [$($mw:expr),*]) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            emit = none,
            middleware = [$($mw),*]        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*, emit_interval = $emit_interval:expr) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            emit = some($emit_interval),
            middleware = []        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* @attachments [$($mw:expr),*], emit_interval = $emit_interval:expr) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            emit = some($emit_interval),
            middleware = [$($mw),*]        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!()) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!(),
            emit = none,
            middleware = []        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!($msg:expr)) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!($msg),
            emit = none,
            middleware = []        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!() @attachments [$($mw:expr),*]) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!(),
            emit = none,
            middleware = [$($mw),*]        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!($msg:expr) @attachments [$($mw:expr),*]) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!($msg),
            emit = none,
            middleware = [$($mw),*]        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!(), emit_interval = $emit_interval:expr) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!(),
            emit = some($emit_interval),
            middleware = []        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!($msg:expr), emit_interval = $emit_interval:expr) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!($msg),
            emit = some($emit_interval),
            middleware = []        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!() @attachments [$($mw:expr),*], emit_interval = $emit_interval:expr) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!(),
            emit = some($emit_interval),
            middleware = [$($mw),*]        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!($msg:expr) @attachments [$($mw:expr),*], emit_interval = $emit_interval:expr) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!($msg),
            emit = some($emit_interval),
            middleware = [$($mw),*]        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler_head:ident $(:: $handler_tail:ident)*) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            emit = none,
            middleware = []        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler_head:ident $(:: $handler_tail:ident)* @attachments [$($mw:expr),*]) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            emit = none,
            middleware = [$($mw),*]        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler_head:ident $(:: $handler_tail:ident)*, emit_interval = $emit_interval:expr) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            emit = some($emit_interval),
            middleware = []        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler_head:ident $(:: $handler_tail:ident)* @attachments [$($mw:expr),*], emit_interval = $emit_interval:expr) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            emit = some($emit_interval),
            middleware = [$($mw),*]        )
    };


    (@collect name = $name:literal, in = ($($in:tt)*), $tok:tt $($rest:tt)+) => {
        $crate::__obzenflow_stateful_exact_contract!(
            @collect
            name = $name,
            in = ($($in)* $tok),
            $($rest)+
        )
    };
    (@collect name = $name:literal, in = (), -> $($rest:tt)*) => {
        compile_error!("stateful!: expected `InputType -> OutputType => handler`");
    };
    (@collect name = $name:literal, in = ($($in:tt)+), $($rest:tt)*) => {
        compile_error!("stateful!: expected `-> OutputType => handler` after input type");
    };
}

/// Create a stateful stage descriptor.
#[macro_export]
macro_rules! __obzenflow_stateful_attachment_contract {
    // ── typed (exact input) ──
    (name: $name:literal, $($rest:tt)+) => {
        $crate::__obzenflow_stateful_exact_contract!(name = $name, $($rest)+)
    };
    ($($rest:tt)+) => {
        $crate::__obzenflow_stateful_exact_contract!(
            name = "__obzenflow_binding_derived_name__",
            $($rest)+
        )
    };
}

// ============================================================================
// effectful_stateful!
// ============================================================================

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_effectful_stateful_attachment_contract {
    (name: $name:literal, $($rest:tt)+) => {
        $crate::__obzenflow_effectful_row!(stateful, name = $name, in = [], $($rest)+)
    };
    ($($rest:tt)+) => {
        $crate::__obzenflow_effectful_row!(
            stateful,
            name = "__obzenflow_binding_derived_name__",
            in = [],
            $($rest)+
        )
    };
}

// ============================================================================
// join!  +  __obzenflow_join_typed!
// ============================================================================

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_join_typed {
    // -- placeholder, explicit output contract --
    (reference = exact, stream = exact, output = $out:ty,
     output_contract = [$($member:ty),+ $(,)?],
     ref_type = ($ref_ty:ty), stream_type = ($str_ty:ty),
     name = $name:literal, ref_var = $ref_var:ident, handler = placeholder!(),
     middleware = [$($mw:expr),*]) => {{
        const _: () = $crate::__private::assert_distinct_outcome_facts::<
            $crate::__obzenflow_row_facts![$($member),+],
        >();
        let __observers: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($mw)),*];
        $crate::dsl::typing::placeholder_join_descriptor::<
            $ref_ty,
            $str_ty,
            $out,
            $crate::__obzenflow_row_facts![$($member),+],
            _,
        >($name, stringify!($ref_var), None, __observers)
    }};
    (reference = exact, stream = exact, output = $out:ty,
     output_contract = [$($member:ty),+ $(,)?],
     ref_type = ($ref_ty:ty), stream_type = ($str_ty:ty),
     name = $name:literal, ref_var = $ref_var:ident, handler = placeholder!($msg:expr),
     middleware = [$($mw:expr),*]) => {{
        const _: () = $crate::__private::assert_distinct_outcome_facts::<
            $crate::__obzenflow_row_facts![$($member),+],
        >();
        let __observers: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($mw)),*];
        $crate::dsl::typing::placeholder_join_descriptor::<
            $ref_ty,
            $str_ty,
            $out,
            $crate::__obzenflow_row_facts![$($member),+],
            _,
        >($name, stringify!($ref_var), Some($msg), __observers)
    }};

    // -- placeholder --
    (reference = exact, stream = exact, output = $out:ty,
     ref_type = ($ref_ty:ty), stream_type = ($str_ty:ty),
     name = $name:literal, ref_var = $ref_var:ident, handler = placeholder!(),
     middleware = [$($mw:expr),*]) => {{
        let __observers: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($mw)),*];
        $crate::dsl::typing::placeholder_join_descriptor::<
            $ref_ty, $str_ty, $out, $out, _,
        >($name, stringify!($ref_var), None, __observers)
    }};
    (reference = exact, stream = exact, output = $out:ty,
     ref_type = ($ref_ty:ty), stream_type = ($str_ty:ty),
     name = $name:literal, ref_var = $ref_var:ident, handler = placeholder!($msg:expr),
     middleware = [$($mw:expr),*]) => {{
        let __observers: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($mw)),*];
        $crate::dsl::typing::placeholder_join_descriptor::<
            $ref_ty, $str_ty, $out, $out, _,
        >($name, stringify!($ref_var), Some($msg), __observers)
    }};

    // ── real handler: both exact ──
    (reference = exact, stream = exact, output = $out:ty,
     output_contract = [$($member:ty),+ $(,)?],
     ref_type = ($ref_ty:ty), stream_type = ($str_ty:ty),
     name = $name:literal, ref_var = $ref_var:ident, handler = $handler:expr,
     middleware = [$($mw:expr),*]) => {{
        let __handler = $handler;
        const _: () = $crate::__private::assert_distinct_outcome_facts::<
            $crate::__obzenflow_row_facts![$($member),+],
        >();
        let __observers: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($mw)),*];
        $crate::dsl::typing::typed_join_descriptor::<
            _,
            $ref_ty,
            $str_ty,
            $out,
            $crate::__obzenflow_row_facts![$($member),+],
            _,
            _,
            _,
        >($name, stringify!($ref_var), __handler, __observers)
    }};

    (reference = exact, stream = exact, output = $out:ty,
     ref_type = ($ref_ty:ty), stream_type = ($str_ty:ty),
     name = $name:literal, ref_var = $ref_var:ident, handler = $handler:expr,
     middleware = [$($mw:expr),*]) => {{
        let __handler = $handler;
        let __observers: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($mw)),*];
        $crate::dsl::typing::typed_join_descriptor::<
            _, $ref_ty, $str_ty, $out, $out, _, _, _,
        >($name, stringify!($ref_var), __handler, __observers)
    }};

}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_join_exact_stream_contract {
    (name = $name:literal, ref_var = $ref_var:ident, reference = exact($reference:ty), $($rest:tt)+) => {
        $crate::__obzenflow_join_exact_stream_contract!(
            @collect
            name = $name,
            ref_var = $ref_var,
            reference = exact,
            ref_type = ($reference),
            stream = (),
            $($rest)+
        )
    };


    (@collect
     name = $name:literal,
     ref_var = $ref_var:ident,
     reference = $ref_hint:tt,
     ref_type = $ref_type:tt,
     stream = ($($stream:tt)+),
     -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!()) => {
        $crate::__obzenflow_join_typed!(
            reference = $ref_hint,
            stream = exact,
            output = $first,
            output_contract = [$first $(, $member)*],
            ref_type = $ref_type,
            stream_type = ($($stream)+),
            name = $name,
            ref_var = $ref_var,
            handler = placeholder!(),
            middleware = []
        )
    };
    (@collect
     name = $name:literal,
     ref_var = $ref_var:ident,
     reference = $ref_hint:tt,
     ref_type = $ref_type:tt,
     stream = ($($stream:tt)+),
     -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr)) => {
        $crate::__obzenflow_join_typed!(
            reference = $ref_hint,
            stream = exact,
            output = $first,
            output_contract = [$first $(, $member)*],
            ref_type = $ref_type,
            stream_type = ($($stream)+),
            name = $name,
            ref_var = $ref_var,
            handler = placeholder!($msg),
            middleware = []
        )
    };
    (@collect
     name = $name:literal,
     ref_var = $ref_var:ident,
     reference = $ref_hint:tt,
     ref_type = $ref_type:tt,
     stream = ($($stream:tt)+),
     -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!() @attachments [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = $ref_hint,
            stream = exact,
            output = $first,
            output_contract = [$first $(, $member)*],
            ref_type = $ref_type,
            stream_type = ($($stream)+),
            name = $name,
            ref_var = $ref_var,
            handler = placeholder!(),
            middleware = [$($mw),*]
        )
    };
    (@collect
     name = $name:literal,
     ref_var = $ref_var:ident,
     reference = $ref_hint:tt,
     ref_type = $ref_type:tt,
     stream = ($($stream:tt)+),
     -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr) @attachments [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = $ref_hint,
            stream = exact,
            output = $first,
            output_contract = [$first $(, $member)*],
            ref_type = $ref_type,
            stream_type = ($($stream)+),
            name = $name,
            ref_var = $ref_var,
            handler = placeholder!($msg),
            middleware = [$($mw),*]
        )
    };
    (@collect
     name = $name:literal,
     ref_var = $ref_var:ident,
     reference = $ref_hint:tt,
     ref_type = $ref_type:tt,
     stream = ($($stream:tt)+),
     -> { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*) => {
        $crate::__obzenflow_join_typed!(
            reference = $ref_hint,
            stream = exact,
            output = $first,
            output_contract = [$first $(, $member)*],
            ref_type = $ref_type,
            stream_type = ($($stream)+),
            name = $name,
            ref_var = $ref_var,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = []
        )
    };
    (@collect
     name = $name:literal,
     ref_var = $ref_var:ident,
     reference = $ref_hint:tt,
     ref_type = $ref_type:tt,
     stream = ($($stream:tt)+),
     -> { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* @attachments [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = $ref_hint,
            stream = exact,
            output = $first,
            output_contract = [$first $(, $member)*],
            ref_type = $ref_type,
            stream_type = ($($stream)+),
            name = $name,
            ref_var = $ref_var,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = [$($mw),*]
        )
    };

    (@collect
     name = $name:literal,
     ref_var = $ref_var:ident,
     reference = $ref_hint:tt,
     ref_type = $ref_type:tt,
     stream = ($($stream:tt)+),
     -> $out:ty => placeholder!()) => {
        $crate::__obzenflow_join_typed!(
            reference = $ref_hint,
            stream = exact,
            output = $out,
            ref_type = $ref_type,
            stream_type = ($($stream)+),
            name = $name,
            ref_var = $ref_var,
            handler = placeholder!(),
            middleware = []
        )
    };
    (@collect
     name = $name:literal,
     ref_var = $ref_var:ident,
     reference = $ref_hint:tt,
     ref_type = $ref_type:tt,
     stream = ($($stream:tt)+),
     -> $out:ty => placeholder!($msg:expr)) => {
        $crate::__obzenflow_join_typed!(
            reference = $ref_hint,
            stream = exact,
            output = $out,
            ref_type = $ref_type,
            stream_type = ($($stream)+),
            name = $name,
            ref_var = $ref_var,
            handler = placeholder!($msg),
            middleware = []
        )
    };
    (@collect
     name = $name:literal,
     ref_var = $ref_var:ident,
     reference = $ref_hint:tt,
     ref_type = $ref_type:tt,
     stream = ($($stream:tt)+),
     -> $out:ty => placeholder!() @attachments [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = $ref_hint,
            stream = exact,
            output = $out,
            ref_type = $ref_type,
            stream_type = ($($stream)+),
            name = $name,
            ref_var = $ref_var,
            handler = placeholder!(),
            middleware = [$($mw),*]
        )
    };
    (@collect
     name = $name:literal,
     ref_var = $ref_var:ident,
     reference = $ref_hint:tt,
     ref_type = $ref_type:tt,
     stream = ($($stream:tt)+),
     -> $out:ty => placeholder!($msg:expr) @attachments [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = $ref_hint,
            stream = exact,
            output = $out,
            ref_type = $ref_type,
            stream_type = ($($stream)+),
            name = $name,
            ref_var = $ref_var,
            handler = placeholder!($msg),
            middleware = [$($mw),*]
        )
    };
    (@collect
     name = $name:literal,
     ref_var = $ref_var:ident,
     reference = $ref_hint:tt,
     ref_type = $ref_type:tt,
     stream = ($($stream:tt)+),
     -> $out:ty => $handler_head:ident $(:: $handler_tail:ident)*) => {
        $crate::__obzenflow_join_typed!(
            reference = $ref_hint,
            stream = exact,
            output = $out,
            ref_type = $ref_type,
            stream_type = ($($stream)+),
            name = $name,
            ref_var = $ref_var,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = []
        )
    };
    (@collect
     name = $name:literal,
     ref_var = $ref_var:ident,
     reference = $ref_hint:tt,
     ref_type = $ref_type:tt,
     stream = ($($stream:tt)+),
     -> $out:ty => $handler_head:ident $(:: $handler_tail:ident)* @attachments [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = $ref_hint,
            stream = exact,
            output = $out,
            ref_type = $ref_type,
            stream_type = ($($stream)+),
            name = $name,
            ref_var = $ref_var,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = [$($mw),*]
        )
    };

    (@collect
     name = $name:literal,
     ref_var = $ref_var:ident,
     reference = $ref_hint:tt,
     ref_type = $ref_type:tt,
     stream = ($($stream:tt)*),
     $tok:tt
     $($rest:tt)+) => {
        $crate::__obzenflow_join_exact_stream_contract!(
            @collect
            name = $name,
            ref_var = $ref_var,
            reference = $ref_hint,
            ref_type = $ref_type,
            stream = ($($stream)* $tok),
            $($rest)+
        )
    };
    (@collect
     name = $name:literal,
     ref_var = $ref_var:ident,
     reference = $ref_hint:tt,
     ref_type = $ref_type:tt,
     stream = (),
     -> $($rest:tt)*) => {
        compile_error!("join!: expected `StreamType -> OutType => handler` after `catalog ref: Ty,`");
    };
    (@collect
     name = $name:literal,
     ref_var = $ref_var:ident,
     reference = $ref_hint:tt,
     ref_type = $ref_type:tt,
     stream = ($($stream:tt)+),
     $($rest:tt)*) => {
        compile_error!("join!: expected `-> OutType => handler` after stream type");
    };
}

/// Create a join stage descriptor.
#[macro_export]
macro_rules! __obzenflow_join_attachment_contract {
    // ── typed (binding-derived name): exact stream ──
    (catalog $ref_var:ident : $reference:ty, $($rest:tt)+) => {
        $crate::__obzenflow_join_exact_stream_contract!(
            name = "__obzenflow_binding_derived_name__",
            ref_var = $ref_var,
            reference = exact($reference),
            $($rest)+
        )
    };

    // ── typed (explicit name override): exact stream ──
    (name: $name:literal, catalog $ref_var:ident : $reference:ty, $($rest:tt)+) => {
        $crate::__obzenflow_join_exact_stream_contract!(
            name = $name,
            ref_var = $ref_var,
            reference = exact($reference),
            $($rest)+
        )
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_stage_row {
    (@sig ai $target:ident [$($sig:tt)*] uses $($rest:tt)*) => {
        compile_error!("ai!: `uses` rows were removed; the effect comes from the role's `type Effect`")
    };
    (@sig ai $target:ident [$($sig:tt)*] via $($rest:tt)*) => {
        compile_error!("ai!: `via` was removed; each stage's model comes from [ai.*] configuration")
    };
    (@sig $kind:ident $target:ident [$($sig:tt)*] uses $($rest:tt)*) => {
        compile_error!("`uses` was removed; a stage performs the effects its command's ports allow, as in `#[obzenflow::command(through = PaymentGatewayPort)]`")
    };
    (@sig $kind:ident $target:ident [$($sig:tt)*] via $($rest:tt)*) => {
        compile_error!("`via` was removed; the command holds the port it works through, and the flow builder connects a system with `perform(ChargeRequest::for_invoice).through(Port::deferred(StripeProcessor::from_env))`")
    };
    (@sig $kind:ident $target:ident [$($sig:tt)*] with {} $($rest:tt)*) => {
        compile_error!("middleware groups must be non-empty; omit `with` when no middleware is attached")
    };
    (@sig $kind:ident $target:ident [$($sig:tt)*] with { $($members:tt)* } => $($rest:tt)*) => {{
        $crate::__obzenflow_flat_attachment_group!($($members)*);
        $crate::__obzenflow_stage_row!(@group $kind $target [$($sig)*] { $($members)* } $($rest)*)
    }};
    (@sig $kind:ident $target:ident [$($sig:tt)*] with { $($members:tt)* } $($rest:tt)*) => {
        compile_error!("a stage takes one `with` clause, written as `with a` or `with { a, b }` just before `=>`")
    };
    (@sig $kind:ident $target:ident [$($sig:tt)*] with [$($members:tt)*] $($rest:tt)*) => {
        compile_error!("middleware attaches as `with a` or `with { a, b }`; square-bracket lists are not part of the grammar")
    };
    (@sig $kind:ident $target:ident [$($sig:tt)*] with $member:expr => $($rest:tt)*) => {
        $crate::__obzenflow_stage_row!(@handler $kind $target [$($sig)*] [$member] $($rest)*)
    };
    (@sig $kind:ident $target:ident [$($sig:tt)*] with $($rest:tt)*) => {
        compile_error!("a stage takes one `with` clause, written as `with a` or `with { a, b }` just before `=>`")
    };
    (@sig $kind:ident $target:ident [$($sig:tt)*] => $($rest:tt)*) => {
        $crate::__obzenflow_stage_row!(@handler $kind $target [$($sig)*] [] $($rest)*)
    };
    (@sig $kind:ident $target:ident [$($sig:tt)*] $next:tt $($rest:tt)*) => {
        $crate::__obzenflow_stage_row!(@sig $kind $target [$($sig)* $next] $($rest)*)
    };
    (@sig $kind:ident $target:ident [$($sig:tt)*]) => {
        $crate::$target!($($sig)*)
    };
    (@group $kind:ident $target:ident [$($sig:tt)*] { $($members:expr),+ $(,)? } $($rest:tt)*) => {
        $crate::__obzenflow_stage_row!(@handler $kind $target [$($sig)*] [$($members),+] $($rest)*)
    };

    (@handler $kind:ident $target:ident $sig:tt $mw:tt placeholder!() $($rest:tt)*) => {
        $crate::__obzenflow_stage_row!(@tail $target $sig $mw [placeholder!()] $($rest)*)
    };
    (@handler $kind:ident $target:ident $sig:tt $mw:tt placeholder!($message:expr) $($rest:tt)*) => {
        $crate::__obzenflow_stage_row!(@tail $target $sig $mw [placeholder!($message)] $($rest)*)
    };
    (@handler $kind:ident $target:ident $sig:tt $mw:tt handler_set!($($set:tt)*) $($rest:tt)*) => {
        compile_error!("`handler_set!` was removed; build each system's port with `Port::to` or `Port::deferred` and choose between them in code")
    };
    (@handler $kind:ident $target:ident $sig:tt $mw:tt $head:ident $($rest:tt)*) => {
        $crate::__obzenflow_stage_row!(@path $kind $target $sig $mw [$head] $($rest)*)
    };
    (@handler $kind:ident $target:ident $sig:tt $mw:tt $($rest:tt)*) => {
        compile_error!("a stage handler is a name; build values with `let` above `flow!`")
    };
    (@path $kind:ident $target:ident $sig:tt $mw:tt [$($path:tt)*] :: $next:ident $($rest:tt)*) => {
        $crate::__obzenflow_stage_row!(@path $kind $target $sig $mw [$($path)* :: $next] $($rest)*)
    };
    (@path $kind:ident $target:ident $sig:tt $mw:tt [$($path:tt)*] $($rest:tt)*) => {
        $crate::__obzenflow_stage_row!(@tail $target $sig $mw [$($path)*] $($rest)*)
    };

    (@tail $target:ident $sig:tt $mw:tt $handler:tt . system $($rest:tt)*) => {
        compile_error!("`.system(…)` was removed; the command holds the port it works through in a `#[port]` field, and `fx.call(request)` performs the request through it")
    };
    (@tail $target:ident $sig:tt $mw:tt $handler:tt . exactly_once $($rest:tt)*) => {
        $crate::__obzenflow_stage_row!(@guarantee $target)
    };
    (@tail $target:ident $sig:tt $mw:tt $handler:tt . at_least_once $($rest:tt)*) => {
        $crate::__obzenflow_stage_row!(@guarantee $target)
    };
    (@tail $target:ident $sig:tt $mw:tt $handler:tt . at_most_once $($rest:tt)*) => {
        $crate::__obzenflow_stage_row!(@guarantee $target)
    };
    (@guarantee __obzenflow_ai_attachment_contract) => {
        compile_error!("the AI role selects an effect with a built-in guarantee; remove the stage suffix")
    };
    (@guarantee $target:ident) => {
        compile_error!("delivery guarantees belong on the port method in `#[effect(...)]`; remove the stage suffix")
    };
    (@tail $target:ident $sig:tt $mw:tt $handler:tt . transactional $($rest:tt)*) => {
        compile_error!("`.transactional` was removed; a method taking an `EffectCommitHandle` declares `exactly_once`")
    };
    (@tail $target:ident $sig:tt $mw:tt $handler:tt . idempotent () $($rest:tt)*) => {
        compile_error!("`.idempotent()` was removed; a sink's port method states its guarantee in `#[effect]`, and a sink row takes no decoration")
    };
    (@tail $target:ident $sig:tt $mw:tt $handler:tt . non_idempotent () $($rest:tt)*) => {
        compile_error!("`.non_idempotent()` was removed; a sink's port method states its guarantee in `#[effect]`, and a sink row takes no decoration")
    };
    (@tail $target:ident $sig:tt $mw:tt $handler:tt with $($rest:tt)*) => {
        compile_error!("`with` comes before `=>`; write `with …` after the stage's types")
    };
    (@tail $target:ident $sig:tt $mw:tt $handler:tt , delivery : $($rest:tt)*) => {
        compile_error!("`delivery:` was removed; a sink's port method states its guarantee in `#[effect]`")
    };
    (@tail $target:ident $sig:tt $mw:tt $handler:tt , backpressure : $($rest:tt)*) => {
        compile_error!("`backpressure:` was removed; set backpressure in configuration, under `[runtime.backpressure.flow]` or `[runtime.backpressure.stages.<stage>]`")
    };
    (@tail $target:ident $sig:tt $mw:tt $handler:tt , emit_interval $($rest:tt)*) => {
        compile_error!("`emit_interval` was removed from stage rows; a stateful handler declares its interval with `emit_interval_hint()`")
    };
    (@tail $target:ident [$($sig:tt)*] [$($mw:tt)*] [$($handler:tt)*] $(,)?) => {
        $crate::$target!($($sig)* => $($handler)* @attachments [$($mw)*])
    };
    (@tail $target:ident $sig:tt $mw:tt $handler:tt $($rest:tt)+) => {
        compile_error!("a stage handler is a name; build values with `let` above `flow!`")
    };
}

#[macro_export]
macro_rules! effectful_stateful {
    ($($tokens:tt)*) => { $crate::__obzenflow_stage_row!(@sig effectful __obzenflow_effectful_stateful_attachment_contract [] $($tokens)*) };
}

/// Declares a typed effectful transform. The handler's effect set declares the
/// effects the stage performs, and middleware built with `.for_effect::<E>()`
/// guards or watches only `E`.
///
/// ```
/// # use async_trait::async_trait;
/// # use obzenflow_adapters::middleware::{rate_limit, MiddlewareFactory};
/// # use obzenflow_runtime::effects::{EffectError, Effects, StageCompletion, Port};
/// # use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Fact;
/// # impl obzenflow_core::TypedPayload for Fact { const EVENT_TYPE: &'static str = "group.fact"; }
/// # #[derive(Clone, Debug, serde::Serialize)]
/// # struct FirstRequest;
/// # #[derive(Clone, Debug, serde::Serialize)]
/// # struct SecondRequest;
/// # #[obzenflow_runtime::effects::port]
/// # trait GroupPort: Send + Sync {
/// #     #[effect(type = "group.first", exactly_once)]
/// #     fn first(&self, request: &FirstRequest) -> Result<Fact, EffectError>;
/// #     #[effect(type = "group.second", exactly_once)]
/// #     fn second(&self, request: &SecondRequest) -> Result<Fact, EffectError>;
/// # }
/// # #[obzenflow_runtime::effects::command]
/// # #[derive(Clone, Debug)]
/// # struct Handler {
/// #     #[port]
/// #     group: Port<dyn GroupPort>,
/// # }
/// # #[async_trait]
/// # impl EffectfulTransformHandler for Handler {
/// #     type Input = Fact;
/// #     type Output = Fact;
/// #     async fn process(&self, _: Fact, _: &mut Effects<Fact, Self::AllowedEffects>) -> Result<StageCompletion<Fact>, HandlerError> { unimplemented!() }
/// # }
/// # fn build(handler: Handler) {
/// let first_limit = rate_limit(10.0).for_effect::<FirstRequest>();
/// let stage = obzenflow_dsl::effectful_transform!(
///     Fact -> Fact with first_limit => handler
/// );
/// # }
/// ```
///
/// The port method supplies the guarantee. A delivery suffix is a compile error.
///
/// ```compile_fail
/// # use obzenflow_runtime::effects::{perform, EffectError, Port};
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Fact;
/// # impl obzenflow_core::TypedPayload for Fact { const EVENT_TYPE: &'static str = "row.fact"; }
/// # #[derive(Clone, Debug, serde::Serialize)]
/// # struct Notice;
/// # #[obzenflow_runtime::effects::port]
/// # trait NoticePort {
/// #     #[effect(type = "row.notice", at_most_once)]
/// #     fn notify(&self, _: &Notice) -> Result<Fact, EffectError>;
/// # }
/// # fn stage(port: Port<dyn NoticePort>) {
/// let notify = perform(|_: &Fact| Notice).through(port);
/// let stage = obzenflow_dsl::effectful_transform!(Fact -> Fact => notify.at_most_once::<Notice>());
/// # }
/// ```
///
/// Middleware groups are flat; a nested group does not compile.
///
/// ```compile_fail
/// # use async_trait::async_trait;
/// # use obzenflow_adapters::middleware::rate_limit;
/// # use obzenflow_runtime::effects::{EffectError, Effects, StageCompletion, Port};
/// # use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Fact;
/// # impl obzenflow_core::TypedPayload for Fact { const EVENT_TYPE: &'static str = "group.fact"; }
/// # #[derive(Clone, Debug, serde::Serialize)]
/// # struct FirstRequest;
/// # #[obzenflow_runtime::effects::port]
/// # trait GroupPort: Send + Sync {
/// #     #[effect(type = "group.first", exactly_once)]
/// #     fn first(&self, request: &FirstRequest) -> Result<Fact, EffectError>;
/// # }
/// # #[obzenflow_runtime::effects::command]
/// # #[derive(Clone, Debug)]
/// # struct Handler {
/// #     #[port]
/// #     group: Port<dyn GroupPort>,
/// # }
/// # #[async_trait]
/// # impl EffectfulTransformHandler for Handler {
/// #     type Input = Fact;
/// #     type Output = Fact;
/// #     async fn process(&self, _: Fact, _: &mut Effects<Fact, Self::AllowedEffects>) -> Result<StageCompletion<Fact>, HandlerError> { unimplemented!() }
/// # }
/// # fn build(handler: Handler) {
/// let stage = obzenflow_dsl::effectful_transform!(
///     Fact -> Fact with { { rate_limit(10.0) } } => handler
/// );
/// # }
/// ```
///
/// A command declares the port it works through, so the row names only the
/// command and `fx.call` takes only the request.
///
/// ```
/// # use async_trait::async_trait;
/// # use obzenflow_core::TypedPayload;
/// # use obzenflow_runtime::effects::{EffectError, Effects, StageCompletion, Port};
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Order;
/// # impl TypedPayload for Order { const EVENT_TYPE: &'static str = "row_doc.order"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Charged;
/// # impl TypedPayload for Charged { const EVENT_TYPE: &'static str = "row_doc.charged"; }
/// # #[obzenflow_runtime::effects::port]
/// # trait CardPort: Send + Sync {
/// #     #[effect(type = "row_doc.charge_card", exactly_once)]
/// #     fn charge(&self, request: &ChargeCardRequest) -> Result<Charged, EffectError>;
/// # }
/// # struct Card;
/// # #[obzenflow_runtime::effects::system("card")]
/// # impl CardPort for Card { fn charge(&self, _: &ChargeCardRequest) -> Result<Charged, EffectError> { Ok(Charged) } }
/// # #[derive(Clone, Debug, Serialize)]
/// # struct ChargeCardRequest;
/// #[obzenflow_runtime::effects::command(through = CardPort)]
/// #[derive(Clone, Debug)]
/// struct Checkout;
///
/// #[async_trait]
/// impl EffectfulTransformHandler for Checkout {
///     type Input = Order;
///     type Output = Charged;
///
///     async fn process(
///         &self,
///         _: Order,
///         fx: &mut Effects<Self::Output, Self::AllowedEffects>,
///     ) -> Result<StageCompletion<Self::Output>, HandlerError> {
///         fx.call(ChargeCardRequest).await?;
///         Ok(fx.complete()?)
///     }
/// }
///
/// # fn main() {
/// let checkout = Checkout::through(Port::to(Card));
/// let stage = obzenflow_dsl::effectful_transform!(Order -> Charged => checkout);
/// # }
/// ```
///
/// A stage that performs one request per input needs no type of its own. Its
/// output is the request's outcome facts.
///
/// ```
/// # use obzenflow_core::TypedPayload;
/// # use obzenflow_runtime::effects::{perform, EffectError, Port};
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Order;
/// # impl TypedPayload for Order { const EVENT_TYPE: &'static str = "row_doc.order"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Charged;
/// # impl TypedPayload for Charged { const EVENT_TYPE: &'static str = "row_doc.charged"; }
/// # #[derive(Clone, Debug, Serialize)]
/// # struct ChargeCardRequest;
/// # impl ChargeCardRequest { fn for_order(_: &Order) -> Self { Self } }
/// # #[obzenflow_runtime::effects::port]
/// # trait CardPort: Send + Sync {
/// #     #[effect(type = "row_doc.charge_card", exactly_once)]
/// #     fn charge(&self, request: &ChargeCardRequest) -> Result<Charged, EffectError>;
/// # }
/// # struct Card;
/// # #[obzenflow_runtime::effects::system("card")]
/// # impl CardPort for Card { fn charge(&self, _: &ChargeCardRequest) -> Result<Charged, EffectError> { Ok(Charged) } }
/// let charge = perform(ChargeCardRequest::for_order).through(Port::to(Card));
/// let stage = obzenflow_dsl::effectful_transform!(Order -> Charged => charge);
/// ```
///
/// A `perform` without its port is not a command, so a row that names one does
/// not compile:
///
/// ```compile_fail,E0277
/// # use obzenflow_core::TypedPayload;
/// # use obzenflow_runtime::effects::{perform, EffectError};
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Order;
/// # impl TypedPayload for Order { const EVENT_TYPE: &'static str = "row_doc.order"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Charged;
/// # impl TypedPayload for Charged { const EVENT_TYPE: &'static str = "row_doc.charged"; }
/// # #[derive(Clone, Debug, Serialize)]
/// # struct ChargeCardRequest;
/// # impl ChargeCardRequest { fn for_order(_: &Order) -> Self { Self } }
/// # #[obzenflow_runtime::effects::port]
/// # trait CardPort: Send + Sync {
/// #     #[effect(type = "row_doc.charge_card", exactly_once)]
/// #     fn charge(&self, request: &ChargeCardRequest) -> Result<Charged, EffectError>;
/// # }
/// let charge = perform(ChargeCardRequest::for_order);
/// let stage = obzenflow_dsl::effectful_transform!(Order -> Charged => charge);
/// ```
///
/// A field left unmarked holds no port for the stage and allows no effect,
/// so a call only that field could answer does not compile:
///
/// ```compile_fail,E0277
/// # use async_trait::async_trait;
/// # use obzenflow_core::TypedPayload;
/// # use obzenflow_runtime::effects::{EffectError, Effects, StageCompletion, Port};
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Order;
/// # impl TypedPayload for Order { const EVENT_TYPE: &'static str = "row_doc.order"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Charged;
/// # impl TypedPayload for Charged { const EVENT_TYPE: &'static str = "row_doc.charged"; }
/// # #[obzenflow_runtime::effects::port]
/// # trait CardPort: Send + Sync {
/// #     #[effect(type = "row_doc.charge_card", exactly_once)]
/// #     fn charge(&self, request: &ChargeCardRequest) -> Result<Charged, EffectError>;
/// # }
/// # #[derive(Clone, Debug, Serialize)]
/// # struct ChargeCardRequest;
/// #[obzenflow_runtime::effects::command]
/// #[derive(Clone, Debug)]
/// struct Checkout {
///     card: Port<dyn CardPort>,
/// }
/// # #[async_trait]
/// # impl EffectfulTransformHandler for Checkout {
/// #     type Input = Order;
/// #     type Output = Charged;
/// #     async fn process(&self, _: Order, fx: &mut Effects<Self::Output, Self::AllowedEffects>) -> Result<StageCompletion<Self::Output>, HandlerError> {
/// #         fx.call(ChargeCardRequest).await?;
/// #         Ok(fx.complete()?)
/// #     }
/// # }
/// # fn build(card: Port<dyn CardPort>) {
/// let checkout = Checkout { card };
/// let stage = obzenflow_dsl::effectful_transform!(Order -> Charged => checkout);
/// # }
/// ```
///
/// Neither does a `#[port]` field whose port declares no effect:
///
/// ```compile_fail,E0277
/// # use async_trait::async_trait;
/// # use obzenflow_core::TypedPayload;
/// # use obzenflow_runtime::effects::{EffectError, Effects, StageCompletion, Port};
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Order;
/// # impl TypedPayload for Order { const EVENT_TYPE: &'static str = "row_doc.order"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Charged;
/// # impl TypedPayload for Charged { const EVENT_TYPE: &'static str = "row_doc.charged"; }
/// # #[obzenflow_runtime::effects::port]
/// # trait CardPort: Send + Sync {
/// #     #[effect(type = "row_doc.charge_card", exactly_once)]
/// #     fn charge(&self, request: &ChargeCardRequest) -> Result<Charged, EffectError>;
/// # }
/// # #[obzenflow_runtime::effects::port]
/// # trait LedgerPort: Send + Sync {}
/// # #[derive(Clone, Debug, Serialize)]
/// # struct ChargeCardRequest;
/// #[obzenflow_runtime::effects::command]
/// #[derive(Clone, Debug)]
/// struct Checkout {
///     #[port]
///     card: Port<dyn CardPort>,
///     #[port]
///     ledger: Port<dyn LedgerPort>,
/// }
/// # #[async_trait]
/// # impl EffectfulTransformHandler for Checkout {
/// #     type Input = Order;
/// #     type Output = Charged;
/// #     async fn process(&self, _: Order, fx: &mut Effects<Self::Output, Self::AllowedEffects>) -> Result<StageCompletion<Self::Output>, HandlerError> {
/// #         fx.call(ChargeCardRequest).await?;
/// #         Ok(fx.complete()?)
/// #     }
/// # }
/// # fn build(card: Port<dyn CardPort>, ledger: Port<dyn LedgerPort>) {
/// let checkout = Checkout { card, ledger };
/// let stage = obzenflow_dsl::effectful_transform!(Order -> Charged => checkout);
/// # }
/// ```
///
/// Nor do two `#[port]` fields for one port type:
///
/// ```compile_fail
/// # use async_trait::async_trait;
/// # use obzenflow_core::TypedPayload;
/// # use obzenflow_runtime::effects::{EffectError, Effects, StageCompletion, Port};
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Order;
/// # impl TypedPayload for Order { const EVENT_TYPE: &'static str = "row_doc.order"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Charged;
/// # impl TypedPayload for Charged { const EVENT_TYPE: &'static str = "row_doc.charged"; }
/// # #[obzenflow_runtime::effects::port]
/// # trait CardPort: Send + Sync {
/// #     #[effect(type = "row_doc.charge_card", exactly_once)]
/// #     fn charge(&self, request: &ChargeCardRequest) -> Result<Charged, EffectError>;
/// # }
/// # #[derive(Clone, Debug, Serialize)]
/// # struct ChargeCardRequest;
/// #[obzenflow_runtime::effects::command]
/// #[derive(Clone, Debug)]
/// struct Checkout {
///     #[port]
///     card: Port<dyn CardPort>,
///     #[port]
///     spare_card: Port<dyn CardPort>,
/// }
/// # #[async_trait]
/// # impl EffectfulTransformHandler for Checkout {
/// #     type Input = Order;
/// #     type Output = Charged;
/// #     async fn process(&self, _: Order, fx: &mut Effects<Self::Output, Self::AllowedEffects>) -> Result<StageCompletion<Self::Output>, HandlerError> {
/// #         fx.call(ChargeCardRequest).await?;
/// #         Ok(fx.complete()?)
/// #     }
/// # }
/// # fn build(card: Port<dyn CardPort>, spare_card: Port<dyn CardPort>) {
/// let checkout = Checkout { card, spare_card };
/// let stage = obzenflow_dsl::effectful_transform!(Order -> Charged => checkout);
/// # }
/// ```
///
/// The row never names its effects or systems: `uses`, `via` and
/// `.system(…)` were removed, `with` comes before `=>`, and the handler slot
/// takes a name, not construction.
///
/// ```compile_fail
/// # use async_trait::async_trait;
/// # use obzenflow_core::TypedPayload;
/// # use obzenflow_runtime::effects::{EffectError, Effects, StageCompletion, Port};
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Order;
/// # impl TypedPayload for Order { const EVENT_TYPE: &'static str = "row_doc.order"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Charged;
/// # impl TypedPayload for Charged { const EVENT_TYPE: &'static str = "row_doc.charged"; }
/// # #[obzenflow_runtime::effects::port]
/// # trait CardPort: Send + Sync {
/// #     #[effect(type = "row_doc.charge_card", exactly_once)]
/// #     fn charge(&self, request: &ChargeCardRequest) -> Result<Charged, EffectError>;
/// # }
/// # #[derive(Clone, Debug, Serialize)]
/// # struct ChargeCardRequest;
/// # #[obzenflow_runtime::effects::command]
/// # #[derive(Clone, Debug)]
/// # struct Checkout {
/// #     #[port]
/// #     card: Port<dyn CardPort>,
/// # }
/// # #[async_trait]
/// # impl EffectfulTransformHandler for Checkout {
/// #     type Input = Order;
/// #     type Output = Charged;
/// #     async fn process(&self, _: Order, fx: &mut Effects<Self::Output, Self::AllowedEffects>) -> Result<StageCompletion<Self::Output>, HandlerError> {
/// #         fx.call(ChargeCardRequest).await?;
/// #         Ok(fx.complete()?)
/// #     }
/// # }
/// # fn build(checkout: Checkout) {
/// let stage = obzenflow_dsl::effectful_transform!(Order -> Charged uses ChargeCardRequest => checkout);
/// # }
/// ```
///
/// ```compile_fail
/// # use async_trait::async_trait;
/// # use obzenflow_core::TypedPayload;
/// # use obzenflow_runtime::effects::{EffectError, Effects, StageCompletion, Port};
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Order;
/// # impl TypedPayload for Order { const EVENT_TYPE: &'static str = "row_doc.order"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Charged;
/// # impl TypedPayload for Charged { const EVENT_TYPE: &'static str = "row_doc.charged"; }
/// # #[obzenflow_runtime::effects::port]
/// # trait CardPort: Send + Sync {
/// #     #[effect(type = "row_doc.charge_card", exactly_once)]
/// #     fn charge(&self, request: &ChargeCardRequest) -> Result<Charged, EffectError>;
/// # }
/// # #[derive(Clone, Debug, Serialize)]
/// # struct ChargeCardRequest;
/// # #[obzenflow_runtime::effects::command]
/// # #[derive(Clone, Debug)]
/// # struct Checkout {
/// #     #[port]
/// #     card: Port<dyn CardPort>,
/// # }
/// # #[async_trait]
/// # impl EffectfulTransformHandler for Checkout {
/// #     type Input = Order;
/// #     type Output = Charged;
/// #     async fn process(&self, _: Order, fx: &mut Effects<Self::Output, Self::AllowedEffects>) -> Result<StageCompletion<Self::Output>, HandlerError> {
/// #         fx.call(ChargeCardRequest).await?;
/// #         Ok(fx.complete()?)
/// #     }
/// # }
/// # fn build(checkout: Checkout, card: Port<dyn CardPort>) {
/// let stage = obzenflow_dsl::effectful_transform!(Order -> Charged via card => checkout);
/// # }
/// ```
///
/// ```compile_fail
/// # use async_trait::async_trait;
/// # use obzenflow_core::TypedPayload;
/// # use obzenflow_runtime::effects::{EffectError, Effects, StageCompletion, Port};
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Order;
/// # impl TypedPayload for Order { const EVENT_TYPE: &'static str = "row_doc.order"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Charged;
/// # impl TypedPayload for Charged { const EVENT_TYPE: &'static str = "row_doc.charged"; }
/// # #[obzenflow_runtime::effects::port]
/// # trait CardPort: Send + Sync {
/// #     #[effect(type = "row_doc.charge_card", exactly_once)]
/// #     fn charge(&self, request: &ChargeCardRequest) -> Result<Charged, EffectError>;
/// # }
/// # #[derive(Clone, Debug, Serialize)]
/// # struct ChargeCardRequest;
/// # #[obzenflow_runtime::effects::command]
/// # #[derive(Clone, Debug)]
/// # struct Checkout {
/// #     #[port]
/// #     card: Port<dyn CardPort>,
/// # }
/// # #[async_trait]
/// # impl EffectfulTransformHandler for Checkout {
/// #     type Input = Order;
/// #     type Output = Charged;
/// #     async fn process(&self, _: Order, fx: &mut Effects<Self::Output, Self::AllowedEffects>) -> Result<StageCompletion<Self::Output>, HandlerError> {
/// #         fx.call(ChargeCardRequest).await?;
/// #         Ok(fx.complete()?)
/// #     }
/// # }
/// # fn build(checkout: Checkout, card: Port<dyn CardPort>) {
/// let stage = obzenflow_dsl::effectful_transform!(Order -> Charged => checkout.system(card));
/// # }
/// ```
///
/// An effect method taking a commit handle declares `exactly_once` on the port:
///
/// ```compile_fail
/// # use async_trait::async_trait;
/// # use obzenflow_core::TypedPayload;
/// # use obzenflow_runtime::effects::{EffectCommitHandle, EffectError, Effects, StageCompletion, Port};
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Order;
/// # impl TypedPayload for Order { const EVENT_TYPE: &'static str = "row_doc.order"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Posted;
/// # impl TypedPayload for Posted { const EVENT_TYPE: &'static str = "row_doc.posted"; }
/// # #[derive(Clone, Debug, Serialize)]
/// # struct LedgerPosting;
/// # #[obzenflow_runtime::effects::port]
/// # trait LedgerPort: Send + Sync {
/// #     #[effect(type = "row_doc.post", exactly_once)]
/// #     fn post(&self, request: &LedgerPosting, tx: EffectCommitHandle<Posted>) -> Result<Posted, EffectError>;
/// # }
/// # #[obzenflow_runtime::effects::command]
/// # #[derive(Clone, Debug)]
/// # struct Post {
/// #     #[port]
/// #     ledger: Port<dyn LedgerPort>,
/// # }
/// # #[async_trait]
/// # impl EffectfulTransformHandler for Post {
/// #     type Input = Order;
/// #     type Output = Posted;
/// #     async fn process(&self, _: Order, fx: &mut Effects<Self::Output, Self::AllowedEffects>) -> Result<StageCompletion<Self::Output>, HandlerError> {
/// #         fx.call(LedgerPosting).await?;
/// #         Ok(fx.complete()?)
/// #     }
/// # }
/// # fn build(post: Post) {
/// let stage = obzenflow_dsl::effectful_transform!(Order -> Posted => post.transactional::<LedgerPosting>());
/// # }
/// ```
///
/// ```compile_fail
/// # use async_trait::async_trait;
/// # use obzenflow_adapters::middleware::retry;
/// # use obzenflow_core::TypedPayload;
/// # use obzenflow_runtime::effects::{EffectError, Effects, StageCompletion, Port};
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Order;
/// # impl TypedPayload for Order { const EVENT_TYPE: &'static str = "row_doc.order"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Charged;
/// # impl TypedPayload for Charged { const EVENT_TYPE: &'static str = "row_doc.charged"; }
/// # #[obzenflow_runtime::effects::port]
/// # trait CardPort: Send + Sync {
/// #     #[effect(type = "row_doc.charge_card", exactly_once)]
/// #     fn charge(&self, request: &ChargeCardRequest) -> Result<Charged, EffectError>;
/// # }
/// # #[derive(Clone, Debug, Serialize)]
/// # struct ChargeCardRequest;
/// # #[obzenflow_runtime::effects::command]
/// # #[derive(Clone, Debug)]
/// # struct Checkout {
/// #     #[port]
/// #     card: Port<dyn CardPort>,
/// # }
/// # #[async_trait]
/// # impl EffectfulTransformHandler for Checkout {
/// #     type Input = Order;
/// #     type Output = Charged;
/// #     async fn process(&self, _: Order, fx: &mut Effects<Self::Output, Self::AllowedEffects>) -> Result<StageCompletion<Self::Output>, HandlerError> {
/// #         fx.call(ChargeCardRequest).await?;
/// #         Ok(fx.complete()?)
/// #     }
/// # }
/// # fn build(checkout: Checkout) {
/// let stage = obzenflow_dsl::effectful_transform!(Order -> Charged => checkout with retry());
/// # }
/// ```
///
/// ```compile_fail
/// # use async_trait::async_trait;
/// # use obzenflow_core::TypedPayload;
/// # use obzenflow_runtime::effects::{EffectError, Effects, StageCompletion, Port};
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Order;
/// # impl TypedPayload for Order { const EVENT_TYPE: &'static str = "row_doc.order"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Charged;
/// # impl TypedPayload for Charged { const EVENT_TYPE: &'static str = "row_doc.charged"; }
/// # #[obzenflow_runtime::effects::port]
/// # trait CardPort: Send + Sync {
/// #     #[effect(type = "row_doc.charge_card", exactly_once)]
/// #     fn charge(&self, request: &ChargeCardRequest) -> Result<Charged, EffectError>;
/// # }
/// # #[derive(Clone, Debug, Serialize)]
/// # struct ChargeCardRequest;
/// # #[obzenflow_runtime::effects::command]
/// # #[derive(Clone, Debug)]
/// # struct Checkout {
/// #     #[port]
/// #     card: Port<dyn CardPort>,
/// # }
/// # impl Checkout {
/// #     fn new(card: Port<dyn CardPort>) -> Self { Self { card } }
/// # }
/// # #[async_trait]
/// # impl EffectfulTransformHandler for Checkout {
/// #     type Input = Order;
/// #     type Output = Charged;
/// #     async fn process(&self, _: Order, fx: &mut Effects<Self::Output, Self::AllowedEffects>) -> Result<StageCompletion<Self::Output>, HandlerError> {
/// #         fx.call(ChargeCardRequest).await?;
/// #         Ok(fx.complete()?)
/// #     }
/// # }
/// # fn build(card: Port<dyn CardPort>) {
/// let stage = obzenflow_dsl::effectful_transform!(Order -> Charged => Checkout::new(card));
/// # }
/// ```
#[macro_export]
macro_rules! effectful_transform {
    ($($tokens:tt)*) => { $crate::__obzenflow_stage_row!(@sig effectful __obzenflow_effectful_transform_attachment_contract [] $($tokens)*) };
}

/// Declares an AI stage over a handler built with `ai::infer` or `ai::map`.
/// The row's arrow equals the handler's input and output, and middleware is
/// optional.
///
/// ```
/// # use obzenflow_adapters::ai::{self, ChatCompletion};
/// # use obzenflow_adapters::middleware::circuit_breaker;
/// # use obzenflow_core::ai::{AiOutcome, ChatCompletionReply, ChatRequestSpec, Chunk, Partials};
/// # use obzenflow_core::TypedPayload;
/// # use obzenflow_runtime::ai::AiRole;
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Story { title: String }
/// # impl TypedPayload for Story { const EVENT_TYPE: &'static str = "ai_doc.story"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Summary { text: String }
/// # impl TypedPayload for Summary { const EVENT_TYPE: &'static str = "ai_doc.summary"; }
/// # #[derive(Clone)]
/// # struct Digest;
/// # impl AiRole<Chunk<Story>> for Digest {
/// #     type Effect = ChatCompletion;
/// #     type Out = Summary;
/// #     fn prepare(&self, chunk: &Chunk<Story>) -> Result<ChatRequestSpec, HandlerError> {
/// #         Ok(ChatRequestSpec::new("Summarize.", format!("{} stories", chunk.len())))
/// #     }
/// #     fn interpret(&self, _: Chunk<Story>, call: AiOutcome<ChatCompletionReply>) -> Result<Summary, HandlerError> {
/// #         Ok(Summary { text: call.replied()?.text().to_string() })
/// #     }
/// # }
/// # impl AiRole<Partials<Story, Summary>> for Digest {
/// #     type Effect = ChatCompletion;
/// #     type Out = Summary;
/// #     fn prepare(&self, input: &Partials<Story, Summary>) -> Result<ChatRequestSpec, HandlerError> {
/// #         Ok(ChatRequestSpec::new("Combine.", format!("{} summaries", input.partials().len())))
/// #     }
/// #     fn interpret(&self, _: Partials<Story, Summary>, call: AiOutcome<ChatCompletionReply>) -> Result<Summary, HandlerError> {
/// #         Ok(Summary { text: call.replied()?.text().to_string() })
/// #     }
/// # }
/// # impl AiRole<Story> for Digest {
/// #     type Effect = ChatCompletion;
/// #     type Out = Summary;
/// #     fn prepare(&self, story: &Story) -> Result<ChatRequestSpec, HandlerError> {
/// #         Ok(ChatRequestSpec::new("Summarize.", story.title.clone()))
/// #     }
/// #     fn interpret(&self, _: Story, call: AiOutcome<ChatCompletionReply>) -> Result<Summary, HandlerError> {
/// #         Ok(Summary { text: call.replied()?.text().to_string() })
/// #     }
/// # }
/// let digest = ai::map(Digest).chunks(10).reduce(Digest).on_eof();
/// let ai_breaker = circuit_breaker().consecutive_failures(5);
/// let digest_stage = obzenflow_dsl::ai!(Story -> Summary with ai_breaker => digest);
///
/// let brief = ai::infer(Digest);
/// let brief_stage = obzenflow_dsl::ai!(Story -> Summary => brief);
/// ```
///
/// A map-reduce names when its windows reduce.
///
/// ```compile_fail,E0277
/// # use obzenflow_adapters::ai::{self, ChatCompletion};
/// # use obzenflow_core::ai::{AiOutcome, ChatCompletionReply, ChatRequestSpec, Chunk, Partials};
/// # use obzenflow_core::TypedPayload;
/// # use obzenflow_runtime::ai::AiRole;
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Story { title: String }
/// # impl TypedPayload for Story { const EVENT_TYPE: &'static str = "ai_doc.story"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Summary { text: String }
/// # impl TypedPayload for Summary { const EVENT_TYPE: &'static str = "ai_doc.summary"; }
/// # #[derive(Clone)]
/// # struct Digest;
/// # impl AiRole<Chunk<Story>> for Digest {
/// #     type Effect = ChatCompletion;
/// #     type Out = Summary;
/// #     fn prepare(&self, chunk: &Chunk<Story>) -> Result<ChatRequestSpec, HandlerError> {
/// #         Ok(ChatRequestSpec::new("Summarize.", format!("{} stories", chunk.len())))
/// #     }
/// #     fn interpret(&self, _: Chunk<Story>, call: AiOutcome<ChatCompletionReply>) -> Result<Summary, HandlerError> {
/// #         Ok(Summary { text: call.replied()?.text().to_string() })
/// #     }
/// # }
/// # impl AiRole<Partials<Story, Summary>> for Digest {
/// #     type Effect = ChatCompletion;
/// #     type Out = Summary;
/// #     fn prepare(&self, input: &Partials<Story, Summary>) -> Result<ChatRequestSpec, HandlerError> {
/// #         Ok(ChatRequestSpec::new("Combine.", format!("{} summaries", input.partials().len())))
/// #     }
/// #     fn interpret(&self, _: Partials<Story, Summary>, call: AiOutcome<ChatCompletionReply>) -> Result<Summary, HandlerError> {
/// #         Ok(Summary { text: call.replied()?.text().to_string() })
/// #     }
/// # }
/// let digest = ai::map(Digest).chunks(10).reduce(Digest);
/// let digest_stage = obzenflow_dsl::ai!(Story -> Summary => digest);
/// ```
///
/// `ai::map` bounds its groups with `.chunks(n)`.
///
/// ```compile_fail,E0277
/// # use obzenflow_adapters::ai::{self, ChatCompletion};
/// # use obzenflow_core::ai::{AiOutcome, ChatCompletionReply, ChatRequestSpec, Chunk};
/// # use obzenflow_core::TypedPayload;
/// # use obzenflow_runtime::ai::AiRole;
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Story { title: String }
/// # impl TypedPayload for Story { const EVENT_TYPE: &'static str = "ai_doc.story"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Summary { text: String }
/// # impl TypedPayload for Summary { const EVENT_TYPE: &'static str = "ai_doc.summary"; }
/// # #[derive(Clone)]
/// # struct Digest;
/// # impl AiRole<Chunk<Story>> for Digest {
/// #     type Effect = ChatCompletion;
/// #     type Out = Summary;
/// #     fn prepare(&self, chunk: &Chunk<Story>) -> Result<ChatRequestSpec, HandlerError> {
/// #         Ok(ChatRequestSpec::new("Summarize.", format!("{} stories", chunk.len())))
/// #     }
/// #     fn interpret(&self, _: Chunk<Story>, call: AiOutcome<ChatCompletionReply>) -> Result<Summary, HandlerError> {
/// #         Ok(Summary { text: call.replied()?.text().to_string() })
/// #     }
/// # }
/// let digest = ai::map(Digest);
/// let digest_stage = obzenflow_dsl::ai!(Story -> Summary => digest);
/// ```
///
/// The arrow states the handler's input and output.
///
/// ```compile_fail,E0277
/// # use obzenflow_adapters::ai::{self, ChatCompletion};
/// # use obzenflow_core::ai::{AiOutcome, ChatCompletionReply, ChatRequestSpec};
/// # use obzenflow_core::TypedPayload;
/// # use obzenflow_runtime::ai::AiRole;
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Story { title: String }
/// # impl TypedPayload for Story { const EVENT_TYPE: &'static str = "ai_doc.story"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Summary { text: String }
/// # impl TypedPayload for Summary { const EVENT_TYPE: &'static str = "ai_doc.summary"; }
/// # #[derive(Clone)]
/// # struct Brief;
/// # impl AiRole<Story> for Brief {
/// #     type Effect = ChatCompletion;
/// #     type Out = Summary;
/// #     fn prepare(&self, story: &Story) -> Result<ChatRequestSpec, HandlerError> {
/// #         Ok(ChatRequestSpec::new("Summarize.", story.title.clone()))
/// #     }
/// #     fn interpret(&self, _: Story, call: AiOutcome<ChatCompletionReply>) -> Result<Summary, HandlerError> {
/// #         Ok(Summary { text: call.replied()?.text().to_string() })
/// #     }
/// # }
/// let brief = ai::infer(Brief);
/// let brief_stage = obzenflow_dsl::ai!(Story -> Story => brief);
/// ```
///
/// A key groups items before they are chunked.
///
/// ```compile_fail,E0277
/// # use obzenflow_adapters::ai::{self, ChatCompletion};
/// # use obzenflow_core::ai::{AiOutcome, ChatCompletionReply, ChatRequestSpec, Chunk};
/// # use obzenflow_core::TypedPayload;
/// # use obzenflow_runtime::ai::AiRole;
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Story { title: String }
/// # impl TypedPayload for Story { const EVENT_TYPE: &'static str = "ai_doc.story"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Summary { text: String }
/// # impl TypedPayload for Summary { const EVENT_TYPE: &'static str = "ai_doc.summary"; }
/// # #[derive(Clone)]
/// # struct Digest;
/// # impl AiRole<Chunk<Story>> for Digest {
/// #     type Effect = ChatCompletion;
/// #     type Out = Summary;
/// #     fn prepare(&self, chunk: &Chunk<Story>) -> Result<ChatRequestSpec, HandlerError> {
/// #         Ok(ChatRequestSpec::new("Summarize.", format!("{} stories", chunk.len())))
/// #     }
/// #     fn interpret(&self, _: Chunk<Story>, call: AiOutcome<ChatCompletionReply>) -> Result<Summary, HandlerError> {
/// #         Ok(Summary { text: call.replied()?.text().to_string() })
/// #     }
/// # }
/// let digest = ai::map(Digest).chunks(10).key_by(|story: &Story| story.title.clone());
/// ```
///
/// A key reads the items the stage takes.
///
/// ```compile_fail,E0631
/// # use obzenflow_adapters::ai::{self, ChatCompletion};
/// # use obzenflow_core::ai::{AiOutcome, ChatCompletionReply, ChatRequestSpec, Chunk};
/// # use obzenflow_core::TypedPayload;
/// # use obzenflow_runtime::ai::AiRole;
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Story { title: String }
/// # impl TypedPayload for Story { const EVENT_TYPE: &'static str = "ai_doc.story"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Summary { text: String }
/// # impl TypedPayload for Summary { const EVENT_TYPE: &'static str = "ai_doc.summary"; }
/// # #[derive(Clone)]
/// # struct Digest;
/// # impl AiRole<Chunk<Story>> for Digest {
/// #     type Effect = ChatCompletion;
/// #     type Out = Summary;
/// #     fn prepare(&self, chunk: &Chunk<Story>) -> Result<ChatRequestSpec, HandlerError> {
/// #         Ok(ChatRequestSpec::new("Summarize.", format!("{} stories", chunk.len())))
/// #     }
/// #     fn interpret(&self, _: Chunk<Story>, call: AiOutcome<ChatCompletionReply>) -> Result<Summary, HandlerError> {
/// #         Ok(Summary { text: call.replied()?.text().to_string() })
/// #     }
/// # }
/// let digest = ai::map(Digest).key_by(|summary: &Summary| summary.text.clone()).chunks(10);
/// ```
///
/// `ai::infer` makes one call per input and has no key.
///
/// ```compile_fail,E0277
/// # use obzenflow_adapters::ai::{self, ChatCompletion};
/// # use obzenflow_core::ai::{AiOutcome, ChatCompletionReply, ChatRequestSpec};
/// # use obzenflow_core::TypedPayload;
/// # use obzenflow_runtime::ai::AiRole;
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Story { title: String }
/// # impl TypedPayload for Story { const EVENT_TYPE: &'static str = "ai_doc.story"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Summary { text: String }
/// # impl TypedPayload for Summary { const EVENT_TYPE: &'static str = "ai_doc.summary"; }
/// # #[derive(Clone)]
/// # struct Brief;
/// # impl AiRole<Story> for Brief {
/// #     type Effect = ChatCompletion;
/// #     type Out = Summary;
/// #     fn prepare(&self, story: &Story) -> Result<ChatRequestSpec, HandlerError> {
/// #         Ok(ChatRequestSpec::new("Summarize.", story.title.clone()))
/// #     }
/// #     fn interpret(&self, _: Story, call: AiOutcome<ChatCompletionReply>) -> Result<Summary, HandlerError> {
/// #         Ok(Summary { text: call.replied()?.text().to_string() })
/// #     }
/// # }
/// let brief = ai::infer(Brief).key_by(|story: &Story| story.title.clone());
/// ```
///
/// A reduce role takes the partials its map role produces.
///
/// ```compile_fail,E0277
/// # use obzenflow_adapters::ai::{self, ChatCompletion};
/// # use obzenflow_core::ai::{AiOutcome, ChatCompletionReply, ChatRequestSpec, Chunk, Partials};
/// # use obzenflow_core::TypedPayload;
/// # use obzenflow_runtime::ai::AiRole;
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Story { title: String }
/// # impl TypedPayload for Story { const EVENT_TYPE: &'static str = "ai_doc.story"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Summary { text: String }
/// # impl TypedPayload for Summary { const EVENT_TYPE: &'static str = "ai_doc.summary"; }
/// # #[derive(Clone)]
/// # struct Digest;
/// # impl AiRole<Chunk<Story>> for Digest {
/// #     type Effect = ChatCompletion;
/// #     type Out = Summary;
/// #     fn prepare(&self, chunk: &Chunk<Story>) -> Result<ChatRequestSpec, HandlerError> {
/// #         Ok(ChatRequestSpec::new("Summarize.", format!("{} stories", chunk.len())))
/// #     }
/// #     fn interpret(&self, _: Chunk<Story>, call: AiOutcome<ChatCompletionReply>) -> Result<Summary, HandlerError> {
/// #         Ok(Summary { text: call.replied()?.text().to_string() })
/// #     }
/// # }
/// # #[derive(Clone)]
/// # struct CombineStories;
/// # impl AiRole<Partials<Story, Story>> for CombineStories {
/// #     type Effect = ChatCompletion;
/// #     type Out = Summary;
/// #     fn prepare(&self, input: &Partials<Story, Story>) -> Result<ChatRequestSpec, HandlerError> {
/// #         Ok(ChatRequestSpec::new("Combine.", format!("{} stories", input.partials().len())))
/// #     }
/// #     fn interpret(&self, _: Partials<Story, Story>, call: AiOutcome<ChatCompletionReply>) -> Result<Summary, HandlerError> {
/// #         Ok(Summary { text: call.replied()?.text().to_string() })
/// #     }
/// # }
/// let digest = ai::map(Digest).chunks(10).reduce(CombineStories).on_eof();
/// ```
///
/// `with` comes before `=>`.
///
/// ```compile_fail
/// # use obzenflow_adapters::ai::{self, ChatCompletion};
/// # use obzenflow_adapters::middleware::circuit_breaker;
/// # use obzenflow_core::ai::{AiOutcome, ChatCompletionReply, ChatRequestSpec};
/// # use obzenflow_core::TypedPayload;
/// # use obzenflow_runtime::ai::AiRole;
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Story { title: String }
/// # impl TypedPayload for Story { const EVENT_TYPE: &'static str = "ai_doc.story"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Summary { text: String }
/// # impl TypedPayload for Summary { const EVENT_TYPE: &'static str = "ai_doc.summary"; }
/// # #[derive(Clone)]
/// # struct Brief;
/// # impl AiRole<Story> for Brief {
/// #     type Effect = ChatCompletion;
/// #     type Out = Summary;
/// #     fn prepare(&self, story: &Story) -> Result<ChatRequestSpec, HandlerError> {
/// #         Ok(ChatRequestSpec::new("Summarize.", story.title.clone()))
/// #     }
/// #     fn interpret(&self, _: Story, call: AiOutcome<ChatCompletionReply>) -> Result<Summary, HandlerError> {
/// #         Ok(Summary { text: call.replied()?.text().to_string() })
/// #     }
/// # }
/// let brief = ai::infer(Brief);
/// let ai_breaker = circuit_breaker().consecutive_failures(5);
/// let brief_stage = obzenflow_dsl::ai!(Story -> Summary => brief with ai_breaker);
/// ```
///
/// A middleware group holds at least one value; omit `with` for none.
///
/// ```compile_fail
/// # use obzenflow_adapters::ai::{self, ChatCompletion};
/// # use obzenflow_core::ai::{AiOutcome, ChatCompletionReply, ChatRequestSpec};
/// # use obzenflow_core::TypedPayload;
/// # use obzenflow_runtime::ai::AiRole;
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Story { title: String }
/// # impl TypedPayload for Story { const EVENT_TYPE: &'static str = "ai_doc.story"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Summary { text: String }
/// # impl TypedPayload for Summary { const EVENT_TYPE: &'static str = "ai_doc.summary"; }
/// # #[derive(Clone)]
/// # struct Brief;
/// # impl AiRole<Story> for Brief {
/// #     type Effect = ChatCompletion;
/// #     type Out = Summary;
/// #     fn prepare(&self, story: &Story) -> Result<ChatRequestSpec, HandlerError> {
/// #         Ok(ChatRequestSpec::new("Summarize.", story.title.clone()))
/// #     }
/// #     fn interpret(&self, _: Story, call: AiOutcome<ChatCompletionReply>) -> Result<Summary, HandlerError> {
/// #         Ok(Summary { text: call.replied()?.text().to_string() })
/// #     }
/// # }
/// let brief = ai::infer(Brief);
/// let brief_stage = obzenflow_dsl::ai!(Story -> Summary with {} => brief);
/// ```
///
/// The role's `type Effect` names the effect, so a row has no `uses`.
///
/// ```compile_fail
/// # use obzenflow_adapters::ai::{self, ChatCompletion};
/// # use obzenflow_core::ai::{AiOutcome, ChatCompletionReply, ChatRequestSpec};
/// # use obzenflow_core::TypedPayload;
/// # use obzenflow_runtime::ai::AiRole;
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Story { title: String }
/// # impl TypedPayload for Story { const EVENT_TYPE: &'static str = "ai_doc.story"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Summary { text: String }
/// # impl TypedPayload for Summary { const EVENT_TYPE: &'static str = "ai_doc.summary"; }
/// # #[derive(Clone)]
/// # struct Brief;
/// # impl AiRole<Story> for Brief {
/// #     type Effect = ChatCompletion;
/// #     type Out = Summary;
/// #     fn prepare(&self, story: &Story) -> Result<ChatRequestSpec, HandlerError> {
/// #         Ok(ChatRequestSpec::new("Summarize.", story.title.clone()))
/// #     }
/// #     fn interpret(&self, _: Story, call: AiOutcome<ChatCompletionReply>) -> Result<Summary, HandlerError> {
/// #         Ok(Summary { text: call.replied()?.text().to_string() })
/// #     }
/// # }
/// let brief = ai::infer(Brief);
/// let brief_stage = obzenflow_dsl::ai!(Story -> Summary uses ChatCompletion => brief);
/// ```
///
/// Configuration chooses each stage's model, so a row has no `via`.
///
/// ```compile_fail
/// # use obzenflow_adapters::ai::{self, ChatCompletion};
/// # use obzenflow_core::ai::{AiOutcome, ChatCompletionReply, ChatRequestSpec};
/// # use obzenflow_core::TypedPayload;
/// # use obzenflow_runtime::ai::AiRole;
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Story { title: String }
/// # impl TypedPayload for Story { const EVENT_TYPE: &'static str = "ai_doc.story"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Summary { text: String }
/// # impl TypedPayload for Summary { const EVENT_TYPE: &'static str = "ai_doc.summary"; }
/// # #[derive(Clone)]
/// # struct Brief;
/// # impl AiRole<Story> for Brief {
/// #     type Effect = ChatCompletion;
/// #     type Out = Summary;
/// #     fn prepare(&self, story: &Story) -> Result<ChatRequestSpec, HandlerError> {
/// #         Ok(ChatRequestSpec::new("Summarize.", story.title.clone()))
/// #     }
/// #     fn interpret(&self, _: Story, call: AiOutcome<ChatCompletionReply>) -> Result<Summary, HandlerError> {
/// #         Ok(Summary { text: call.replied()?.text().to_string() })
/// #     }
/// # }
/// let brief = ai::infer(Brief);
/// let brief_stage = obzenflow_dsl::ai!(Story -> Summary via chat => brief);
/// ```
///
/// The role's effect declares the guarantee.
///
/// ```compile_fail
/// # use obzenflow_adapters::ai::{self, ChatCompletion};
/// # use obzenflow_core::ai::{AiOutcome, ChatCompletionReply, ChatRequestSpec};
/// # use obzenflow_core::TypedPayload;
/// # use obzenflow_runtime::ai::AiRole;
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Story { title: String }
/// # impl TypedPayload for Story { const EVENT_TYPE: &'static str = "ai_doc.story"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Summary { text: String }
/// # impl TypedPayload for Summary { const EVENT_TYPE: &'static str = "ai_doc.summary"; }
/// # #[derive(Clone)]
/// # struct Brief;
/// # impl AiRole<Story> for Brief {
/// #     type Effect = ChatCompletion;
/// #     type Out = Summary;
/// #     fn prepare(&self, story: &Story) -> Result<ChatRequestSpec, HandlerError> {
/// #         Ok(ChatRequestSpec::new("Summarize.", story.title.clone()))
/// #     }
/// #     fn interpret(&self, _: Story, call: AiOutcome<ChatCompletionReply>) -> Result<Summary, HandlerError> {
/// #         Ok(Summary { text: call.replied()?.text().to_string() })
/// #     }
/// # }
/// let brief = ai::infer(Brief);
/// let brief_stage =
///     obzenflow_dsl::ai!(Story -> Summary => brief.at_least_once::<ChatCompletion>());
/// ```
#[macro_export]
macro_rules! ai {
    ($($tokens:tt)*) => { $crate::__obzenflow_stage_row!(@sig ai __obzenflow_ai_attachment_contract [] $($tokens)*) };
}

/// Rejects every row; declare the stage with `ai!` over a handler built with
/// `ai::infer`.
///
/// ```compile_fail
/// let brief_stage = obzenflow_dsl::inference!(Story -> Summary => brief);
/// ```
#[macro_export]
macro_rules! inference {
    ($($tokens:tt)*) => {
        compile_error!("inference! and ai_map_reduce! were replaced by ai!; build the handler with ai::infer or ai::map")
    };
}

/// Rejects every row; declare the stage with `ai!` over a handler built with
/// `ai::map`.
///
/// ```compile_fail
/// let digest_stage = obzenflow_dsl::ai_map_reduce!(Story -> Summary => digest);
/// ```
#[macro_export]
macro_rules! ai_map_reduce {
    ($($tokens:tt)*) => {
        compile_error!("inference! and ai_map_reduce! were replaced by ai!; build the handler with ai::infer or ai::map")
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_ai_attachment_contract {
    (name: $name:literal, $($rest:tt)+) => {
        $crate::__obzenflow_ai_row!(name = $name, in = [], $($rest)+)
    };
    ($($rest:tt)+) => {
        $crate::__obzenflow_ai_row!(
            name = "__obzenflow_binding_derived_name__",
            in = [],
            $($rest)+
        )
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_ai_row {
    (name = $name:literal, in = [], -> $($rest:tt)*) => {
        compile_error!("expected an input type before `->`")
    };
    (name = $name:literal, in = [$($in:tt)+], -> $($rest:tt)+) => {
        $crate::__obzenflow_ai_row!(@output name = $name, in = [$($in)+], out = [], $($rest)+)
    };
    (name = $name:literal, in = [$($in:tt)*], $next:tt $($rest:tt)*) => {
        $crate::__obzenflow_ai_row!(name = $name, in = [$($in)* $next], $($rest)*)
    };
    (name = $name:literal, in = [$($in:tt)*]) => {
        compile_error!("expected `Input -> Output with middleware => handler`")
    };
    (@output name = $name:literal, in = [$($in:tt)+], out = [$($out:tt)+], => $($rest:tt)+) => {
        $crate::__obzenflow_ai_row!(@handler name = $name, in = [$($in)+], out = [$($out)+], $($rest)+)
    };
    (@output name = $name:literal, in = [$($in:tt)+], out = [$($out:tt)*], $next:tt $($rest:tt)*) => {
        $crate::__obzenflow_ai_row!(@output name = $name, in = [$($in)+], out = [$($out)* $next], $($rest)*)
    };
    (@output name = $name:literal, in = [$($in:tt)+], out = [$($out:tt)*]) => {
        compile_error!("expected `Input -> Output with middleware => handler`")
    };
    (@handler name = $name:literal, in = [$in:ty], out = [{ $first:ty $(, $member:ty)* $(,)? }], $handler_head:ident $(:: $handler_tail:ident)* @attachments [$($mw:expr),* $(,)?]) => {{
        let __handler = $handler_head $(:: $handler_tail)*;
        $crate::__private::assert_ai_group_size(&__handler);
        $crate::__private::assert_ai_reduce_trigger(&__handler);
        $crate::__private::assert_ai_input::<$in, _>(&__handler);
        $crate::__obzenflow_assert_ai_outputs!(__handler, [$first $(, $member)*]);
        $crate::dsl::ai_stage::ai_stage(
            $name,
            __handler,
            vec![$($crate::__private::capture_middleware($mw)),*],
            $crate::__obzenflow_effectful_metadata!(transform, $in, $first, [$first $(, $member)*]),
        )
    }};
    (@handler name = $name:literal, in = [$in:ty], out = [$out:ty], $handler_head:ident $(:: $handler_tail:ident)* @attachments [$($mw:expr),* $(,)?]) => {{
        let __handler = $handler_head $(:: $handler_tail)*;
        $crate::__private::assert_ai_group_size(&__handler);
        $crate::__private::assert_ai_reduce_trigger(&__handler);
        $crate::__private::assert_ai_arrow::<$in, $out, _>(&__handler);
        $crate::dsl::ai_stage::ai_stage(
            $name,
            __handler,
            vec![$($crate::__private::capture_middleware($mw)),*],
            $crate::__obzenflow_effectful_metadata!(transform, $in, $out, single),
        )
    }};
    (@handler name = $name:literal, in = [$($in:tt)+], out = [$($out:tt)+], placeholder!($($message:tt)*) $($rest:tt)*) => {
        compile_error!("an `ai!` stage takes a named handler built with `ai::infer` or `ai::map`")
    };
    (@handler name = $name:literal, in = [$($in:tt)+], out = [$($out:tt)+], $($rest:tt)*) => {
        compile_error!("expected `Input -> Output with middleware => handler`")
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_assert_ai_outputs {
    ($handler:ident, [$($member:ty),+]) => {{
        fn __obzenflow_assert_ai_outputs<H, ArrowToHandlerProof, HandlerToArrowProof>(_: &H)
        where
            H: $crate::dsl::ai_stage::AiStageHandler,
            <$crate::__obzenflow_row_facts![$($member),+] as $crate::__private::Outcome>::Members:
                $crate::dsl::typing::ArrowOutputsAreDeclaredByHandler<
                    <<H as $crate::dsl::ai_stage::AiStageHandler>::Out as $crate::__private::Outcome>::Members,
                    ArrowToHandlerProof,
                >,
            <<H as $crate::dsl::ai_stage::AiStageHandler>::Out as $crate::__private::Outcome>::Members:
                $crate::dsl::typing::HandlerOutputsAreDeclaredByArrow<
                    <$crate::__obzenflow_row_facts![$($member),+] as $crate::__private::Outcome>::Members,
                    HandlerToArrowProof,
                >,
        {}

        __obzenflow_assert_ai_outputs::<_, _, _>(&$handler);
        const _: () = $crate::__private::assert_distinct_outcome_facts::<
            $crate::__obzenflow_row_facts![$($member),+],
        >();
    }};
}

/// Declares a typed join whose catalog fact type comes first.
///
/// The handler is a built-in join, a `TypedJoinHandler`, or a point-in-time
/// join, which an author-written `TypedAsOfJoinHandler` becomes through
/// `AsOfJoin::new`. A handler whose catalog type differs from the arrow's does
/// not compile:
///
/// ```compile_fail,E0277
/// # use obzenflow_core::TypedPayload;
/// # use serde::{Deserialize, Serialize};
/// use obzenflow_runtime::stages::join::{left_as_of, AsOf};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct FxRate { currency: String, effective_at: u64 }
/// # impl TypedPayload for FxRate { const EVENT_TYPE: &'static str = "docs.join.fx_rate"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Payment { currency: String, booked_at: u64 }
/// # impl TypedPayload for Payment { const EVENT_TYPE: &'static str = "docs.join.payment"; }
///
/// let price = left_as_of(
///     |rate: &FxRate| rate.currency.clone(),
///     |rate: &FxRate| rate.effective_at,
///     |payment: &Payment| payment.currency.clone(),
///     |payment: &Payment| payment.booked_at,
///     |_rate: AsOf<FxRate>, payment: Payment| payment,
/// );
/// let _ = obzenflow_dsl::join!(catalog rates: Payment, Payment -> Payment => price);
/// ```
///
/// An author-written point-in-time handler is bound with `AsOfJoin::new`:
///
/// ```compile_fail,E0277
/// # use obzenflow_core::TypedPayload;
/// # use serde::{Deserialize, Serialize};
/// use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// use obzenflow_runtime::stages::join::{AsOfReferenceView, TypedAsOfJoinHandler};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Rate { currency: String, effective_at: u64 }
/// # impl TypedPayload for Rate { const EVENT_TYPE: &'static str = "docs.join.rate"; }
///
/// #[derive(Clone, Debug)]
/// struct Price;
///
/// impl TypedAsOfJoinHandler for Price {
///     type State = ();
///     type ReferenceKey = String;
///     type EffectiveTime = u64;
///     type Reference = Rate;
///     type Stream = Rate;
///     type Output = Rate;
///
///     fn initial_state(&self) {}
///
///     fn admit_reference(&self, rate: &Rate) -> Result<(String, u64), HandlerError> {
///         Ok((rate.currency.clone(), rate.effective_at))
///     }
///
///     fn process_stream(
///         &self,
///         _state: &mut (),
///         _references: &mut AsOfReferenceView<'_, String, u64, Rate>,
///         row: Rate,
///     ) -> Result<Vec<Rate>, HandlerError> {
///         Ok(vec![row])
///     }
/// }
///
/// let _ = obzenflow_dsl::join!(catalog rates: Rate, Rate -> Rate => Price);
/// ```
#[macro_export]
macro_rules! join {
    ($($tokens:tt)*) => { $crate::__obzenflow_stage_row!(@sig plain __obzenflow_join_attachment_contract [] $($tokens)*) };
}

/// Declares a typed sink. The row's command builds one request per input and
/// sends it through its port. The port method's `#[effect]` states the
/// guarantee, so the row takes no decoration.
///
/// ```
/// # use obzenflow_runtime::effects::{perform, EffectError, Port};
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Fact;
/// # impl obzenflow_core::TypedPayload for Fact { const EVENT_TYPE: &'static str = "sink_doc.fact"; }
/// #[derive(Clone, Debug, serde::Serialize)]
/// struct Entry;
///
/// #[obzenflow_runtime::effects::port]
/// trait LedgerPort {
///     #[effect(type = "sink_doc.record", exactly_once, records = reply)]
///     fn record(&self, entry: &Entry) -> Result<(), EffectError>;
/// }
///
/// struct Ledger;
///
/// #[obzenflow_runtime::effects::system("ledger")]
/// impl LedgerPort for Ledger {
///     fn record(&self, _: &Entry) -> Result<(), EffectError> {
///         Ok(())
///     }
/// }
///
/// let ledger = perform(|_: &Fact| Entry).through(Port::to(Ledger));
/// let sink = obzenflow_dsl::sink!(Fact => ledger);
/// ```
///
/// A request whose port method records facts belongs in `effectful_transform!`:
///
/// ```compile_fail,E0277
/// # use obzenflow_runtime::effects::{perform, EffectError, Port};
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Fact;
/// # impl obzenflow_core::TypedPayload for Fact { const EVENT_TYPE: &'static str = "sink_doc.fact"; }
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Recorded;
/// # impl obzenflow_core::TypedPayload for Recorded { const EVENT_TYPE: &'static str = "sink_doc.recorded"; }
/// #[derive(Clone, Debug, serde::Serialize)]
/// struct Entry;
///
/// #[obzenflow_runtime::effects::port]
/// trait LedgerPort {
///     #[effect(type = "sink_doc.record", exactly_once)]
///     fn record(&self, entry: &Entry) -> Result<Recorded, EffectError>;
/// }
///
/// struct Ledger;
///
/// #[obzenflow_runtime::effects::system("ledger")]
/// impl LedgerPort for Ledger {
///     fn record(&self, _: &Entry) -> Result<Recorded, EffectError> {
///         Ok(Recorded)
///     }
/// }
///
/// let ledger = perform(|_: &Fact| Entry).through(Port::to(Ledger));
/// let sink = obzenflow_dsl::sink!(Fact => ledger);
/// ```
///
/// The console port declares `at_least_once`. Repeating it on the sink row fails to compile.
///
/// ```compile_fail
/// # use obzenflow_adapters::sinks::{ConsoleOutput, ConsoleSink, DebugFormatter};
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Fact;
/// # impl obzenflow_core::TypedPayload for Fact { const EVENT_TYPE: &'static str = "sink_doc.fact"; }
/// let log = ConsoleSink::new(DebugFormatter);
/// let sink = obzenflow_dsl::sink!(Fact => log.at_least_once::<ConsoleOutput>());
/// ```
///
/// `.idempotent()` and `.non_idempotent()` were removed:
///
/// ```compile_fail
/// # use obzenflow_runtime::effects::{perform, EffectError, Port};
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Fact;
/// # impl obzenflow_core::TypedPayload for Fact { const EVENT_TYPE: &'static str = "sink_doc.fact"; }
/// # #[derive(Clone, Debug, serde::Serialize)]
/// # struct Entry;
/// # #[obzenflow_runtime::effects::port]
/// # trait LedgerPort {
/// #     #[effect(type = "sink_doc.record", exactly_once, records = reply)]
/// #     fn record(&self, entry: &Entry) -> Result<(), EffectError>;
/// # }
/// # struct Ledger;
/// # #[obzenflow_runtime::effects::system("ledger")]
/// # impl LedgerPort for Ledger {
/// #     fn record(&self, _: &Entry) -> Result<(), EffectError> { Ok(()) }
/// # }
/// let ledger = perform(|_: &Fact| Entry).through(Port::to(Ledger));
/// let sink = obzenflow_dsl::sink!(Fact => ledger.idempotent());
/// ```
///
/// ```compile_fail
/// # use obzenflow_runtime::effects::{perform, EffectError, Port};
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Fact;
/// # impl obzenflow_core::TypedPayload for Fact { const EVENT_TYPE: &'static str = "sink_doc.fact"; }
/// # #[derive(Clone, Debug, serde::Serialize)]
/// # struct Entry;
/// # #[obzenflow_runtime::effects::port]
/// # trait LedgerPort {
/// #     #[effect(type = "sink_doc.record", exactly_once, records = reply)]
/// #     fn record(&self, entry: &Entry) -> Result<(), EffectError>;
/// # }
/// # struct Ledger;
/// # #[obzenflow_runtime::effects::system("ledger")]
/// # impl LedgerPort for Ledger {
/// #     fn record(&self, _: &Entry) -> Result<(), EffectError> { Ok(()) }
/// # }
/// let ledger = perform(|_: &Fact| Entry).through(Port::to(Ledger));
/// let sink = obzenflow_dsl::sink!(Fact => ledger.non_idempotent());
/// ```
///
/// `handler_set!` was removed:
///
/// ```compile_fail
/// # use obzenflow_runtime::effects::{perform, EffectError, Port};
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Fact;
/// # impl obzenflow_core::TypedPayload for Fact { const EVENT_TYPE: &'static str = "sink_doc.fact"; }
/// # #[derive(Clone, Debug, serde::Serialize)]
/// # struct Entry;
/// # #[obzenflow_runtime::effects::port]
/// # trait LedgerPort {
/// #     #[effect(type = "sink_doc.record", exactly_once, records = reply)]
/// #     fn record(&self, entry: &Entry) -> Result<(), EffectError>;
/// # }
/// # struct Ledger;
/// # #[obzenflow_runtime::effects::system("ledger")]
/// # impl LedgerPort for Ledger {
/// #     fn record(&self, _: &Entry) -> Result<(), EffectError> { Ok(()) }
/// # }
/// let ledger = perform(|_: &Fact| Entry).through(Port::to(Ledger));
/// let sink = obzenflow_dsl::sink!(Fact => handler_set!(ledger));
/// ```
///
/// A command that calls ports itself belongs in `effectful_transform!`:
///
/// ```compile_fail,E0277
/// # use async_trait::async_trait;
/// # use obzenflow_runtime::effects::{EffectError, Effects, Port, StageCompletion};
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Fact;
/// # impl obzenflow_core::TypedPayload for Fact { const EVENT_TYPE: &'static str = "sink_doc.fact"; }
/// # #[derive(Clone, Debug, serde::Serialize)]
/// # struct Entry;
/// # #[obzenflow_runtime::effects::port]
/// # trait LedgerPort {
/// #     #[effect(type = "sink_doc.record", exactly_once, records = reply)]
/// #     fn record(&self, entry: &Entry) -> Result<(), EffectError>;
/// # }
/// #[obzenflow_runtime::effects::command]
/// #[derive(Clone, Debug)]
/// struct RecordEntry {
///     #[port]
///     ledger: Port<dyn LedgerPort>,
/// }
///
/// #[async_trait]
/// impl EffectfulTransformHandler for RecordEntry {
///     type Input = Fact;
///     type Output = Fact;
///     async fn process(&self, _: Fact, _: &mut Effects<Fact, Self::AllowedEffects>) -> Result<StageCompletion<Fact>, HandlerError> { unimplemented!() }
/// }
/// # fn build(record_entry: RecordEntry) {
/// let sink = obzenflow_dsl::sink!(Fact => record_entry);
/// # }
/// ```
#[macro_export]
macro_rules! sink {
    ($($tokens:tt)*) => { $crate::__obzenflow_stage_row!(@sig sink __obzenflow_sink_attachment_contract [] $($tokens)*) };
}

/// `source!` declares a typed source with optional middleware, written after the
/// stage's type and before `=>`.
///
/// ```
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Fact;
/// # impl obzenflow_core::TypedPayload for Fact { const EVENT_TYPE: &'static str = "grammar.fact"; }
/// # use obzenflow_adapters::middleware::{rate_limit, circuit_breaker};
/// let source = obzenflow_dsl::source!(Fact with { rate_limit(10.0), circuit_breaker().consecutive_failures(3) } => placeholder!());
/// ```
///
/// Empty groups are invalid.
///
/// ```compile_fail
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Fact;
/// # impl obzenflow_core::TypedPayload for Fact { const EVENT_TYPE: &'static str = "grammar.fact"; }
/// # use obzenflow_adapters::middleware::{rate_limit, circuit_breaker};
/// let source = obzenflow_dsl::source!(Fact with {} => placeholder!());
/// ```
///
/// Groups cannot be nested.
///
/// ```compile_fail
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Fact;
/// # impl obzenflow_core::TypedPayload for Fact { const EVENT_TYPE: &'static str = "grammar.fact"; }
/// # use obzenflow_adapters::middleware::{rate_limit, circuit_breaker};
/// let source = obzenflow_dsl::source!(Fact with { { rate_limit(10.0) } } => placeholder!());
/// ```
///
/// Each stage accepts one clause.
///
/// ```compile_fail
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Fact;
/// # impl obzenflow_core::TypedPayload for Fact { const EVENT_TYPE: &'static str = "grammar.fact"; }
/// # use obzenflow_adapters::middleware::{rate_limit, circuit_breaker};
/// let source = obzenflow_dsl::source!(Fact with rate_limit(10.0) with circuit_breaker().consecutive_failures(3) => placeholder!());
/// ```
///
/// Square-bracket attachment lists are not part of the grammar.
///
/// ```compile_fail
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Fact;
/// # impl obzenflow_core::TypedPayload for Fact { const EVENT_TYPE: &'static str = "grammar.fact"; }
/// # use obzenflow_adapters::middleware::{rate_limit, circuit_breaker};
/// let source = obzenflow_dsl::source!(Fact with [rate_limit(10.0)] => placeholder!());
/// ```
///
/// `with` after the handler is refused.
///
/// ```compile_fail
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Fact;
/// # impl obzenflow_core::TypedPayload for Fact { const EVENT_TYPE: &'static str = "grammar.fact"; }
/// # use obzenflow_adapters::middleware::rate_limit;
/// let source = obzenflow_dsl::source!(Fact => placeholder!() with rate_limit(10.0));
/// ```
///
/// Backpressure is set in configuration, not after the handler.
///
/// ```compile_fail
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Fact;
/// # impl obzenflow_core::TypedPayload for Fact { const EVENT_TYPE: &'static str = "grammar.fact"; }
/// let source = obzenflow_dsl::source!(Fact => placeholder!(), backpressure: enforced(64));
/// ```
///
/// Observers use the same attachment clause.
///
/// ```compile_fail
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Fact;
/// # impl obzenflow_core::TypedPayload for Fact { const EVENT_TYPE: &'static str = "grammar.fact"; }
/// # use obzenflow_adapters::middleware::{rate_limit, circuit_breaker};
/// let source = obzenflow_dsl::source!(Fact => placeholder!()  , observers: []);
/// ```
///
/// Hosted ingress placement is inferred from the configured source.
///
/// ```compile_fail
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Fact;
/// # impl obzenflow_core::TypedPayload for Fact { const EVENT_TYPE: &'static str = "grammar.fact"; }
/// # use obzenflow_adapters::middleware::{rate_limit, circuit_breaker};
/// let source = obzenflow_dsl::source!(Fact => placeholder!()  , ingress with rate_limit(10.0));
/// ```
///
/// The handler slot of a source stage takes a source handler or
/// `placeholder!()`. The handler declares its read port. The framework reads
/// through that port. Hosted ingress registration returns a hosted source
/// handler.
///
/// ```
/// # use obzenflow_runtime::queries::{QueryContext, QueryError, QueryPort};
/// # use obzenflow_runtime::stages::common::handlers::source::{FiniteRead, SourceRead};
/// # use obzenflow_runtime::stages::source::TypedFiniteSourceHandler;
/// # use obzenflow_runtime::stages::{HostedSource, HostedSubmission};
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Fact;
/// # impl obzenflow_core::TypedPayload for Fact { const EVENT_TYPE: &'static str = "grammar.fact"; }
/// #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// struct NextFact;
///
/// #[obzenflow_runtime::effects::port]
/// #[async_trait::async_trait]
/// trait FactInput {
///     #[query(type = "grammar.facts.read")]
///     async fn read(
///         &self,
///         from: &NextFact,
///         ctx: &QueryContext,
///     ) -> Result<FiniteRead<Fact, NextFact>, QueryError>;
/// }
///
/// struct FactFile;
///
/// #[obzenflow_runtime::effects::system("fact-file")]
/// #[async_trait::async_trait]
/// impl FactInput for FactFile {
///     async fn read(
///         &self,
///         _from: &NextFact,
///         _ctx: &QueryContext,
///     ) -> Result<FiniteRead<Fact, NextFact>, QueryError> {
///         Ok(FiniteRead::EndOfInput)
///     }
/// }
///
/// struct Facts(QueryPort<dyn FactInput>);
///
/// impl TypedFiniteSourceHandler for Facts {
///     type Output = Fact;
///     fn read(&self) -> SourceRead<Fact> {
///         SourceRead::finite(self.0.clone(), NextFact, Ok)
///     }
/// }
///
/// let facts = Facts(QueryPort::to(FactFile));
/// # let (_, submissions) =
/// #     obzenflow_runtime::stages::common::handlers::source::source_channel::<
/// #         HostedSubmission<Fact>,
/// #     >(1);
/// # let posted = HostedSource::ingress(
/// #     submissions,
/// #     obzenflow_core::ingress::HostedIngressBindingSlot::new("grammar.facts"),
/// # );
/// let read = obzenflow_dsl::source!(Fact => facts);
/// let received = obzenflow_dsl::source!(Fact => posted);
/// ```
///
/// A port is not a source handler, so the compiler refuses it.
///
/// ```compile_fail,E0277
/// # use obzenflow_runtime::queries::{QueryContext, QueryError, QueryPort};
/// # use obzenflow_runtime::stages::common::handlers::source::FiniteRead;
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Fact;
/// # impl obzenflow_core::TypedPayload for Fact { const EVENT_TYPE: &'static str = "grammar.fact"; }
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct NextFact;
/// # #[obzenflow_runtime::effects::port]
/// # #[async_trait::async_trait]
/// # trait FactInput {
/// #     #[query(type = "grammar.facts.read")]
/// #     async fn read(
/// #         &self,
/// #         from: &NextFact,
/// #         ctx: &QueryContext,
/// #     ) -> Result<FiniteRead<Fact, NextFact>, QueryError>;
/// # }
/// # struct FactFile;
/// # #[obzenflow_runtime::effects::system("fact-file")]
/// # #[async_trait::async_trait]
/// # impl FactInput for FactFile {
/// #     async fn read(
/// #         &self,
/// #         _from: &NextFact,
/// #         _ctx: &QueryContext,
/// #     ) -> Result<FiniteRead<Fact, NextFact>, QueryError> {
/// #         Ok(FiniteRead::EndOfInput)
/// #     }
/// # }
/// let fact_file: QueryPort<dyn FactInput> = QueryPort::to(FactFile);
/// let read = obzenflow_dsl::source!(Fact => fact_file);
/// ```
#[macro_export]
macro_rules! source {
    ($($tokens:tt)*) => { $crate::__obzenflow_stage_row!(@sig plain __obzenflow_source_attachment_contract [] $($tokens)*) };
}

/// Declares a typed stateful stage. Its handler declares any emit interval
/// through `emit_interval_hint()`; the row takes no interval clause.
///
/// ```
/// # use obzenflow_runtime::stages::common::handlers::{StatefulEmission, TypedStatefulHandler};
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Fact;
/// # impl obzenflow_core::TypedPayload for Fact { const EVENT_TYPE: &'static str = "stateful_doc.fact"; }
/// # #[derive(Clone, Debug)]
/// # struct Totals;
/// # impl TypedStatefulHandler for Totals {
/// #     type State = ();
/// #     type Input = Fact;
/// #     type Output = Fact;
/// #     fn accumulate(&self, _: &mut (), _: Fact) {}
/// #     fn initial_state(&self) {}
/// #     fn emit(&self, _: &()) -> StatefulEmission<Fact> {
/// #         StatefulEmission::RetainEpoch { outputs: Vec::new() }
/// #     }
/// #     fn post_emit(&self, _: &mut ()) {}
/// # }
/// let totals = Totals;
/// let stage = obzenflow_dsl::stateful!(Fact -> Fact => totals);
/// ```
///
/// ```compile_fail
/// # use obzenflow_runtime::stages::common::handlers::{StatefulEmission, TypedStatefulHandler};
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Fact;
/// # impl obzenflow_core::TypedPayload for Fact { const EVENT_TYPE: &'static str = "stateful_doc.fact"; }
/// # #[derive(Clone, Debug)]
/// # struct Totals;
/// # impl TypedStatefulHandler for Totals {
/// #     type State = ();
/// #     type Input = Fact;
/// #     type Output = Fact;
/// #     fn accumulate(&self, _: &mut (), _: Fact) {}
/// #     fn initial_state(&self) {}
/// #     fn emit(&self, _: &()) -> StatefulEmission<Fact> {
/// #         StatefulEmission::RetainEpoch { outputs: Vec::new() }
/// #     }
/// #     fn post_emit(&self, _: &mut ()) {}
/// # }
/// let totals = Totals;
/// let stage = obzenflow_dsl::stateful!(
///     Fact -> Fact => totals,
///     emit_interval = std::time::Duration::from_secs(5)
/// );
/// ```
#[macro_export]
macro_rules! stateful {
    ($($tokens:tt)*) => { $crate::__obzenflow_stage_row!(@sig plain __obzenflow_stateful_attachment_contract [] $($tokens)*) };
}

#[macro_export]
macro_rules! transform {
    ($($tokens:tt)*) => { $crate::__obzenflow_stage_row!(@sig plain __obzenflow_transform_attachment_contract [] $($tokens)*) };
}

/// Declares a merge stage, which runs no author code and combines several
/// producers of one type in the order its framework strategy decides.
///
/// ```
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Fact;
/// # impl obzenflow_core::TypedPayload for Fact { const EVENT_TYPE: &'static str = "merge_doc.fact"; }
/// use obzenflow_runtime::stages::merges;
/// let _stage = obzenflow_dsl::merge!(Fact => merges::then_live);
/// ```
///
/// A merge has no default strategy.
///
/// ```compile_fail
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Fact;
/// # impl obzenflow_core::TypedPayload for Fact { const EVENT_TYPE: &'static str = "merge_doc.fact"; }
/// let _stage = obzenflow_dsl::merge!(Fact);
/// ```
///
/// The `=>` slot takes only a strategy from `merges`.
///
/// ```compile_fail
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Fact;
/// # impl obzenflow_core::TypedPayload for Fact { const EVENT_TYPE: &'static str = "merge_doc.fact"; }
/// #[derive(Clone, Copy)]
/// struct PreferStore;
/// let _stage = obzenflow_dsl::merge!(Fact => PreferStore);
/// ```
///
/// A merge takes no `with` clause.
///
/// ```compile_fail
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Fact;
/// # impl obzenflow_core::TypedPayload for Fact { const EVENT_TYPE: &'static str = "merge_doc.fact"; }
/// # use obzenflow_adapters::middleware::rate_limit;
/// use obzenflow_runtime::stages::merges;
/// let _stage = obzenflow_dsl::merge!(Fact => merges::then_live with rate_limit(10.0));
/// ```
///
/// A `concat` merge names the producer it reads first, left of `=>`, and
/// topology wires both producers with plain edges.
///
/// ```
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Fact;
/// # impl obzenflow_core::TypedPayload for Fact { const EVENT_TYPE: &'static str = "merge_doc.fact"; }
/// use obzenflow_runtime::stages::merges;
/// let _stage = obzenflow_dsl::merge!(first score: Fact => merges::concat);
/// ```
///
/// A `concat` merge without a `first` clause does not compile.
///
/// ```compile_fail
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Fact;
/// # impl obzenflow_core::TypedPayload for Fact { const EVENT_TYPE: &'static str = "merge_doc.fact"; }
/// use obzenflow_runtime::stages::merges;
/// let _stage = obzenflow_dsl::merge!(Fact => merges::concat);
/// ```
///
/// A `then_live` merge orders its producers by their sources and takes no
/// `first` clause.
///
/// ```compile_fail
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Fact;
/// # impl obzenflow_core::TypedPayload for Fact { const EVENT_TYPE: &'static str = "merge_doc.fact"; }
/// use obzenflow_runtime::stages::merges;
/// let _stage = obzenflow_dsl::merge!(first score: Fact => merges::then_live);
/// ```
#[macro_export]
macro_rules! merge {
    (name: $name:literal, first $first:ident : $ty:ty => $($strategy:ident)::+ , backpressure : $($rest:tt)*) => {
        compile_error!("`backpressure:` was removed; set backpressure in configuration, under `[runtime.backpressure.flow]` or `[runtime.backpressure.stages.<stage>]`")
    };
    (first $first:ident : $ty:ty => $($strategy:ident)::+ , backpressure : $($rest:tt)*) => {
        compile_error!("`backpressure:` was removed; set backpressure in configuration, under `[runtime.backpressure.flow]` or `[runtime.backpressure.stages.<stage>]`")
    };
    (name: $name:literal, $ty:ty => $($strategy:ident)::+ , backpressure : $($rest:tt)*) => {
        compile_error!("`backpressure:` was removed; set backpressure in configuration, under `[runtime.backpressure.flow]` or `[runtime.backpressure.stages.<stage>]`")
    };
    ($ty:ty => $($strategy:ident)::+ , backpressure : $($rest:tt)*) => {
        compile_error!("`backpressure:` was removed; set backpressure in configuration, under `[runtime.backpressure.flow]` or `[runtime.backpressure.stages.<stage>]`")
    };
    (name: $name:literal, first $first:ident : $ty:ty => $($strategy:ident)::+ $(,)?) => {
        $crate::dsl::typing::first_merge_descriptor::<$ty, _>(
            $name,
            stringify!($first),
            $($strategy)::+,
        )
    };
    (name: $name:literal, first $first:ident : $ty:ty => $($strategy:ident)::+ with $($rest:tt)*) => {
        compile_error!("merge! takes no `with` clause. A merge runs no author code, so middleware has nothing to attach to.")
    };
    (name: $name:literal, first $first:ident : $ty:ty $(,)?) => {
        compile_error!("merge! needs a strategy, for example `merge!(first score: AlertRaised => merges::concat)`.\nA merge has no default strategy.")
    };
    (first $first:ident : $ty:ty => $($strategy:ident)::+ $(,)?) => {
        $crate::merge!(name: "__obzenflow_binding_derived_name__", first $first: $ty => $($strategy)::+)
    };
    (first $first:ident : $ty:ty => $($strategy:ident)::+ with $($rest:tt)*) => {
        compile_error!("merge! takes no `with` clause. A merge runs no author code, so middleware has nothing to attach to.")
    };
    (first $first:ident : $ty:ty $(,)?) => {
        compile_error!("merge! needs a strategy, for example `merge!(first score: AlertRaised => merges::concat)`.\nA merge has no default strategy.")
    };
    (name: $name:literal, $ty:ty => $($strategy:ident)::+ $(,)?) => {
        $crate::dsl::typing::merge_descriptor::<$ty, _>(
            $name,
            $($strategy)::+,
        )
    };
    (name: $name:literal, $ty:ty => $($strategy:ident)::+ with $($rest:tt)*) => {
        compile_error!("merge! takes no `with` clause. A merge runs no author code, so middleware has nothing to attach to.")
    };
    (name: $name:literal, $ty:ty $(,)?) => {
        compile_error!("merge! needs a strategy, for example `merge!(AccountOpened => merges::then_live)`.\nA merge has no default strategy.")
    };
    (name: $($rest:tt)*) => {
        compile_error!("expected `merge!(name: \"stage\", Type => merges::strategy)`")
    };
    ($ty:ty => $($strategy:ident)::+ $(,)?) => {
        $crate::merge!(name: "__obzenflow_binding_derived_name__", $ty => $($strategy)::+)
    };
    ($ty:ty => $($strategy:ident)::+ with $($rest:tt)*) => {
        compile_error!("merge! takes no `with` clause. A merge runs no author code, so middleware has nothing to attach to.")
    };
    ($ty:ty $(,)?) => {
        compile_error!("merge! needs a strategy, for example `merge!(AccountOpened => merges::then_live)`.\nA merge has no default strategy.")
    };
}

/// A middleware group is a flat collection, not a nested execution stack.
#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_flat_attachment_group {
    () => { compile_error!("middleware groups must be non-empty; omit `with` when no middleware is attached") };
    ({ $($nested:tt)* } $($rest:tt)*) => { compile_error!("middleware groups must be flat; attach each definition directly") };
    ($member:expr, $($rest:tt)+) => { $crate::__obzenflow_flat_attachment_group!($($rest)+) };
    ($member:expr $(,)?) => {};
}
