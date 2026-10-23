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
//! async_source!(Out => handler)
//! infinite_source!(Out => handler)
//! async_infinite_source!(Out with rate_limit(10.0) => handler)
//! transform!(In -> Out => handler)
//! effectful_transform!(In -> Out with policy => handler)
//! stateful!(In -> Out => handler)
//! effectful_stateful!(In -> Out => handler.at_least_once::<ChargeCard>())
//! sink!(In => handler.idempotent())
//! join!(catalog CatalogStage: Catalog, Stream -> Out => handler)
//! inference!(In -> Out uses at_least_once(ChatCompletion) via chat with policy => handler)
//! ai_map_reduce!(Seed -> Out => { /* named roles */ }, chunking: by_budget { /* ... */ })
//! ```
//!
//! Every stage macro except `inference!` and `ai_map_reduce!` takes one row
//! shape: the stage's types, an optional `with` clause holding its middleware,
//! `=>`, then the handler and its decorations. An effectful handler's effect
//! set declares the effects it performs, its `#[system]` fields hold the
//! systems that answer them, and `.at_least_once::<E>()` or
//! `.transactional::<E>()` acknowledges a paid effect. A sink's `.idempotent()` or `.non_idempotent()`
//! classifies its redelivery. Backpressure is set only in configuration.
//!
//! Ordinary handler and role slots accept only a local name, unit value path,
//! or qualified value path made from identifier segments, followed by its
//! decorations. Calls, builder chains, closures, and struct literals are
//! rejected with a diagnostic that teaches the binding idiom. Builder-owned
//! values are constructed as ordinary Rust inside
//! [`FlowDefinition::materialize`](crate::FlowDefinition::materialize),
//! immediately above the inner `flow!`, and only their names appear here.
//! `placeholder!()` and `placeholder!("reason")` remain the sketching forms.
//!
//! The decoration matrix covers binding-derived and explicit names plus each
//! family's applicable contract, the `with` clause, handler decorations and
//! catalog clauses. Async-source poll timeout is configured on the handler and
//! exposed through its `poll_timeout()` method; it is not a stage-macro clause
//! or positional tuple.
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

/// Implementation helper for the removed async-source timeout tuple.
#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_async_source_timeout_diagnostic {
    ($surface:literal, $example:literal) => {
        compile_error!(concat!(
            $surface,
            ": poll timeout is handler configuration; timeout tuples are no longer accepted. ",
            "Configure the handler inside the materialiser, then pass its name. Example: ",
            $example,
        ))
    };
}

/// Shared proof-carrying source admission used by all four source families.
#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_admit_typed_source {
    (factory = $factory:ident, output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = $handler:expr, source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*]) => {{
        const _: () = $crate::__private::assert_distinct_stage_fact_set::<
            $crate::__private::stage_fact_set![$($member),+],
        >();
        let __source_policies: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($policy)),*];
        let __ingress_policy: Option<Box<dyn $crate::__private::MiddlewareFactory>> =
            None $(.or_else(|| Some($crate::__private::capture_middleware($ingress))))?;
        let __observers: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($observer)),*];        $crate::dsl::typing::$factory::<
            _,
            _,
            $out,
            $crate::__private::stage_fact_set![$($member),+],
            _,
            _,
            _,
        >(
            $name,
            $handler,
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
            vec![$($crate::__private::capture_middleware($observer)),*];        $crate::dsl::typing::$factory::<_, _, $out, $out, _, _, _>(
            $name,
            $handler,
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
        const _: () = $crate::__private::assert_distinct_stage_fact_set::<
            $crate::__private::stage_fact_set![$($member),+],
        >();
        let __source_policies: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($policy)),*];
        let __ingress_policy: Option<Box<dyn $crate::__private::MiddlewareFactory>> =
            None $(.or_else(|| Some($crate::__private::capture_middleware($ingress))))?;
        let __observers: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($observer)),*];        $crate::dsl::typing::$factory::<
            $out,
            $crate::__private::stage_fact_set![$($member),+],
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
        $crate::__obzenflow_admit_typed_source!(factory = typed_finite_source_descriptor, output = $out, output_contract = [$($member),+], name = $name, handler = $handler, source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*])
    }};
    (output = $out:ty, name = $name:literal, handler = placeholder!(), source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*]) => {{
        $crate::__obzenflow_admit_placeholder_source!(factory = placeholder_finite_source_descriptor, output = $out, name = $name, message = None, source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*])
    }};
    (output = $out:ty, name = $name:literal, handler = placeholder!($msg:expr), source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*]) => {{
        $crate::__obzenflow_admit_placeholder_source!(factory = placeholder_finite_source_descriptor, output = $out, name = $name, message = Some($msg), source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*])
    }};
    (output = $out:ty, name = $name:literal, handler = $handler:expr, source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*]) => {{
        $crate::__obzenflow_admit_typed_source!(factory = typed_finite_source_descriptor, output = $out, name = $name, handler = $handler, source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*])
    }};
}

/// Create a finite source stage descriptor.
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
// async_source!  +  __obzenflow_async_source_typed!
// ============================================================================

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_async_source_typed {
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!(), source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*]) => {{
        $crate::__obzenflow_admit_placeholder_source!(factory = placeholder_async_finite_source_descriptor, output = $out, output_contract = [$($member),+], name = $name, message = None, source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*])
    }};
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!($msg:expr), source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*]) => {{
        $crate::__obzenflow_admit_placeholder_source!(factory = placeholder_async_finite_source_descriptor, output = $out, output_contract = [$($member),+], name = $name, message = Some($msg), source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*])
    }};
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = $handler:expr, source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*]) => {{
        $crate::__obzenflow_admit_typed_source!(factory = typed_async_finite_source_descriptor, output = $out, output_contract = [$($member),+], name = $name, handler = $handler, source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*])
    }};
    (output = $out:ty, name = $name:literal, handler = placeholder!(), source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*]) => {{
        $crate::__obzenflow_admit_placeholder_source!(factory = placeholder_async_finite_source_descriptor, output = $out, name = $name, message = None, source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*])
    }};
    (output = $out:ty, name = $name:literal, handler = placeholder!($msg:expr), source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*]) => {{
        $crate::__obzenflow_admit_placeholder_source!(factory = placeholder_async_finite_source_descriptor, output = $out, name = $name, message = Some($msg), source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*])
    }};
    (output = $out:ty, name = $name:literal, handler = $handler:expr, source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*]) => {{
        $crate::__obzenflow_admit_typed_source!(factory = typed_async_finite_source_descriptor, output = $out, name = $name, handler = $handler, source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*])
    }};
}

/// Create an async finite source stage descriptor.
#[macro_export]
macro_rules! __obzenflow_async_source_attachment_contract {












    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!() $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => {
        $crate::__obzenflow_async_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = "__obzenflow_binding_derived_name__", handler = placeholder!(), source_policies = [$($($policy),*)?], ingress_policy = [], observers = [])
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr) $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => {
        $crate::__obzenflow_async_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = "__obzenflow_binding_derived_name__", handler = placeholder!($msg), source_policies = [$($($policy),*)?], ingress_policy = [], observers = [])
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => {
        $crate::__obzenflow_async_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = "__obzenflow_binding_derived_name__", handler = $handler_head $(:: $handler_tail)*, source_policies = [$($($policy),*)?], ingress_policy = [], observers = [])
    };
    ($out:ty => placeholder!() $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = placeholder!(), source_policies = [$($($policy),*)?], ingress_policy = [], observers = [])
    };
    ($out:ty => placeholder!($msg:expr) $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = placeholder!($msg), source_policies = [$($($policy),*)?], ingress_policy = [], observers = [])
    };
    ($out:ty => $handler_head:ident $(:: $handler_tail:ident)* $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = $handler_head $(:: $handler_tail)*, source_policies = [$($($policy),*)?], ingress_policy = [], observers = [])
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!() $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => {
        $crate::__obzenflow_async_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = placeholder!(), source_policies = [$($($policy),*)?], ingress_policy = [], observers = [])
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr) $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => {
        $crate::__obzenflow_async_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = placeholder!($msg), source_policies = [$($($policy),*)?], ingress_policy = [], observers = [])
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => {
        $crate::__obzenflow_async_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = $handler_head $(:: $handler_tail)*, source_policies = [$($($policy),*)?], ingress_policy = [], observers = [])
    };
    (name: $name:literal, $out:ty => placeholder!() $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = placeholder!(), source_policies = [$($($policy),*)?], ingress_policy = [], observers = [])
    };
    (name: $name:literal, $out:ty => placeholder!($msg:expr) $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = placeholder!($msg), source_policies = [$($($policy),*)?], ingress_policy = [], observers = [])
    };
    (name: $name:literal, $out:ty => $handler_head:ident $(:: $handler_tail:ident)* $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = $handler_head $(:: $handler_tail)*, source_policies = [$($($policy),*)?], ingress_policy = [], observers = [])
    };
    // ── typed (binding-derived name) ──
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!()) => {
        $crate::__obzenflow_async_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = "__obzenflow_binding_derived_name__", handler = placeholder!(), middleware = [])
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr)) => {
        $crate::__obzenflow_async_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = "__obzenflow_binding_derived_name__", handler = placeholder!($msg), middleware = [])
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = "__obzenflow_binding_derived_name__", handler = placeholder!(), middleware = [$($mw),*])
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = "__obzenflow_binding_derived_name__", handler = placeholder!($msg), middleware = [$($mw),*])
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*) => {
        $crate::__obzenflow_async_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = "__obzenflow_binding_derived_name__", handler = $handler_head $(:: $handler_tail)*, middleware = [])
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*]) => {
        $crate::__obzenflow_async_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = "__obzenflow_binding_derived_name__", handler = $handler_head $(:: $handler_tail)*, middleware = [$($mw),*])
    };
    ($out:ty => placeholder!()) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = placeholder!(), middleware = [])
    };
    ($out:ty => placeholder!($msg:expr)) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = placeholder!($msg), middleware = [])
    };
    ($out:ty => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = placeholder!(), middleware = [$($mw),*])
    };
    ($out:ty => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = placeholder!($msg), middleware = [$($mw),*])
    };
    ($out:ty => $handler_head:ident $(:: $handler_tail:ident)*) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = $handler_head $(:: $handler_tail)*, middleware = [])
    };
    ($out:ty => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*]) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = $handler_head $(:: $handler_tail)*, middleware = [$($mw),*])
    };

    // ── typed (explicit name override) ──
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!()) => {
        $crate::__obzenflow_async_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = placeholder!(), middleware = [])
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr)) => {
        $crate::__obzenflow_async_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = placeholder!($msg), middleware = [])
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = placeholder!(), middleware = [$($mw),*])
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = placeholder!($msg), middleware = [$($mw),*])
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*) => {
        $crate::__obzenflow_async_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = $handler_head $(:: $handler_tail)*, middleware = [])
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*]) => {
        $crate::__obzenflow_async_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = $handler_head $(:: $handler_tail)*, middleware = [$($mw),*])
    };
    (name: $name:literal, $out:ty => placeholder!()) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = placeholder!(), middleware = [])
    };
    (name: $name:literal, $out:ty => placeholder!($msg:expr)) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = placeholder!($msg), middleware = [])
    };
    (name: $name:literal, $out:ty => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = placeholder!(), middleware = [$($mw),*])
    };
    (name: $name:literal, $out:ty => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = placeholder!($msg), middleware = [$($mw),*])
    };
    (name: $name:literal, $out:ty => $handler_head:ident $(:: $handler_tail:ident)*) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = $handler_head $(:: $handler_tail)*, middleware = [])
    };
    (name: $name:literal, $out:ty => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*]) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = $handler_head $(:: $handler_tail)*, middleware = [$($mw),*])
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => ($handler:expr, $poll_timeout:expr) $(, [$($mw:expr),*])?) => {
        $crate::__obzenflow_async_source_timeout_diagnostic!(
            "async_source!",
            "let source = MySource::builder().poll_timeout(timeout).build()?; events = async_source!(Event => source);"
        )
    };
    ($out:ty => ($handler:expr, $poll_timeout:expr) $(, [$($mw:expr),*])?) => {
        $crate::__obzenflow_async_source_timeout_diagnostic!(
            "async_source!",
            "let source = MySource::builder().poll_timeout(timeout).build()?; events = async_source!(Event => source);"
        )
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => ($handler:expr, $poll_timeout:expr) $(, [$($mw:expr),*])?) => {
        $crate::__obzenflow_async_source_timeout_diagnostic!(
            "async_source!",
            "let source = MySource::builder().poll_timeout(timeout).build()?; events = async_source!(Event => source);"
        )
    };
    (name: $name:literal, $out:ty => ($handler:expr, $poll_timeout:expr) $(, [$($mw:expr),*])?) => {
        $crate::__obzenflow_async_source_timeout_diagnostic!(
            "async_source!",
            "let source = MySource::builder().poll_timeout(timeout).build()?; events = async_source!(Event => source);"
        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => $handler:expr $(, [$($mw:expr),*])?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "async_source!",
            "let source = MyAsyncSource::new(...); events = async_source!(Event => source);"
        )
    };
    ($out:ty => $handler:expr $(, [$($mw:expr),*])?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "async_source!",
            "let source = MyAsyncSource::new(...); events = async_source!(Event => source);"
        )
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => $handler:expr $(, [$($mw:expr),*])?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "async_source!",
            "let source = MyAsyncSource::new(...); events = async_source!(name: \"events\", Event => source);"
        )
    };
    (name: $name:literal, $out:ty => $handler:expr $(, [$($mw:expr),*])?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "async_source!",
            "let source = MyAsyncSource::new(...); events = async_source!(name: \"events\", Event => source);"
        )
    };
}

// ============================================================================
// infinite_source!  +  __obzenflow_infinite_source_typed!
// ============================================================================

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_infinite_source_typed {
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!(), source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*]) => {{
        $crate::__obzenflow_admit_placeholder_source!(factory = placeholder_infinite_source_descriptor, output = $out, output_contract = [$($member),+], name = $name, message = None, source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*])
    }};
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!($msg:expr), source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*]) => {{
        $crate::__obzenflow_admit_placeholder_source!(factory = placeholder_infinite_source_descriptor, output = $out, output_contract = [$($member),+], name = $name, message = Some($msg), source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*])
    }};
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = $handler:expr, source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*]) => {{
        $crate::__obzenflow_admit_typed_source!(factory = typed_infinite_source_descriptor, output = $out, output_contract = [$($member),+], name = $name, handler = $handler, source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*])
    }};
    (output = $out:ty, name = $name:literal, handler = placeholder!(), source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*]) => {{
        $crate::__obzenflow_admit_placeholder_source!(factory = placeholder_infinite_source_descriptor, output = $out, name = $name, message = None, source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*])
    }};
    (output = $out:ty, name = $name:literal, handler = placeholder!($msg:expr), source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*]) => {{
        $crate::__obzenflow_admit_placeholder_source!(factory = placeholder_infinite_source_descriptor, output = $out, name = $name, message = Some($msg), source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*])
    }};
    (output = $out:ty, name = $name:literal, handler = $handler:expr, source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*]) => {{
        $crate::__obzenflow_admit_typed_source!(factory = typed_infinite_source_descriptor, output = $out, name = $name, handler = $handler, source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*])
    }};
}

/// Create an infinite source stage descriptor.
#[macro_export]
macro_rules! __obzenflow_infinite_source_attachment_contract {












    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!() $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => { $crate::__obzenflow_infinite_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = "__obzenflow_binding_derived_name__", handler = placeholder!(), source_policies = [$($($policy),*)?], ingress_policy = [], observers = []) };
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr) $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => { $crate::__obzenflow_infinite_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = "__obzenflow_binding_derived_name__", handler = placeholder!($msg), source_policies = [$($($policy),*)?], ingress_policy = [], observers = []) };
    ({ $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => { $crate::__obzenflow_infinite_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = "__obzenflow_binding_derived_name__", handler = $handler_head $(:: $handler_tail)*, source_policies = [$($($policy),*)?], ingress_policy = [], observers = []) };
    ($out:ty => placeholder!() $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => { $crate::__obzenflow_infinite_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = placeholder!(), source_policies = [$($($policy),*)?], ingress_policy = [], observers = []) };
    ($out:ty => placeholder!($msg:expr) $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => { $crate::__obzenflow_infinite_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = placeholder!($msg), source_policies = [$($($policy),*)?], ingress_policy = [], observers = []) };
    ($out:ty => $handler_head:ident $(:: $handler_tail:ident)* $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => { $crate::__obzenflow_infinite_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = $handler_head $(:: $handler_tail)*, source_policies = [$($($policy),*)?], ingress_policy = [], observers = []) };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!() $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => { $crate::__obzenflow_infinite_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = placeholder!(), source_policies = [$($($policy),*)?], ingress_policy = [], observers = []) };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr) $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => { $crate::__obzenflow_infinite_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = placeholder!($msg), source_policies = [$($($policy),*)?], ingress_policy = [], observers = []) };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => { $crate::__obzenflow_infinite_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = $handler_head $(:: $handler_tail)*, source_policies = [$($($policy),*)?], ingress_policy = [], observers = []) };
    (name: $name:literal, $out:ty => placeholder!() $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => { $crate::__obzenflow_infinite_source_typed!(output = $out, name = $name, handler = placeholder!(), source_policies = [$($($policy),*)?], ingress_policy = [], observers = []) };
    (name: $name:literal, $out:ty => placeholder!($msg:expr) $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => { $crate::__obzenflow_infinite_source_typed!(output = $out, name = $name, handler = placeholder!($msg), source_policies = [$($($policy),*)?], ingress_policy = [], observers = []) };
    (name: $name:literal, $out:ty => $handler_head:ident $(:: $handler_tail:ident)* $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => { $crate::__obzenflow_infinite_source_typed!(output = $out, name = $name, handler = $handler_head $(:: $handler_tail)*, source_policies = [$($($policy),*)?], ingress_policy = [], observers = []) };
    // ── typed (binding-derived name) ──
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!()) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!(),
            middleware = []        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr)) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!($msg),
            middleware = []        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!(),
            middleware = [$($mw),*]        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!($msg),
            middleware = [$($mw),*]        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = "__obzenflow_binding_derived_name__",
            handler = $handler_head $(:: $handler_tail)*,
            middleware = []        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*]) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = "__obzenflow_binding_derived_name__",
            handler = $handler_head $(:: $handler_tail)*,
            middleware = [$($mw),*]        )
    };
    ($out:ty => placeholder!()) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!(),
            middleware = []        )
    };
    ($out:ty => placeholder!($msg:expr)) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!($msg),
            middleware = []        )
    };
    ($out:ty => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!(),
            middleware = [$($mw),*]        )
    };
    ($out:ty => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!($msg),
            middleware = [$($mw),*]        )
    };
    ($out:ty => $handler_head:ident $(:: $handler_tail:ident)*) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = $handler_head $(:: $handler_tail)*,
            middleware = []        )
    };
    ($out:ty => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*]) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = $handler_head $(:: $handler_tail)*,
            middleware = [$($mw),*]        )
    };

    // ── typed (explicit name override) ──
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!()) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!(),
            middleware = []        )
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr)) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!($msg),
            middleware = []        )
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!(),
            middleware = [$($mw),*]        )
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!($msg),
            middleware = [$($mw),*]        )
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = []        )
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*]) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = [$($mw),*]        )
    };
    (name: $name:literal, $out:ty => placeholder!()) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = placeholder!(),
            middleware = []        )
    };
    (name: $name:literal, $out:ty => placeholder!($msg:expr)) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = placeholder!($msg),
            middleware = []        )
    };
    (name: $name:literal, $out:ty => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = placeholder!(),
            middleware = [$($mw),*]        )
    };
    (name: $name:literal, $out:ty => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = placeholder!($msg),
            middleware = [$($mw),*]        )
    };
    (name: $name:literal, $out:ty => $handler_head:ident $(:: $handler_tail:ident)*) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = []        )
    };
    (name: $name:literal, $out:ty => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*]) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = [$($mw),*]        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => $handler:expr $(, [$($mw:expr),*])?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "infinite_source!",
            "let source = MyInfiniteSource::new(...); events = infinite_source!(Event => source);"
        )
    };
    ($out:ty => $handler:expr $(, [$($mw:expr),*])?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "infinite_source!",
            "let source = MyInfiniteSource::new(...); events = infinite_source!(Event => source);"
        )
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => $handler:expr $(, [$($mw:expr),*])?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "infinite_source!",
            "let source = MyInfiniteSource::new(...); events = infinite_source!(name: \"events\", Event => source);"
        )
    };
    (name: $name:literal, $out:ty => $handler:expr $(, [$($mw:expr),*])?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "infinite_source!",
            "let source = MyInfiniteSource::new(...); events = infinite_source!(name: \"events\", Event => source);"
        )
    };
}

// ============================================================================
// async_infinite_source!  +  __obzenflow_async_infinite_source_typed!
// ============================================================================

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_async_infinite_source_typed {
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!(), source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*]) => {{
        $crate::__obzenflow_admit_placeholder_source!(factory = placeholder_async_infinite_source_descriptor, output = $out, output_contract = [$($member),+], name = $name, message = None, source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*])
    }};
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!($msg:expr), source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*]) => {{
        $crate::__obzenflow_admit_placeholder_source!(factory = placeholder_async_infinite_source_descriptor, output = $out, output_contract = [$($member),+], name = $name, message = Some($msg), source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*])
    }};
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = $handler:expr, source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*]) => {{
        $crate::__obzenflow_admit_typed_source!(factory = typed_async_infinite_source_descriptor, output = $out, output_contract = [$($member),+], name = $name, handler = $handler, source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*])
    }};
    (output = $out:ty, name = $name:literal, handler = placeholder!(), source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*]) => {{
        $crate::__obzenflow_admit_placeholder_source!(factory = placeholder_async_infinite_source_descriptor, output = $out, name = $name, message = None, source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*])
    }};
    (output = $out:ty, name = $name:literal, handler = placeholder!($msg:expr), source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*]) => {{
        $crate::__obzenflow_admit_placeholder_source!(factory = placeholder_async_infinite_source_descriptor, output = $out, name = $name, message = Some($msg), source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*])
    }};
    (output = $out:ty, name = $name:literal, handler = $handler:expr, source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*]) => {{
        $crate::__obzenflow_admit_typed_source!(factory = typed_async_infinite_source_descriptor, output = $out, name = $name, handler = $handler, source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*])
    }};
}

/// Create an async infinite source stage descriptor.
#[macro_export]
macro_rules! __obzenflow_async_infinite_source_attachment_contract {












    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!() $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => { $crate::__obzenflow_async_infinite_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = "__obzenflow_binding_derived_name__", handler = placeholder!(), source_policies = [$($($policy),*)?], ingress_policy = [], observers = []) };
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr) $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => { $crate::__obzenflow_async_infinite_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = "__obzenflow_binding_derived_name__", handler = placeholder!($msg), source_policies = [$($($policy),*)?], ingress_policy = [], observers = []) };
    ({ $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => { $crate::__obzenflow_async_infinite_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = "__obzenflow_binding_derived_name__", handler = $handler_head $(:: $handler_tail)*, source_policies = [$($($policy),*)?], ingress_policy = [], observers = []) };
    ($out:ty => placeholder!() $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => { $crate::__obzenflow_async_infinite_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = placeholder!(), source_policies = [$($($policy),*)?], ingress_policy = [], observers = []) };
    ($out:ty => placeholder!($msg:expr) $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => { $crate::__obzenflow_async_infinite_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = placeholder!($msg), source_policies = [$($($policy),*)?], ingress_policy = [], observers = []) };
    ($out:ty => $handler_head:ident $(:: $handler_tail:ident)* $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => { $crate::__obzenflow_async_infinite_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = $handler_head $(:: $handler_tail)*, source_policies = [$($($policy),*)?], ingress_policy = [], observers = []) };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!() $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => { $crate::__obzenflow_async_infinite_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = placeholder!(), source_policies = [$($($policy),*)?], ingress_policy = [], observers = []) };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr) $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => { $crate::__obzenflow_async_infinite_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = placeholder!($msg), source_policies = [$($($policy),*)?], ingress_policy = [], observers = []) };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => { $crate::__obzenflow_async_infinite_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = $handler_head $(:: $handler_tail)*, source_policies = [$($($policy),*)?], ingress_policy = [], observers = []) };
    (name: $name:literal, $out:ty => placeholder!() $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => { $crate::__obzenflow_async_infinite_source_typed!(output = $out, name = $name, handler = placeholder!(), source_policies = [$($($policy),*)?], ingress_policy = [], observers = []) };
    (name: $name:literal, $out:ty => placeholder!($msg:expr) $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => { $crate::__obzenflow_async_infinite_source_typed!(output = $out, name = $name, handler = placeholder!($msg), source_policies = [$($($policy),*)?], ingress_policy = [], observers = []) };
    (name: $name:literal, $out:ty => $handler_head:ident $(:: $handler_tail:ident)* $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => { $crate::__obzenflow_async_infinite_source_typed!(output = $out, name = $name, handler = $handler_head $(:: $handler_tail)*, source_policies = [$($($policy),*)?], ingress_policy = [], observers = []) };
    // ── typed (binding-derived name) ──
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!()) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!(),
            middleware = []        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr)) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!($msg),
            middleware = []        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!(),
            middleware = [$($mw),*]        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!($msg),
            middleware = [$($mw),*]        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = "__obzenflow_binding_derived_name__",
            handler = $handler_head $(:: $handler_tail)*,
            middleware = []        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*]) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = "__obzenflow_binding_derived_name__",
            handler = $handler_head $(:: $handler_tail)*,
            middleware = [$($mw),*]        )
    };
    ($out:ty => placeholder!()) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!(),
            middleware = []        )
    };
    ($out:ty => placeholder!($msg:expr)) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!($msg),
            middleware = []        )
    };
    ($out:ty => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!(),
            middleware = [$($mw),*]        )
    };
    ($out:ty => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!($msg),
            middleware = [$($mw),*]        )
    };
    ($out:ty => $handler_head:ident $(:: $handler_tail:ident)*) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = $handler_head $(:: $handler_tail)*,
            middleware = []        )
    };
    ($out:ty => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*]) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = $handler_head $(:: $handler_tail)*,
            middleware = [$($mw),*]        )
    };

    // ── typed (explicit name override) ──
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!()) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!(),
            middleware = []        )
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr)) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!($msg),
            middleware = []        )
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!(),
            middleware = [$($mw),*]        )
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!($msg),
            middleware = [$($mw),*]        )
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = []        )
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*]) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = [$($mw),*]        )
    };
    (name: $name:literal, $out:ty => placeholder!()) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = placeholder!(),
            middleware = []        )
    };
    (name: $name:literal, $out:ty => placeholder!($msg:expr)) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = placeholder!($msg),
            middleware = []        )
    };
    (name: $name:literal, $out:ty => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = placeholder!(),
            middleware = [$($mw),*]        )
    };
    (name: $name:literal, $out:ty => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = placeholder!($msg),
            middleware = [$($mw),*]        )
    };
    (name: $name:literal, $out:ty => $handler_head:ident $(:: $handler_tail:ident)*) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = []        )
    };
    (name: $name:literal, $out:ty => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*]) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = [$($mw),*]        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => ($handler:expr, $poll_timeout:expr) $(, [$($mw:expr),*])?) => {
        $crate::__obzenflow_async_source_timeout_diagnostic!(
            "async_infinite_source!",
            "let source = MySource::builder().poll_timeout(timeout).build()?; events = async_infinite_source!(Event => source);"
        )
    };
    ($out:ty => ($handler:expr, $poll_timeout:expr) $(, [$($mw:expr),*])?) => {
        $crate::__obzenflow_async_source_timeout_diagnostic!(
            "async_infinite_source!",
            "let source = MySource::builder().poll_timeout(timeout).build()?; events = async_infinite_source!(Event => source);"
        )
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => ($handler:expr, $poll_timeout:expr) $(, [$($mw:expr),*])?) => {
        $crate::__obzenflow_async_source_timeout_diagnostic!(
            "async_infinite_source!",
            "let source = MySource::builder().poll_timeout(timeout).build()?; events = async_infinite_source!(Event => source);"
        )
    };
    (name: $name:literal, $out:ty => ($handler:expr, $poll_timeout:expr) $(, [$($mw:expr),*])?) => {
        $crate::__obzenflow_async_source_timeout_diagnostic!(
            "async_infinite_source!",
            "let source = MySource::builder().poll_timeout(timeout).build()?; events = async_infinite_source!(Event => source);"
        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => $handler:expr $(, [$($mw:expr),*])?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "async_infinite_source!",
            "let source = MyAsyncInfiniteSource::new(...); events = async_infinite_source!(Event => source);"
        )
    };
    ($out:ty => $handler:expr $(, [$($mw:expr),*])?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "async_infinite_source!",
            "let source = MyAsyncInfiniteSource::new(...); events = async_infinite_source!(Event => source);"
        )
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => $handler:expr $(, [$($mw:expr),*])?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "async_infinite_source!",
            "let source = MyAsyncInfiniteSource::new(...); events = async_infinite_source!(name: \"events\", Event => source);"
        )
    };
    (name: $name:literal, $out:ty => $handler:expr $(, [$($mw:expr),*])?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "async_infinite_source!",
            "let source = MyAsyncInfiniteSource::new(...); events = async_infinite_source!(name: \"events\", Event => source);"
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
        const _: () = $crate::__private::assert_distinct_stage_fact_set::<
            $crate::__private::stage_fact_set![$($member),+],
        >();
        let __observers: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($mw)),*];        $crate::dsl::typing::placeholder_transform_descriptor::<
            $in,
            $out,
            $crate::__private::stage_fact_set![$($member),+],
            _,
        >($name, None, __observers)
    }};
    (input = exact($in:ty), output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!($msg:expr), middleware = [$($mw:expr),*]) => {{
        const _: () = $crate::__private::assert_distinct_stage_fact_set::<
            $crate::__private::stage_fact_set![$($member),+],
        >();
        let __observers: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($mw)),*];        $crate::dsl::typing::placeholder_transform_descriptor::<
            $in,
            $out,
            $crate::__private::stage_fact_set![$($member),+],
            _,
        >($name, Some($msg), __observers)
    }};
    // -- exact input, real handler, explicit output contract --
    // FLOWIP-120b Option B keeps the flat output contract in the arrow's
    // type-signature position; the handler-side carrier is never named in the
    // flow.
    (input = exact($in:ty), output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*]) => {{
        let __handler = $handler;
        const _: () = $crate::__private::assert_distinct_stage_fact_set::<
            $crate::__private::stage_fact_set![$($member),+],
        >();
        let __observers: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($mw)),*];        $crate::dsl::typing::typed_transform_descriptor::<
            _,
            $in,
            $out,
            $crate::__private::stage_fact_set![$($member),+],
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

/// Generated-chat row parser for `inference!` and `ai_map_reduce!`, whose rows
/// still name `at_least_once(ChatCompletion) via <chat binding>`.
#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_effect_entries {
    (@generated_chat surface = $surface:tt, row = {
        at_least_once(ChatCompletion) via $binding:ident with { $($members:tt)* } $(,)?
    }) => {{
        $crate::__obzenflow_flat_attachment_group!($($members)*);
        $crate::__obzenflow_effect_entries!(@generated_chat_group surface = $surface,
            row = { at_least_once(ChatCompletion) via $binding with { $($members)* } })
    }};
(@generated_chat_group surface = $surface:tt, row = {
        at_least_once(ChatCompletion)
            via $binding:ident
            with { $($policy:expr),+ $(,)? }
        $(,)?
    }) => {{
        let __chat_binding: $crate::__private::EffectBinding<
            $crate::__private::ChatCompletion,
        > = $crate::__obzenflow_clone_ai_chat_contract!($surface, $binding);
        let mut __chat_declarations: Vec<
            $crate::__private::EffectDeclaration,
        > = Vec::new();
        let mut __chat_policy_attachments: Vec<
            $crate::dsl::stage_descriptor::EffectPolicyAttachment,
        > = Vec::new();
        $crate::__obzenflow_effect_entries!(
            @entry __chat_declarations,
            __chat_policy_attachments,
            [],
            at_least_once($crate::__private::ChatCompletion)
                via __chat_binding
                with { $($policy),+ }
        );
        $crate::dsl::ai_effect::GeneratedChatEffectRow {
            binding: __chat_binding,
            declarations: __chat_declarations,
            policy_attachments: __chat_policy_attachments,
            implementation_middleware: Vec::new(),
        }
    }};
(@generated_chat_then surface = $surface:tt, row = {
        at_least_once(ChatCompletion)
            via $binding:ident
            with { $($policy:expr),+ $(,)? }
        $(,)?
    }, then = { $($then:tt)* }) => {
        $($then)*
    };
(@entry $effects:ident, $atts:ident, [], at_least_once($effect:ty) via $binding:ident with { $($policy:expr),+ $(,)? }) => {
        $effects.push($crate::__private::declare_named_at_least_once_effect::<$effect, _>(&$binding));
        $crate::__obzenflow_effect_entries!(@attach_group $atts, $effect, [$($policy),+]);
    };

    // Generated AI surfaces accept one constrained row, but lower it through
    // the same declaration and attachment rules as ordinary effectful stages.

    (@generated_chat surface = $surface:tt, row = {
        at_least_once(ChatCompletion)
            via $binding:ident
            with retry($($retry:tt)*)
        $(,)?
    }) => {
        compile_error!(concat!(
            $surface,
            ": ChatCompletion is NonIdempotentAtLeastOnce; retry is forbidden"
        ))
    };

    (@generated_chat surface = $surface:tt, row = {
        at_least_once(ChatCompletion)
            via $binding:ident
            with $policy:expr
        $(,)?
    }) => {{
        let __chat_binding: $crate::__private::EffectBinding<
            $crate::__private::ChatCompletion,
        > = $crate::__obzenflow_clone_ai_chat_contract!($surface, $binding);
        let mut __chat_declarations: Vec<
            $crate::__private::EffectDeclaration,
        > = Vec::new();
        let mut __chat_policy_attachments: Vec<
            $crate::dsl::stage_descriptor::EffectPolicyAttachment,
        > = Vec::new();
        $crate::__obzenflow_effect_entries!(
            @entry __chat_declarations,
            __chat_policy_attachments,
            [],
            at_least_once($crate::__private::ChatCompletion)
                via __chat_binding
                with $policy
        );
        $crate::dsl::ai_effect::GeneratedChatEffectRow {
            binding: __chat_binding,
            declarations: __chat_declarations,
            policy_attachments: __chat_policy_attachments,
            implementation_middleware: Vec::new(),
        }
    }};
    (@generated_chat surface = $surface:tt, row = { ChatCompletion $($rest:tt)* }) => {
        compile_error!(concat!(
            $surface,
            ": paid non-idempotent ChatCompletion requires \
             `at_least_once(ChatCompletion)` acknowledgement"
        ))
    };
    (@generated_chat surface = $surface:tt, row = { transactional(ChatCompletion) $($rest:tt)* }) => {
        compile_error!(concat!(
            $surface,
            ": ChatCompletion accepts only `at_least_once(ChatCompletion)`"
        ))
    };
    (@generated_chat surface = $surface:tt, row = { $($invalid:tt)* }) => {
        compile_error!(concat!(
            $surface,
            ": expected `at_least_once(ChatCompletion) via <chat binding> \
             with <middleware>`"
        ))
    };

    // Preserve row diagnostics before the non-path role diagnostic without
    // maintaining a second generated-row parser.

    (@generated_chat_then surface = $surface:tt, row = {
        at_least_once(ChatCompletion)
            via $binding:ident
            with retry($($retry:tt)*)
        $(,)?
    }, then = { $($then:tt)* }) => {
        $crate::__obzenflow_effect_entries!(
            @generated_chat surface = $surface,
            row = { at_least_once(ChatCompletion) via $binding with retry($($retry)*) }
        )
    };
    (@generated_chat_then surface = $surface:tt, row = {
        at_least_once(ChatCompletion)
            via $binding:ident
            with $policy:expr
        $(,)?
    }, then = { $($then:tt)* }) => {
        $($then)*
    };
    (@generated_chat_then surface = $surface:tt, row = { $($invalid:tt)* }, then = { $($then:tt)* }) => {
        $crate::__obzenflow_effect_entries!(
            @generated_chat surface = $surface,
            row = { $($invalid)* }
        )
    };

    (@entry $effects:ident, $atts:ident, [], at_least_once($effect:ty) via $binding:ident with $policy:expr) => {
        $effects.push($crate::__private::declare_named_at_least_once_effect::<$effect, _>(&$binding));
        $crate::__obzenflow_effect_entries!(@attach $atts, $effect, $policy);
    };

    (@attach_group $atts:ident, $effect:ty, [$($policy:expr),+]) => {
        $( $crate::__obzenflow_effect_entries!(@attach $atts, $effect, $policy); )+
    };
    (@attach $atts:ident, $effect:ty, $policy:expr) => {{
        let __effect_type: &'static str =
            <$effect as $crate::__private::Effect>::EFFECT_TYPE;
        let __factory: Box<dyn $crate::__private::MiddlewareFactory> = $crate::__private::capture_middleware($policy);
        $atts.push($crate::dsl::stage_descriptor::EffectPolicyAttachment {
            effect_type: __effect_type,
            factory: __factory,
            authored_site: $crate::__private::MiddlewareAttachmentSite::Effect,
        });
    }};
}

/// Enforce the public `uses` delimiter law before lowering an effect entry.
/// A bare clause denotes exactly one entry; a top-level comma means the
/// author must use the unordered braced set form.
#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_single_effect_uses_gate {
    (effects = [$($effects:tt)*], then = [$($then:tt)*]) => {
        $crate::__obzenflow_single_effect_uses_gate!(
            @scan
            seen = no,
            remaining = [$($effects)*],
            then = [$($then)*]
        )
    };
    (@scan seen = no, remaining = [], then = [$($then:tt)*]) => {
        compile_error!("`uses` requires one effect; omit the clause for an effect-free stage")
    };
    (@scan seen = yes, remaining = [], then = [$($then:tt)*]) => {
        $($then)*
    };
    (@scan seen = $seen:ident, remaining = [, $($rest:tt)*], then = [$($then:tt)*]) => {
        compile_error!("multiple effects require an unordered set; write `uses { EffectA, EffectB }`")
    };
    (@scan seen = $seen:ident, remaining = [$next:tt $($rest:tt)*], then = [$($then:tt)*]) => {
        $crate::__obzenflow_single_effect_uses_gate!(
            @scan
            seen = yes,
            remaining = [$($rest)*],
            then = [$($then)*]
        )
    };
}

/// Braces after `uses` are reserved for a genuine unordered set containing
/// at least two entries. Commas inside grouped Rust syntax are not visible to
/// this scanner, matching the effect-entry parser's existing requirement that
/// generic types with top-level commas be parenthesised.
#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_multi_effect_uses_gate {
    (effects = [$($effects:tt)*], then = [$($then:tt)*]) => {
        $crate::__obzenflow_multi_effect_uses_gate!(
            @scan
            seen = no,
            remaining = [$($effects)*],
            then = [$($then)*]
        )
    };
    (@scan seen = no, remaining = [], then = [$($then:tt)*]) => {
        compile_error!("empty effect sets are not a purity marker; write `Input -> Output => handler`")
    };
    (@scan seen = yes, remaining = [], then = [$($then:tt)*]) => {
        compile_error!("a single effect must be written bare; write `uses Effect`")
    };
    (@scan seen = yes, remaining = [,], then = [$($then:tt)*]) => {
        compile_error!("a single effect must be written bare; write `uses Effect`")
    };
    (@scan seen = yes, remaining = [, $next:tt $($rest:tt)*], then = [$($then:tt)*]) => {
        $($then)*
    };
    (@scan seen = $seen:ident, remaining = [$next:tt $($rest:tt)*], then = [$($then:tt)*]) => {
        $crate::__obzenflow_multi_effect_uses_gate!(
            @scan
            seen = yes,
            remaining = [$($rest)*],
            then = [$($then)*]
        )
    };
}

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
            <$crate::__private::stage_fact_set![$($member),+] as $crate::__private::StageFactSet>::Members:
                $crate::dsl::typing::ArrowOutputsAreDeclaredByHandler<
                    <<H as $crate::__private::EffectfulTransformHandler>::Output as $crate::__private::StageFactSet>::Members,
                    ArrowToHandlerProof,
                >,
            <<H as $crate::__private::EffectfulTransformHandler>::Output as $crate::__private::StageFactSet>::Members:
                $crate::dsl::typing::HandlerOutputsAreDeclaredByArrow<
                    <$crate::__private::stage_fact_set![$($member),+] as $crate::__private::StageFactSet>::Members,
                    HandlerToArrowProof,
                >,
        {}

        __obzenflow_assert_effectful_transform_contract::<_, _, _>(&$handler);
        const _: () = $crate::__private::assert_distinct_stage_fact_set::<
            $crate::__private::stage_fact_set![$($member),+],
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
            <$crate::__private::stage_fact_set![$($member),+] as $crate::__private::StageFactSet>::Members:
                $crate::dsl::typing::ArrowOutputsAreDeclaredByHandler<
                    <<H as $crate::__private::EffectfulStatefulHandler>::Output as $crate::__private::StageFactSet>::Members,
                    ArrowToHandlerProof,
                >,
            <<H as $crate::__private::EffectfulStatefulHandler>::Output as $crate::__private::StageFactSet>::Members:
                $crate::dsl::typing::HandlerOutputsAreDeclaredByArrow<
                    <$crate::__private::stage_fact_set![$($member),+] as $crate::__private::StageFactSet>::Members,
                    HandlerToArrowProof,
                >,
        {}

        __obzenflow_assert_effectful_stateful_contract::<_, _, _>(&$handler);
        const _: () = $crate::__private::assert_distinct_stage_fact_set::<
            $crate::__private::stage_fact_set![$($member),+],
        >();
    }};
}

/// Lower an effectful row. The handler's effect set declares the stage's
/// effects, and the systems named after the handler answer its named effects.
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
    (@handler $family:ident, name = $name:literal, in = [$($in:tt)+], out = [{ $first:ty $(, $member:ty)* $(,)? }], $handler_head:ident $(:: $handler_tail:ident)* @attachments [$($mw:expr),* $(,)?] @acks [$($acks:tt)*]) => {
        $crate::__obzenflow_effectful_lower!(
            $family,
            input = exact($($in)+),
            output = $first,
            members = [$first $(, $member)*],
            contract = [$first $(, $member)*],
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = [$($mw),*],
            acks = [$($acks)*]
        )
    };
    (@handler $family:ident, name = $name:literal, in = [$($in:tt)+], out = [$out:ty], $handler_head:ident $(:: $handler_tail:ident)* @attachments [$($mw:expr),* $(,)?] @acks [$($acks:tt)*]) => {
        $crate::__obzenflow_effectful_lower!(
            $family,
            input = exact($($in)+),
            output = $out,
            members = [$out],
            contract = single,
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = [$($mw),*],
            acks = [$($acks)*]
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
    (transform, input = exact($in:ty), output = $out:ty, members = [$($member:ty),+], contract = $contract:tt, name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*], acks = [$($acks:tt)*]) => {{
        let __handler = $handler;
        $crate::__obzenflow_assert_effectful_transform_contract!(__handler, $in, [$($member),+]);
        let __effects =
            $crate::__private::effectful_transform_effects(&__handler, &[$($acks)*]);
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
    (stateful, input = exact($in:ty), output = $out:ty, members = [$($member:ty),+], contract = $contract:tt, name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*], acks = [$($acks:tt)*]) => {{
        let __handler = $handler;
        $crate::__obzenflow_assert_effectful_stateful_contract!(__handler, $in, [$($member),+]);
        let __effects =
            $crate::__private::effectful_stateful_effects(&__handler, &[$($acks)*]);
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
    // ── exact input, placeholder ──
    (input = exact($in:ty), name = $name:literal, handler = placeholder!(), sink_policies = [$($policy:expr),*], observers = [$($observer:expr),*]) => {{
        $crate::dsl::typing::placeholder_sink_descriptor::<$in>(
            $name,
            None,
            vec![$($crate::__private::capture_middleware($policy)),*],
            vec![$($crate::__private::capture_middleware($observer)),*],
        )
    }};
    (input = exact($in:ty), name = $name:literal, handler = placeholder!($msg:expr), sink_policies = [$($policy:expr),*], observers = [$($observer:expr),*]) => {{
        $crate::dsl::typing::placeholder_sink_descriptor::<$in>(
            $name,
            Some($msg),
            vec![$($crate::__private::capture_middleware($policy)),*],
            vec![$($crate::__private::capture_middleware($observer)),*],
        )
    }};

    // ── exact input, real handler (facade call anchoring) ──
    //
    // Like joins, sink facade helpers often need the contract type injected to avoid
    // turbofish/annotations at the call site (e.g., `sinks::ConsoleSink::new(sinks::JsonFormatter)` and `sinks::ConsoleSink::new(sinks::TableFormatter::new(...)).batch_size(256)?`).

    // ── exact input, real handler ──
    //
    (input = exact($in:ty), name = $name:literal, handler = $handler:expr, sink_policies = [$($policy:expr),*], observers = [$($observer:expr),*]) => {{
        let __handler = $handler;
        $crate::dsl::typing::typed_sink_descriptor::<_, $in>(
            $name,
            __handler,
            vec![$($crate::__private::capture_middleware($policy)),*],
            vec![$($crate::__private::capture_middleware($observer)),*],
        )
    }};
}

/// Lower a sink's `.idempotent()` or `.non_idempotent()` decoration. Routes
/// through the sealed `SetSinkRedeliverySafety` trait so the decoration is
/// accepted only for a `SinkConnector`, including the inline tier, and wraps
/// its description before the DSL snapshots it.
#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_sink_delivery {
    ($handler:expr) => {
        $handler
    };
    ($handler:expr, idempotent) => {
        $crate::__private::SetSinkRedeliverySafety::safe_to_repeat($handler)
    };
    ($handler:expr, non_idempotent) => {
        $crate::__private::SetSinkRedeliverySafety::duplicate_sensitive($handler)
    };
}

/// Render the closed binding-name set used by a config-selected `sink!`
/// diagnostic.
#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_sink_selection_expected {
    ($only:ident) => {
        concat!("\"", stringify!($only), "\"")
    };
    ($first:ident, $second:ident) => {
        concat!(
            "\"",
            stringify!($first),
            "\" or \"",
            stringify!($second),
            "\""
        )
    };
    ($first:ident, $second:ident, $($rest:ident),+) => {
        concat!(
            "one of \"",
            stringify!($first),
            "\", \"",
            stringify!($second),
            "\"",
            $(", \"", stringify!($rest), "\"")+
        )
    };
}

/// Apply the enclosing sink site's delivery declaration to one selected
/// handler constructor. The token group is carried intact through the
/// heterogeneous branch repetition so every arm receives the same site law.
#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_sink_handler_set_delivery {
    ($handler:expr, []) => {
        $handler
    };
    ($handler:expr, [idempotent]) => {
        $crate::__obzenflow_sink_delivery!($handler, idempotent)
    };
    ($handler:expr, [non_idempotent]) => {
        $crate::__obzenflow_sink_delivery!($handler, non_idempotent)
    };
}

/// Lower one branch of a selected sink through the ordinary typed sink
/// boundary. Keeping this helper branch-local is what makes every authored
/// alternative satisfy the same arrow-input witness before descriptor erasure.
#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_sink_handler_set_branch {
    (
        input = exact($in:ty),
        name = $name:literal,
        handler = $handler:expr,
        delivery = $delivery:tt,
        sink_policies = [$($policy:expr),*],
        observers = [$($observer:expr),*]
    ) => {
        $crate::__obzenflow_sink_typed!(
            input = exact($in),
            name = $name,
            handler = $crate::__obzenflow_sink_handler_set_delivery!($handler, $delivery),
            sink_policies = [$($policy),*],
            observers = [$($observer),*]
        )
    };
}

/// Sink-owned lowering for the syntax-only `handler_set!` operand.
#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_sink_handler_set {
    (
        input = exact($in:ty),
        name = $name:literal,
        handlers = [$($handler:ident),+ $(,)?],
        delivery = $delivery:tt,
        sink_policies = $sink_policies:tt,
        observers = $observers:tt
    ) => {{
        const _: () = assert!(
            $crate::dsl::stage_descriptor::configured_sink_handler_keys_are_unique(&[
                $(stringify!($handler)),+
            ]),
            "sink!: handler_set! binding identifiers must be unique"
        );
        let __obzenflow_configured_sink: ::std::boxed::Box<
            dyn $crate::dsl::stage_descriptor::StageDescriptor,
        > = $crate::dsl::stage_descriptor::configured_sink_descriptor(
            $name,
            &[$(stringify!($handler)),+],
            $crate::__obzenflow_sink_selection_expected!($($handler),+),
            move |__obzenflow_selected_key: &str| {
                match __obzenflow_selected_key {
                    $(
                        stringify!($handler) => ::core::option::Option::Some(
                            $crate::__obzenflow_sink_handler_set_branch!(
                                input = exact($in),
                                name = $name,
                                handler = $handler,
                                delivery = $delivery,
                                sink_policies = $sink_policies,
                                observers = $observers
                            )
                        ),
                    )+
                    _ => ::core::option::Option::None,
                }
            },
        );
        ::core::result::Result::<
            ::std::boxed::Box<dyn $crate::dsl::stage_descriptor::StageDescriptor>,
            $crate::dsl::FlowBuildError,
        >::Ok(__obzenflow_configured_sink)
    }};
}

/// A closed set of cold sink bindings consumed by `sink!`.
///
/// This macro is a syntax-only operand. `sink!` must provide the declared input
/// witness and site clauses before any selected branch can lower to a descriptor.
/// Each binding identifier is also its exact deployment-configuration key.
/// A standalone invocation is deliberately rejected:
///
/// ```compile_fail
/// use obzenflow_dsl::handler_set;
///
/// let _ = handler_set!(console_sink, postgres_sink);
/// ```
#[macro_export]
macro_rules! handler_set {
    ($($tokens:tt)*) => {
        compile_error!(
            "handler_set! is a sink! operand, not a standalone handler; use \
             sink!(Input => handler_set!(console_sink, postgres_sink))"
        )
    };
}

/// Create a sink stage descriptor.
///
/// Canonical grammar: `InputType with middleware => handler_path`, where
/// `with middleware` or `with { a, b }` is optional and holds controls and
/// passive observers, and the handler may be followed by `.idempotent()` or
/// `.non_idempotent()`. Configure a built-in or named trait implementation in
/// ordinary Rust inside the materialiser, then pass the resulting binding by
/// path.
///
/// The config-selected form composes the direct surface with a syntax-only
/// handler set:
/// `InputType => handler_set!(primary_sink, audit_sink)`. Framework
/// configuration resolves one binding name for the logical sink stage. Only
/// that binding lowers to an ordinary descriptor, while every branch is
/// checked through the same exact input witness before erasure.
/// An incompatible unselected binding is therefore still a compile error:
///
/// ```compile_fail
/// use obzenflow_core::TypedPayload;
/// use obzenflow_dsl::sink;
/// use obzenflow_adapters::sinks::{ConsoleSink, JsonFormatter};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Clone, Debug, Deserialize, Serialize)]
/// struct Output;
/// impl TypedPayload for Output {
///     const EVENT_TYPE: &'static str = "docs.handler-set.output";
/// }
/// #[derive(Clone, Debug, Deserialize, Serialize)]
/// struct Wrong;
/// impl TypedPayload for Wrong {
///     const EVENT_TYPE: &'static str = "docs.handler-set.wrong";
/// }
///
/// let output_sink = ConsoleSink::<Output, _>::new(JsonFormatter);
/// let wrong_sink = ConsoleSink::<Wrong, _>::new(JsonFormatter);
/// let _ = sink!(Output => handler_set!(output_sink, wrong_sink));
/// ```
/// Duplicate binding names are also a compile error rather than an ambiguous
/// deployment key:
///
/// ```compile_fail
/// use obzenflow_core::TypedPayload;
/// use obzenflow_dsl::sink;
/// use obzenflow_adapters::sinks::{ConsoleSink, JsonFormatter};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Clone, Debug, Deserialize, Serialize)]
/// struct Output;
/// impl TypedPayload for Output {
///     const EVENT_TYPE: &'static str = "docs.handler-set.duplicate";
/// }
///
/// let output_sink = ConsoleSink::<Output, _>::new(JsonFormatter);
/// let _ = sink!(Output => handler_set!(output_sink, output_sink));
/// ```
/// The earlier uncomposed spelling receives a migration diagnostic:
///
/// ```compile_fail
/// use obzenflow_dsl::sink;
///
/// struct Output;
/// let _ = sink!(
///     Output => select("console".to_owned()) {
///         "console" => console_handler,
///     }
/// );
/// ```
#[macro_export]
macro_rules! __obzenflow_sink_attachment_contract {
    // FLOWIP-010o B1/B2: framework configuration selects one binding name
    // from a compile-time-closed handler set. The enclosing sink owns the
    // arrow witness and all site metadata; the set disappears before topology.
    ($in:ty => handler_set!($($handler:ident),+ $(,)?)
        $(@attachments [$($policy:expr),* $(,)?])?
        $(@delivery $delivery:ident)?
    ) => {
        $crate::__obzenflow_sink_handler_set!(
            input = exact($in),
            name = "__obzenflow_binding_derived_name__",
            handlers = [$($handler),+],
            delivery = [$($delivery)?],
            sink_policies = [$($($policy),*)?],
            observers = []
        )
    };
    (name: $name:literal, $in:ty => handler_set!($($handler:ident),+ $(,)?)
        $(@attachments [$($policy:expr),* $(,)?])?
        $(@delivery $delivery:ident)?
    ) => {
        $crate::__obzenflow_sink_handler_set!(
            input = exact($in),
            name = $name,
            handlers = [$($handler),+],
            delivery = [$($delivery)?],
            sink_policies = [$($($policy),*)?],
            observers = []
        )
    };
    ($in:ty => handler_set!() $($rest:tt)*) => {
        compile_error!("sink!: handler_set! requires one or more sink binding identifiers")
    };
    (name: $name:literal, $in:ty => handler_set!() $($rest:tt)*) => {
        compile_error!("sink!: handler_set! requires one or more sink binding identifiers")
    };
    ($in:ty => handler_set!($($handler_set:tt)*) $($rest:tt)*) => {
        compile_error!(
            "sink!: expected `handler_set!(primary_sink, audit_sink)`; each alternative must be a unique binding identifier, and that identifier is its exact config key"
        )
    };
    (name: $name:literal, $in:ty => handler_set!($($handler_set:tt)*) $($rest:tt)*) => {
        compile_error!(
            "sink!: expected `handler_set!(primary_sink, audit_sink)`; each alternative must be a unique binding identifier, and that identifier is its exact config key"
        )
    };
    ($in:ty => select($selector:expr) {$($arms:tt)*} $($rest:tt)*) => {
        compile_error!(
            "sink!: selected handlers compose through handler_set!; use \
             sink!(Input => handler_set!(primary_sink, audit_sink))"
        )
    };
    (name: $name:literal, $in:ty => select($selector:expr) {$($arms:tt)*} $($rest:tt)*) => {
        compile_error!(
            "sink!: selected handlers compose through handler_set!; use \
             sink!(name: \"output\", Input => handler_set!(primary_sink, audit_sink))"
        )
    };


    // ── typed (binding-derived name): exact input ──
    ($in:ty => placeholder!() $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = "__obzenflow_binding_derived_name__", handler = placeholder!(), sink_policies = [$($($policy),*)?], observers = [])
    };
    ($in:ty => placeholder!($msg:expr) $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = "__obzenflow_binding_derived_name__", handler = placeholder!($msg), sink_policies = [$($($policy),*)?], observers = [])
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
        $(@attachments [$($policy:expr),* $(,)?])?
        $(@delivery $delivery:ident)?
    ) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = "__obzenflow_binding_derived_name__", handler = $crate::__obzenflow_sink_delivery!($handler_head $(:: $handler_tail)* $(, $delivery)?), sink_policies = [$($($policy),*)?], observers = [])
    };

    // ── typed (explicit name override): exact input ──
    (name: $name:literal, $in:ty => placeholder!() $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = $name, handler = placeholder!(), sink_policies = [$($($policy),*)?], observers = [])
    };
    (name: $name:literal, $in:ty => placeholder!($msg:expr) $(@attachments [$($policy:expr),* $(,)?])? $(,)?) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = $name, handler = placeholder!($msg), sink_policies = [$($($policy),*)?], observers = [])
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
        $(@attachments [$($policy:expr),* $(,)?])?
        $(@delivery $delivery:ident)?
    ) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = $name, handler = $crate::__obzenflow_sink_delivery!($handler_head $(:: $handler_tail)* $(, $delivery)?), sink_policies = [$($($policy),*)?], observers = [])
    };
    ($in:ty => placeholder!($($message:tt)*) $(@attachments [$($policy:expr),* $(,)?])? @delivery $delivery:ident) => {
        compile_error!("a placeholder sink delivers nothing, so it takes no `.idempotent()` or `.non_idempotent()`")
    };
    (name: $name:literal, $in:ty => placeholder!($($message:tt)*) $(@attachments [$($policy:expr),* $(,)?])? @delivery $delivery:ident) => {
        compile_error!("a placeholder sink delivers nothing, so it takes no `.idempotent()` or `.non_idempotent()`")
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
        const _: () = $crate::__private::assert_distinct_stage_fact_set::<
            $crate::__private::stage_fact_set![$($member),+],
        >();
        let __observers: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($mw)),*];        $crate::dsl::typing::placeholder_stateful_descriptor::<
            $in,
            $out,
            $crate::__private::stage_fact_set![$($member),+],
            _,
        >(
            $name,
            None,
            $crate::__obzenflow_stateful_emit_option!($emit $(($interval))?),
            __observers,        )
    }};
    (input = exact($in:ty), output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!($msg:expr), emit = $emit:ident $(($interval:expr))?, middleware = [$($mw:expr),*]) => {{
        const _: () = $crate::__private::assert_distinct_stage_fact_set::<
            $crate::__private::stage_fact_set![$($member),+],
        >();
        let __observers: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($mw)),*];        $crate::dsl::typing::placeholder_stateful_descriptor::<
            $in,
            $out,
            $crate::__private::stage_fact_set![$($member),+],
            _,
        >(
            $name,
            Some($msg),
            $crate::__obzenflow_stateful_emit_option!($emit $(($interval))?),
            __observers,        )
    }};
    (input = exact($in:ty), output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = $handler:expr, emit = $emit:ident $(($interval:expr))?, middleware = [$($mw:expr),*]) => {{
        let __handler = $handler;
        const _: () = $crate::__private::assert_distinct_stage_fact_set::<
            $crate::__private::stage_fact_set![$($member),+],
        >();
        let __observers: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($mw)),*];        $crate::dsl::typing::typed_stateful_descriptor::<
            _,
            $in,
            $out,
            $crate::__private::stage_fact_set![$($member),+],
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
        const _: () = $crate::__private::assert_distinct_stage_fact_set::<
            $crate::__private::stage_fact_set![$($member),+],
        >();
        let __observers: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($mw)),*];
        $crate::dsl::typing::placeholder_join_descriptor::<
            $ref_ty,
            $str_ty,
            $out,
            $crate::__private::stage_fact_set![$($member),+],
            _,
        >($name, stringify!($ref_var), None, __observers)
    }};
    (reference = exact, stream = exact, output = $out:ty,
     output_contract = [$($member:ty),+ $(,)?],
     ref_type = ($ref_ty:ty), stream_type = ($str_ty:ty),
     name = $name:literal, ref_var = $ref_var:ident, handler = placeholder!($msg:expr),
     middleware = [$($mw:expr),*]) => {{
        const _: () = $crate::__private::assert_distinct_stage_fact_set::<
            $crate::__private::stage_fact_set![$($member),+],
        >();
        let __observers: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($mw)),*];
        $crate::dsl::typing::placeholder_join_descriptor::<
            $ref_ty,
            $str_ty,
            $out,
            $crate::__private::stage_fact_set![$($member),+],
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
        const _: () = $crate::__private::assert_distinct_stage_fact_set::<
            $crate::__private::stage_fact_set![$($member),+],
        >();
        let __observers: Vec<Box<dyn $crate::__private::MiddlewareFactory>> =
            vec![$($crate::__private::capture_middleware($mw)),*];
        $crate::dsl::typing::typed_join_descriptor::<
            _,
            $ref_ty,
            $str_ty,
            $out,
            $crate::__private::stage_fact_set![$($member),+],
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

// ============================================================================
// ai_map_reduce! generated effect protocol
// ============================================================================

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_ai_map_reduce_oversize_policy {
    (error $(,)?) => {
        $crate::__private::OversizePolicy::Error
    };

    (decompose { max_depth: $max_depth:expr, exhaustion: fail $(,)? } $(,)?) => {
        $crate::__private::OversizePolicy::Rerender {
            max_depth: $max_depth,
            min_progress_tokens: $crate::__private::TokenCount::new(1),
            exhaustion: $crate::__private::OversizeExhaustion::Fail,
        }
    };
    (decompose { max_depth: $max_depth:expr, exhaustion: exclude $(,)? } $(,)?) => {
        $crate::__private::OversizePolicy::Rerender {
            max_depth: $max_depth,
            min_progress_tokens: $crate::__private::TokenCount::new(1),
            exhaustion: $crate::__private::OversizeExhaustion::Exclude,
        }
    };

    (decompose { max_depth: $max_depth:expr, min_progress_tokens: $min_progress_tokens:expr, exhaustion: fail $(,)? } $(,)?) => {
        $crate::__private::OversizePolicy::Rerender {
            max_depth: $max_depth,
            min_progress_tokens: $min_progress_tokens,
            exhaustion: $crate::__private::OversizeExhaustion::Fail,
        }
    };
    (decompose { max_depth: $max_depth:expr, min_progress_tokens: $min_progress_tokens:expr, exhaustion: exclude $(,)? } $(,)?) => {
        $crate::__private::OversizePolicy::Rerender {
            max_depth: $max_depth,
            min_progress_tokens: $min_progress_tokens,
            exhaustion: $crate::__private::OversizeExhaustion::Exclude,
        }
    };

    ($policy:expr $(,)?) => {
        $policy
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_ai_map_reduce_chunker_by_budget {
    (
        seed_type = ($($seed_ty:tt)+),
        item_type = ($item_ty:ty),
        $(estimator: $estimator:expr,)?
        items: $items:expr,
        render: $render:expr,
        budget: $budget:expr,
        max_items: $max_items:expr,
        oversize: error
        $(, snapshot_excluded_items_limit: $snapshot_excluded_items_limit:expr)?
        $(,)?
    ) => {{
        let __oversize = $crate::__obzenflow_ai_map_reduce_oversize_policy!(error);
        $crate::__private::ChunkByBudgetBuilder::<$($seed_ty)+, $item_ty>::new()
            $(.estimator($estimator))?
            .items($items)
            .render($render)
            .budget($budget)
            .max_items_per_chunk($max_items)
            .oversize(__oversize)
            $(.snapshot_excluded_items_limit($snapshot_excluded_items_limit))?
            .build()
    }};

    (
        seed_type = ($($seed_ty:tt)+),
        item_type = ($item_ty:ty),
        $(estimator: $estimator:expr,)?
        items: $items:expr,
        render: $render:expr,
        budget: $budget:expr,
        max_items: $max_items:expr,
        oversize: decompose { $($oversize:tt)* }
        $(, snapshot_excluded_items_limit: $snapshot_excluded_items_limit:expr)?
        $(,)?
    ) => {{
        let __oversize =
            $crate::__obzenflow_ai_map_reduce_oversize_policy!(decompose { $($oversize)* });
        $crate::__private::ChunkByBudgetBuilder::<$($seed_ty)+, $item_ty>::new()
            $(.estimator($estimator))?
            .items($items)
            .render($render)
            .budget($budget)
            .max_items_per_chunk($max_items)
            .oversize(__oversize)
            $(.snapshot_excluded_items_limit($snapshot_excluded_items_limit))?
            .build()
    }};

    (
        seed_type = ($($seed_ty:tt)+),
        item_type = ($item_ty:ty),
        $(estimator: $estimator:expr,)?
        items: $items:expr,
        render: $render:expr,
        budget: $budget:expr,
        max_items: $max_items:expr,
        oversize: $oversize:expr
        $(, snapshot_excluded_items_limit: $snapshot_excluded_items_limit:expr)?
        $(,)?
    ) => {{
        $crate::__private::ChunkByBudgetBuilder::<$($seed_ty)+, $item_ty>::new()
            $(.estimator($estimator))?
            .items($items)
            .render($render)
            .budget($budget)
            .max_items_per_chunk($max_items)
            .oversize($oversize)
            $(.snapshot_excluded_items_limit($snapshot_excluded_items_limit))?
            .build()
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_clone_ai_chat_contract {
    ("inference!", $binding:ident) => {{
        #[allow(non_camel_case_types)]
        struct $binding {
            _private: (),
        }
        $crate::dsl::ai_effect::clone_inference_chat_binding::<_, $binding>(&$binding)
    }};
    ($surface:literal, $binding:ident) => {
        $crate::dsl::ai_effect::clone_chat_binding(&$binding)
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_inference_contract {
    // Retired arrow-embedded singleton row.
    (
        name = $name:literal,
        input = ($($input:tt)+),
        -> { $($row:tt)* } $out:ty => $role_head:ident $(:: $role_tail:ident)*
        $($rest:tt)*
    ) => {
        compile_error!("inference!: arrow-embedded effect rows were removed; write `Input -> Output uses at_least_once(ChatCompletion) via chat with resilience => handler`")
    };
    (
        name = $name:literal,
        input = ($($input:tt)+),
        -> $($rest:tt)+
    ) => {
        $crate::__obzenflow_inference_contract!(
            @output
            name = $name,
            input = ($($input)+),
            output = [],
            $($rest)+
        )
    };

    // Collect the complete output contract before `uses` so it reads and
    // parses independently from the effect capability.
    (
        @output
        name = $name:literal,
        input = ($($input:tt)+),
        output = [$($out:tt)+],
        uses { $($row:tt)* } => $($rest:tt)+
    ) => {
        $crate::__obzenflow_multi_effect_uses_gate!(effects = [$($row)*], then = [
            $crate::__obzenflow_inference_contract!(
                @lower
                name = $name,
                input = ($($input)+),
                output = [$($out)+],
                row = [$($row)*],
                rest = [$($rest)+]
            )
        ])
    };
    (
        @output
        name = $name:literal,
        input = ($($input:tt)+),
        output = [$($out:tt)+],
        uses $($rest:tt)+
    ) => {
        $crate::__obzenflow_inference_contract!(
            @effect
            name = $name,
            input = ($($input)+),
            output = [$($out)+],
            row = [],
            $($rest)+
        )
    };
    (
        @output
        name = $name:literal,
        input = ($($input:tt)+),
        output = [$($out:tt)+],
        => $($rest:tt)+
    ) => {
        compile_error!(
            "inference!: expected `Input -> Output uses at_least_once(ChatCompletion) \
             via <chat binding> with <middleware> => handler`"
        )
    };
    (
        @output
        name = $name:literal,
        input = ($($input:tt)+),
        output = [$($out:tt)*],
        $token:tt $($rest:tt)+
    ) => {
        $crate::__obzenflow_inference_contract!(
            @output
            name = $name,
            input = ($($input)+),
            output = [$($out)* $token],
            $($rest)+
        )
    };
    (
        @output
        name = $name:literal,
        input = ($($input:tt)+),
        output = [$($out:tt)*],
        $($rest:tt)*
    ) => {
        compile_error!("inference!: expected `Input -> Output uses Effect => handler`")
    };

    (
        @effect
        name = $name:literal,
        input = ($($input:tt)+),
        output = [$($out:tt)+],
        row = [$($row:tt)*],
        => $($rest:tt)+
    ) => {
        $crate::__obzenflow_single_effect_uses_gate!(effects = [$($row)*], then = [
            $crate::__obzenflow_inference_contract!(
                @lower
                name = $name,
                input = ($($input)+),
                output = [$($out)+],
                row = [$($row)*],
                rest = [$($rest)+]
            )
        ])
    };
    (
        @effect
        name = $name:literal,
        input = ($($input:tt)+),
        output = [$($out:tt)+],
        row = [$($row:tt)*],
        $token:tt $($rest:tt)+
    ) => {
        $crate::__obzenflow_inference_contract!(
            @effect
            name = $name,
            input = ($($input)+),
            output = [$($out)+],
            row = [$($row)* $token],
            $($rest)+
        )
    };
    (
        @effect
        name = $name:literal,
        input = ($($input:tt)+),
        output = [$($out:tt)+],
        row = [$($row:tt)*],
        $($rest:tt)*
    ) => {
        compile_error!("inference!: expected `=> handler` after the `uses` clause")
    };

    (
        @lower
        name = $name:literal,
        input = ($($input:tt)+),
        output = [$out:ty],
        row = [$($row:tt)*],
        rest = [$role_head:ident $(:: $role_tail:ident)* $(@attachments [$($middleware:expr),*])? $(,)?]
    ) => {{
        let mut __chat_effect_row = $crate::__obzenflow_effect_entries!(
            @generated_chat surface = "inference!",
            row = { $($row)* }
        );
        __chat_effect_row.implementation_middleware = vec![$($($crate::__private::capture_middleware($middleware)),*)?];
        $crate::dsl::inference::generated_inference::<$($input)+, $out>(
            $name,
            $role_head $(:: $role_tail)*,
            __chat_effect_row,
        )
    }};
    (
        @lower
        name = $name:literal,
        input = ($($input:tt)+),
        output = [$out:ty],
        row = [$($row:tt)*],
        rest = [$role:expr, chunking: $($chunking:tt)*]
    ) => {
        compile_error!(
            "inference!: `chunking` is not supported; use `ai_map!` or `ai_map_reduce!`"
        )
    };
    (
        @lower
        name = $name:literal,
        input = ($($input:tt)+),
        output = [$out:ty],
        row = [$($row:tt)*],
        rest = [$role:expr $(,)?]
    ) => {
        $crate::__obzenflow_effect_entries!(
            @generated_chat_then surface = "inference!",
            row = { $($row)* },
            then = {
                $crate::__obzenflow_handler_path_diagnostic!(
                    "inference!",
                    "implement InferenceHandler for a handler type, bind a value immediately above the flow, then pass that name: let handler = MyInferenceHandler; answer = inference!(Input -> Output uses Effect => handler);"
                )
            }
        )
    };
    (
        @lower
        name = $name:literal,
        input = ($($input:tt)+),
        output = [$($out:tt)*],
        row = [$($row:tt)*],
        rest = [$($rest:tt)*]
    ) => {
        compile_error!("inference!: expected one scalar output type before `uses`")
    };

    (
        name = $name:literal,
        input = (),
        -> $($rest:tt)*
    ) => {
        compile_error!("inference!: expected an input type before `->`")
    };
    (
        name = $name:literal,
        input = ($($input:tt)*),
        $token:tt $($rest:tt)+
    ) => {
        $crate::__obzenflow_inference_contract!(
            name = $name,
            input = ($($input)* $token),
            $($rest)+
        )
    };
}

/// Create one generated replay-safe scalar AI inference stage.
#[macro_export]
macro_rules! __obzenflow_inference_attachment_contract {
    ([$item:ty] $($rest:tt)*) => {
        compile_error!(
            "inference!: batch input `[T]` is not supported; use `ai_map!` or `ai_map_reduce!`"
        )
    };
    ($($rest:tt)+) => {
        $crate::__obzenflow_inference_contract!(
            name = "__obzenflow_binding_derived_name__",
            input = (),
            $($rest)+
        )
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_ai_map_reduce_generated_typed {
    (
        name = $name:literal,
        seed_type = ($($seed_ty:tt)+),
        item_type = ($item_ty:ty),
        partial_type = ($partial_ty:ty),
        out_type = ($out_ty:ty),
        chunker = ($chunker:expr),
        map_role = ($map_role:expr),
        finalize_role = ($finalize_role:expr),
        map_effect_row = ($map_effect_row:expr),
        finalize_effect_row = ($finalize_effect_row:expr)
    ) => {{
        $crate::dsl::composites::ai_map_reduce::generated_map_reduce::<
            $($seed_ty)+,
            $item_ty,
            $partial_ty,
            $out_ty,
            _,
            _,
        >(
            $name,
            ($chunker, $map_role, $finalize_role),
            ($map_effect_row, $finalize_effect_row),
        )
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_ai_map_reduce_build {
    (
        name = $name:literal,
        seed_type = ($($seed_ty:tt)+),
        item_type = ($item_ty:ty),
        partial_type = ($partial_ty:ty),
        out_type = ($out_ty:ty),
        reduce_seed_type = ($reduce_seed_ty:ty),
        reduce_partial_type = ($reduce_partial_ty:ty),
        reduce_out_type = ($reduce_out_ty:ty),
        map_role = ($map_role:expr),
        map_middleware = ($map_middleware:expr),
        finalize_role = ($finalize_role:expr),
        finalize_middleware = ($finalize_middleware:expr),
        map_row = { $($map_row:tt)* },
        finalize_row = { $($finalize_row:tt)* },
        chunking = {
            estimator: $estimator:expr,
            $($chunking:tt)*
        }
    ) => {
        compile_error!(
            "ai_map_reduce!: `chunking.estimator` is supplied by `via chat`"
        )
    };
    (
        name = $name:literal,
        seed_type = ($($seed_ty:tt)+),
        item_type = ($item_ty:ty),
        partial_type = ($partial_ty:ty),
        out_type = ($out_ty:ty),
        reduce_seed_type = ($reduce_seed_ty:ty),
        reduce_partial_type = ($reduce_partial_ty:ty),
        reduce_out_type = ($reduce_out_ty:ty),
        map_role = ($map_role:expr),
        map_middleware = ($map_middleware:expr),
        finalize_role = ($finalize_role:expr),
        finalize_middleware = ($finalize_middleware:expr),
        map_row = { $($map_row:tt)* },
        finalize_row = { $($finalize_row:tt)* },
        chunking = { $($chunking:tt)+ }
    ) => {{
        let mut __map_effect_row = $crate::__obzenflow_effect_entries!(
            @generated_chat surface = "ai_map_reduce!",
            row = { $($map_row)* }
        );
        let mut __finalize_effect_row = $crate::__obzenflow_effect_entries!(
            @generated_chat surface = "ai_map_reduce!",
            row = { $($finalize_row)* }
        );
        __map_effect_row.implementation_middleware = $map_middleware;
        __finalize_effect_row.implementation_middleware = $finalize_middleware;
        let __chunker = $crate::__obzenflow_ai_map_reduce_chunker_by_budget!(
            seed_type = ($($seed_ty)+),
            item_type = ($item_ty),
            estimator: $crate::__private::ChatBindingMetadata::estimator(
                &__map_effect_row.binding
            ).estimator(),
            $($chunking)+
        );
        let _: ::core::marker::PhantomData<$($seed_ty)+> =
            ::core::marker::PhantomData::<$reduce_seed_ty>;
        let _: ::core::marker::PhantomData<$partial_ty> =
            ::core::marker::PhantomData::<$reduce_partial_ty>;
        let _: ::core::marker::PhantomData<$out_ty> =
            ::core::marker::PhantomData::<$reduce_out_ty>;

        $crate::__obzenflow_ai_map_reduce_generated_typed!(
            name = $name,
            seed_type = ($($seed_ty)+),
            item_type = ($item_ty),
            partial_type = ($partial_ty),
            out_type = ($out_ty),
            chunker = (__chunker),
            map_role = ($map_role),
            finalize_role = ($finalize_role),
            map_effect_row = (__map_effect_row),
            finalize_effect_row = (__finalize_effect_row)
        )
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_ai_map_reduce_uses_contract {
    // Retired map-role arrow row.
    (
        @map
        name = $name:literal,
        seed_type = [$seed_ty:ty],
        out_type = [$out_ty:ty],
        chunking = [$($chunking:tt)+],
        roles = [
            map: [$item_ty:ty] -> { $($row:tt)* } $partial_ty:ty => $($rest:tt)*
        ]
    ) => {
        compile_error!("ai_map_reduce!: arrow-embedded effect rows were removed; write `map: [Item] -> Partial uses Effect => map_role`")
    };
    (
        @map
        name = $name:literal,
        seed_type = [$seed_ty:ty],
        out_type = [$out_ty:ty],
        chunking = [$($chunking:tt)+],
        roles = [map: [$item_ty:ty] -> $($rest:tt)+]
    ) => {
        $crate::__obzenflow_ai_map_reduce_uses_contract!(
            @map_output
            name = $name,
            seed_type = [$seed_ty],
            out_type = [$out_ty],
            item_type = [$item_ty],
            chunking = [$($chunking)+],
            output = [],
            $($rest)+
        )
    };

    (
        @map_output
        name = $name:literal,
        seed_type = [$seed_ty:ty],
        out_type = [$out_ty:ty],
        item_type = [$item_ty:ty],
        chunking = [$($chunking:tt)+],
        output = [$($partial_ty:tt)+],
        uses { $($row:tt)* } => $($rest:tt)+
    ) => {
        $crate::__obzenflow_multi_effect_uses_gate!(effects = [$($row)*], then = [
            $crate::__obzenflow_ai_map_reduce_uses_contract!(
                @map_role
                name = $name,
                seed_type = [$seed_ty],
                out_type = [$out_ty],
                item_type = [$item_ty],
                partial_type = [$($partial_ty)+],
                map_row = [$($row)*],
                chunking = [$($chunking)+],
                role = [],
                remaining = [$($rest)+]
            )
        ])
    };
    (
        @map_output
        name = $name:literal,
        seed_type = [$seed_ty:ty],
        out_type = [$out_ty:ty],
        item_type = [$item_ty:ty],
        chunking = [$($chunking:tt)+],
        output = [$($partial_ty:tt)+],
        uses $($rest:tt)+
    ) => {
        $crate::__obzenflow_ai_map_reduce_uses_contract!(
            @map_effect
            name = $name,
            seed_type = [$seed_ty],
            out_type = [$out_ty],
            item_type = [$item_ty],
            partial_type = [$($partial_ty)+],
            chunking = [$($chunking)+],
            row = [],
            $($rest)+
        )
    };
    (
        @map_output
        name = $name:literal,
        seed_type = [$seed_ty:ty],
        out_type = [$out_ty:ty],
        item_type = [$item_ty:ty],
        chunking = [$($chunking:tt)+],
        output = [$($partial_ty:tt)+],
        => $($rest:tt)+
    ) => {
        compile_error!("ai_map_reduce!: map role requires `uses at_least_once(ChatCompletion) via <chat binding> with <middleware>`")
    };
    (
        @map_output
        name = $name:literal,
        seed_type = [$seed_ty:ty],
        out_type = [$out_ty:ty],
        item_type = [$item_ty:ty],
        chunking = [$($chunking:tt)+],
        output = [$($partial_ty:tt)*],
        $token:tt $($rest:tt)+
    ) => {
        $crate::__obzenflow_ai_map_reduce_uses_contract!(
            @map_output
            name = $name,
            seed_type = [$seed_ty],
            out_type = [$out_ty],
            item_type = [$item_ty],
            chunking = [$($chunking)+],
            output = [$($partial_ty)* $token],
            $($rest)+
        )
    };

    (
        @map_effect
        name = $name:literal,
        seed_type = [$seed_ty:ty],
        out_type = [$out_ty:ty],
        item_type = [$item_ty:ty],
        partial_type = [$($partial_ty:tt)+],
        chunking = [$($chunking:tt)+],
        row = [$($row:tt)*],
        => $($rest:tt)+
    ) => {
        $crate::__obzenflow_single_effect_uses_gate!(effects = [$($row)*], then = [
            $crate::__obzenflow_ai_map_reduce_uses_contract!(
                @map_role
                name = $name,
                seed_type = [$seed_ty],
                out_type = [$out_ty],
                item_type = [$item_ty],
                partial_type = [$($partial_ty)+],
                map_row = [$($row)*],
                chunking = [$($chunking)+],
                role = [],
                remaining = [$($rest)+]
            )
        ])
    };
    (
        @map_effect
        name = $name:literal,
        seed_type = [$seed_ty:ty],
        out_type = [$out_ty:ty],
        item_type = [$item_ty:ty],
        partial_type = [$($partial_ty:tt)+],
        chunking = [$($chunking:tt)+],
        row = [$($row:tt)*],
        $token:tt $($rest:tt)+
    ) => {
        $crate::__obzenflow_ai_map_reduce_uses_contract!(
            @map_effect
            name = $name,
            seed_type = [$seed_ty],
            out_type = [$out_ty],
            item_type = [$item_ty],
            partial_type = [$($partial_ty)+],
            chunking = [$($chunking)+],
            row = [$($row)* $token],
            $($rest)+
        )
    };

    // The comma followed by `reduce:` terminates the map role. Role
    // expressions remain diagnosed after both effect clauses are validated.
    (
        @map_role
        name = $name:literal,
        seed_type = [$seed_ty:ty],
        out_type = [$out_ty:ty],
        item_type = [$item_ty:ty],
        partial_type = [$($partial_ty:tt)+],
        map_row = [$($map_row:tt)*],
        chunking = [$($chunking:tt)+],
        role = [$($map_role:tt)+],
        remaining = [
            , reduce: ($reduce_seed_ty:ty, [$reduce_partial_ty:ty])
                -> { $($finalize_row:tt)* } $reduce_out_ty:ty => $($rest:tt)*
        ]
    ) => {
        compile_error!("ai_map_reduce!: arrow-embedded effect rows were removed; write `reduce: (Seed, [Partial]) -> Output uses Effect => reduce_role`")
    };
    (
        @map_role
        name = $name:literal,
        seed_type = [$seed_ty:ty],
        out_type = [$out_ty:ty],
        item_type = [$item_ty:ty],
        partial_type = [$($partial_ty:tt)+],
        map_row = [$($map_row:tt)*],
        chunking = [$($chunking:tt)+],
        role = [$($map_role:tt)+],
        remaining = [
            , reduce: ($reduce_seed_ty:ty, [$reduce_partial_ty:ty]) -> $($rest:tt)+
        ]
    ) => {
        $crate::__obzenflow_ai_map_reduce_uses_contract!(
            @reduce_output
            name = $name,
            seed_type = [$seed_ty],
            out_type = [$out_ty],
            item_type = [$item_ty],
            partial_type = [$($partial_ty)+],
            map_row = [$($map_row)*],
            map_role = [$($map_role)+],
            reduce_seed_type = [$reduce_seed_ty],
            reduce_partial_type = [$reduce_partial_ty],
            chunking = [$($chunking)+],
            output = [],
            $($rest)+
        )
    };
    (
        @map_role
        name = $name:literal,
        seed_type = [$seed_ty:ty],
        out_type = [$out_ty:ty],
        item_type = [$item_ty:ty],
        partial_type = [$($partial_ty:tt)+],
        map_row = [$($map_row:tt)*],
        chunking = [$($chunking:tt)+],
        role = [$($map_role:tt)*],
        remaining = [$token:tt $($rest:tt)*]
    ) => {
        $crate::__obzenflow_ai_map_reduce_uses_contract!(
            @map_role
            name = $name,
            seed_type = [$seed_ty],
            out_type = [$out_ty],
            item_type = [$item_ty],
            partial_type = [$($partial_ty)+],
            map_row = [$($map_row)*],
            chunking = [$($chunking)+],
            role = [$($map_role)* $token],
            remaining = [$($rest)*]
        )
    };
    (
        @map_role
        name = $name:literal,
        seed_type = [$seed_ty:ty],
        out_type = [$out_ty:ty],
        item_type = [$item_ty:ty],
        partial_type = [$($partial_ty:tt)+],
        map_row = [$($map_row:tt)*],
        chunking = [$($chunking:tt)+],
        role = [$($map_role:tt)*],
        remaining = []
    ) => {
        compile_error!("ai_map_reduce!: expected a role-local `reduce:` declaration after the map role")
    };

    (
        @reduce_output
        name = $name:literal,
        seed_type = [$seed_ty:ty],
        out_type = [$out_ty:ty],
        item_type = [$item_ty:ty],
        partial_type = [$($partial_ty:tt)+],
        map_row = [$($map_row:tt)*],
        map_role = [$($map_role:tt)+],
        reduce_seed_type = [$reduce_seed_ty:ty],
        reduce_partial_type = [$reduce_partial_ty:ty],
        chunking = [$($chunking:tt)+],
        output = [$($reduce_out_ty:tt)+],
        uses { $($row:tt)* } => $($rest:tt)+
    ) => {
        $crate::__obzenflow_multi_effect_uses_gate!(effects = [$($row)*], then = [
            $crate::__obzenflow_ai_map_reduce_uses_contract!(
                @reduce_role
                name = $name,
                seed_type = [$seed_ty],
                out_type = [$out_ty],
                item_type = [$item_ty],
                partial_type = [$($partial_ty)+],
                map_row = [$($map_row)*],
                map_role = [$($map_role)+],
                reduce_seed_type = [$reduce_seed_ty],
                reduce_partial_type = [$reduce_partial_ty],
                reduce_out_type = [$($reduce_out_ty)+],
                finalize_row = [$($row)*],
                chunking = [$($chunking)+],
                role = [],
                remaining = [$($rest)+]
            )
        ])
    };
    (
        @reduce_output
        name = $name:literal,
        seed_type = [$seed_ty:ty],
        out_type = [$out_ty:ty],
        item_type = [$item_ty:ty],
        partial_type = [$($partial_ty:tt)+],
        map_row = [$($map_row:tt)*],
        map_role = [$($map_role:tt)+],
        reduce_seed_type = [$reduce_seed_ty:ty],
        reduce_partial_type = [$reduce_partial_ty:ty],
        chunking = [$($chunking:tt)+],
        output = [$($reduce_out_ty:tt)+],
        uses $($rest:tt)+
    ) => {
        $crate::__obzenflow_ai_map_reduce_uses_contract!(
            @reduce_effect
            name = $name,
            seed_type = [$seed_ty],
            out_type = [$out_ty],
            item_type = [$item_ty],
            partial_type = [$($partial_ty)+],
            map_row = [$($map_row)*],
            map_role = [$($map_role)+],
            reduce_seed_type = [$reduce_seed_ty],
            reduce_partial_type = [$reduce_partial_ty],
            reduce_out_type = [$($reduce_out_ty)+],
            chunking = [$($chunking)+],
            row = [],
            $($rest)+
        )
    };
    (
        @reduce_output
        name = $name:literal,
        seed_type = [$seed_ty:ty],
        out_type = [$out_ty:ty],
        item_type = [$item_ty:ty],
        partial_type = [$($partial_ty:tt)+],
        map_row = [$($map_row:tt)*],
        map_role = [$($map_role:tt)+],
        reduce_seed_type = [$reduce_seed_ty:ty],
        reduce_partial_type = [$reduce_partial_ty:ty],
        chunking = [$($chunking:tt)+],
        output = [$($reduce_out_ty:tt)+],
        => $($rest:tt)+
    ) => {
        compile_error!("ai_map_reduce!: reduce role requires `uses at_least_once(ChatCompletion) via <chat binding> with <middleware>`")
    };
    (
        @reduce_output
        name = $name:literal,
        seed_type = [$seed_ty:ty],
        out_type = [$out_ty:ty],
        item_type = [$item_ty:ty],
        partial_type = [$($partial_ty:tt)+],
        map_row = [$($map_row:tt)*],
        map_role = [$($map_role:tt)+],
        reduce_seed_type = [$reduce_seed_ty:ty],
        reduce_partial_type = [$reduce_partial_ty:ty],
        chunking = [$($chunking:tt)+],
        output = [$($reduce_out_ty:tt)*],
        $token:tt $($rest:tt)+
    ) => {
        $crate::__obzenflow_ai_map_reduce_uses_contract!(
            @reduce_output
            name = $name,
            seed_type = [$seed_ty],
            out_type = [$out_ty],
            item_type = [$item_ty],
            partial_type = [$($partial_ty)+],
            map_row = [$($map_row)*],
            map_role = [$($map_role)+],
            reduce_seed_type = [$reduce_seed_ty],
            reduce_partial_type = [$reduce_partial_ty],
            chunking = [$($chunking)+],
            output = [$($reduce_out_ty)* $token],
            $($rest)+
        )
    };

    (
        @reduce_effect
        name = $name:literal,
        seed_type = [$seed_ty:ty],
        out_type = [$out_ty:ty],
        item_type = [$item_ty:ty],
        partial_type = [$($partial_ty:tt)+],
        map_row = [$($map_row:tt)*],
        map_role = [$($map_role:tt)+],
        reduce_seed_type = [$reduce_seed_ty:ty],
        reduce_partial_type = [$reduce_partial_ty:ty],
        reduce_out_type = [$($reduce_out_ty:tt)+],
        chunking = [$($chunking:tt)+],
        row = [$($row:tt)*],
        => $($rest:tt)+
    ) => {
        $crate::__obzenflow_single_effect_uses_gate!(effects = [$($row)*], then = [
            $crate::__obzenflow_ai_map_reduce_uses_contract!(
                @reduce_role
                name = $name,
                seed_type = [$seed_ty],
                out_type = [$out_ty],
                item_type = [$item_ty],
                partial_type = [$($partial_ty)+],
                map_row = [$($map_row)*],
                map_role = [$($map_role)+],
                reduce_seed_type = [$reduce_seed_ty],
                reduce_partial_type = [$reduce_partial_ty],
                reduce_out_type = [$($reduce_out_ty)+],
                finalize_row = [$($row)*],
                chunking = [$($chunking)+],
                role = [],
                remaining = [$($rest)+]
            )
        ])
    };
    (
        @reduce_effect
        name = $name:literal,
        seed_type = [$seed_ty:ty],
        out_type = [$out_ty:ty],
        item_type = [$item_ty:ty],
        partial_type = [$($partial_ty:tt)+],
        map_row = [$($map_row:tt)*],
        map_role = [$($map_role:tt)+],
        reduce_seed_type = [$reduce_seed_ty:ty],
        reduce_partial_type = [$reduce_partial_ty:ty],
        reduce_out_type = [$($reduce_out_ty:tt)+],
        chunking = [$($chunking:tt)+],
        row = [$($row:tt)*],
        $token:tt $($rest:tt)+
    ) => {
        $crate::__obzenflow_ai_map_reduce_uses_contract!(
            @reduce_effect
            name = $name,
            seed_type = [$seed_ty],
            out_type = [$out_ty],
            item_type = [$item_ty],
            partial_type = [$($partial_ty)+],
            map_row = [$($map_row)*],
            map_role = [$($map_role)+],
            reduce_seed_type = [$reduce_seed_ty],
            reduce_partial_type = [$reduce_partial_ty],
            reduce_out_type = [$($reduce_out_ty)+],
            chunking = [$($chunking)+],
            row = [$($row)* $token],
            $($rest)+
        )
    };

    (
        @reduce_role
        name = $name:literal,
        seed_type = [$seed_ty:ty],
        out_type = [$out_ty:ty],
        item_type = [$item_ty:ty],
        partial_type = [$($partial_ty:tt)+],
        map_row = [$($map_row:tt)*],
        map_role = [$($map_role:tt)+],
        reduce_seed_type = [$reduce_seed_ty:ty],
        reduce_partial_type = [$reduce_partial_ty:ty],
        reduce_out_type = [$($reduce_out_ty:tt)+],
        finalize_row = [$($finalize_row:tt)*],
        chunking = [$($chunking:tt)+],
        role = [$($finalize_role:tt)*],
        remaining = [$token:tt $($rest:tt)*]
    ) => {
        $crate::__obzenflow_ai_map_reduce_uses_contract!(
            @reduce_role
            name = $name,
            seed_type = [$seed_ty],
            out_type = [$out_ty],
            item_type = [$item_ty],
            partial_type = [$($partial_ty)+],
            map_row = [$($map_row)*],
            map_role = [$($map_role)+],
            reduce_seed_type = [$reduce_seed_ty],
            reduce_partial_type = [$reduce_partial_ty],
            reduce_out_type = [$($reduce_out_ty)+],
            finalize_row = [$($finalize_row)*],
            chunking = [$($chunking)+],
            role = [$($finalize_role)* $token],
            remaining = [$($rest)*]
        )
    };
    (
        @reduce_role
        name = $name:literal,
        seed_type = [$seed_ty:ty],
        out_type = [$out_ty:ty],
        item_type = [$item_ty:ty],
        partial_type = [$($partial_ty:tt)+],
        map_row = [$($map_row:tt)*],
        map_role = [$($map_role:tt)+],
        reduce_seed_type = [$reduce_seed_ty:ty],
        reduce_partial_type = [$reduce_partial_ty:ty],
        reduce_out_type = [$($reduce_out_ty:tt)+],
        finalize_row = [$($finalize_row:tt)*],
        chunking = [$($chunking:tt)+],
        role = [$($finalize_role:tt)+],
        remaining = []
    ) => {
        $crate::__obzenflow_ai_map_reduce_uses_contract!(
            @lower
            name = $name,
            seed_type = [$seed_ty],
            out_type = [$out_ty],
            item_type = [$item_ty],
            partial_type = [$($partial_ty)+],
            map_row = [$($map_row)*],
            map_role = [$($map_role)+],
            reduce_seed_type = [$reduce_seed_ty],
            reduce_partial_type = [$reduce_partial_ty],
            reduce_out_type = [$($reduce_out_ty)+],
            finalize_row = [$($finalize_row)*],
            finalize_role = [$($finalize_role)+],
            chunking = [$($chunking)+]
        )
    };

    (
        @lower
        name = $name:literal,
        seed_type = [$seed_ty:ty],
        out_type = [$out_ty:ty],
        item_type = [$item_ty:ty],
        partial_type = [$partial_ty:ty],
        map_row = [$($map_row:tt)*],
        map_role = [$($map_role:tt)+],
        reduce_seed_type = [$reduce_seed_ty:ty],
        reduce_partial_type = [$reduce_partial_ty:ty],
        reduce_out_type = [$reduce_out_ty:ty],
        finalize_row = [$($finalize_row:tt)*],
        finalize_role = [$($finalize_role:tt)+],
        chunking = [$($chunking:tt)+]
    ) => {{
        let (__map_role, __map_middleware) = $crate::__obzenflow_role_attachments!($($map_role)+);
        let (__finalize_role, __finalize_middleware) = $crate::__obzenflow_role_attachments!($($finalize_role)+);
        $crate::__obzenflow_ai_map_reduce_build!(
            name = $name,
            seed_type = ($seed_ty),
            item_type = ($item_ty),
            partial_type = ($partial_ty),
            out_type = ($out_ty),
            reduce_seed_type = ($reduce_seed_ty),
            reduce_partial_type = ($reduce_partial_ty),
            reduce_out_type = ($reduce_out_ty),
            map_role = (__map_role),
            map_middleware = (__map_middleware),
            finalize_role = (__finalize_role),
            finalize_middleware = (__finalize_middleware),
            map_row = { $($map_row)* },
            finalize_row = { $($finalize_row)* },
            chunking = { $($chunking)+ }
        )
    }};
    (
        @lower
        name = $name:literal,
        seed_type = [$seed_ty:ty],
        out_type = [$out_ty:ty],
        item_type = [$item_ty:ty],
        partial_type = [$partial_ty:ty],
        map_row = [$($map_row:tt)*],
        map_role = [$map_role:expr $(,)?],
        reduce_seed_type = [$reduce_seed_ty:ty],
        reduce_partial_type = [$reduce_partial_ty:ty],
        reduce_out_type = [$reduce_out_ty:ty],
        finalize_row = [$($finalize_row:tt)*],
        finalize_role = [$finalize_role_head:ident $(:: $finalize_role_tail:ident)* $(,)?],
        chunking = [$($chunking:tt)+]
    ) => {
        $crate::__obzenflow_effect_entries!(
            @generated_chat_then surface = "ai_map_reduce!",
            row = { $($map_row)* },
            then = {
                $crate::__obzenflow_effect_entries!(
                    @generated_chat_then surface = "ai_map_reduce!",
                    row = { $($finalize_row)* },
                    then = {
                        $crate::__obzenflow_handler_path_diagnostic!(
                            "ai_map_reduce! map role",
                            "let map_role = MyMapRole::new(...); use `map: [Item] -> Partial uses Effect => map_role`"
                        )
                    }
                )
            }
        )
    };
    (
        @lower
        name = $name:literal,
        seed_type = [$seed_ty:ty],
        out_type = [$out_ty:ty],
        item_type = [$item_ty:ty],
        partial_type = [$partial_ty:ty],
        map_row = [$($map_row:tt)*],
        map_role = [$map_role_head:ident $(:: $map_role_tail:ident)* $(,)?],
        reduce_seed_type = [$reduce_seed_ty:ty],
        reduce_partial_type = [$reduce_partial_ty:ty],
        reduce_out_type = [$reduce_out_ty:ty],
        finalize_row = [$($finalize_row:tt)*],
        finalize_role = [$finalize_role:expr $(,)?],
        chunking = [$($chunking:tt)+]
    ) => {
        $crate::__obzenflow_effect_entries!(
            @generated_chat_then surface = "ai_map_reduce!",
            row = { $($map_row)* },
            then = {
                $crate::__obzenflow_effect_entries!(
                    @generated_chat_then surface = "ai_map_reduce!",
                    row = { $($finalize_row)* },
                    then = {
                        $crate::__obzenflow_handler_path_diagnostic!(
                            "ai_map_reduce! reduce role",
                            "let reduce_role = MyReduceRole::new(...); use `reduce: (Seed, [Partial]) -> Out uses Effect => reduce_role`"
                        )
                    }
                )
            }
        )
    };
    (
        @lower
        name = $name:literal,
        seed_type = [$seed_ty:ty],
        out_type = [$out_ty:ty],
        item_type = [$item_ty:ty],
        partial_type = [$partial_ty:ty],
        map_row = [$($map_row:tt)*],
        map_role = [$map_role:expr $(,)?],
        reduce_seed_type = [$reduce_seed_ty:ty],
        reduce_partial_type = [$reduce_partial_ty:ty],
        reduce_out_type = [$reduce_out_ty:ty],
        finalize_row = [$($finalize_row:tt)*],
        finalize_role = [$finalize_role:expr $(,)?],
        chunking = [$($chunking:tt)+]
    ) => {
        $crate::__obzenflow_effect_entries!(
            @generated_chat_then surface = "ai_map_reduce!",
            row = { $($map_row)* },
            then = {
                $crate::__obzenflow_effect_entries!(
                    @generated_chat_then surface = "ai_map_reduce!",
                    row = { $($finalize_row)* },
                    then = {
                        $crate::__obzenflow_handler_path_diagnostic!(
                            "ai_map_reduce! roles",
                            "bind both roles inside the materialiser, then pass their names"
                        )
                    }
                )
            }
        )
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_ai_map_reduce_generated_contract {
    (
        name = $name:literal,
        seed = ($($seed_ty:tt)+),
        -> $out_ty:ty => { $($roles:tt)* },
        chunking: by_budget { $($chunking:tt)* },
        effects: { $($effects:tt)* }
        $(,)?
    ) => {
        compile_error!(
            "ai_map_reduce!: `effects: { ... }` was replaced by role-local `uses` clauses"
        )
    };
    (
        name = $name:literal,
        seed = ($($seed_ty:tt)+),
        -> $out_ty:ty => { $($roles:tt)* },
        chunking: by_budget { $($chunking:tt)+ }
        $(,)?
    ) => {
        $crate::__obzenflow_ai_map_reduce_uses_contract!(
            @map
            name = $name,
            seed_type = [$($seed_ty)+],
            out_type = [$out_ty],
            chunking = [$($chunking)+],
            roles = [$($roles)*]
        )
    };
    (
        name = $name:literal,
        seed = (),
        -> $($rest:tt)*
    ) => {
        compile_error!("ai_map_reduce!: expected a seed type before `->`")
    };
    (
        name = $name:literal,
        seed = ($($seed:tt)*),
        -> $($rest:tt)+
    ) => {
        compile_error!(
            "ai_map_reduce!: expected role-local `Output uses at_least_once(ChatCompletion) \
             via <chat binding> with <middleware> => role` clauses on map and reduce"
        )
    };
    (
        name = $name:literal,
        seed = ($($seed:tt)*),
        $token:tt $($rest:tt)+
    ) => {
        $crate::__obzenflow_ai_map_reduce_generated_contract!(
            name = $name,
            seed = ($($seed)* $token),
            $($rest)+
        )
    };
}
/// Create an AI map-reduce composite stage descriptor.
#[macro_export]
macro_rules! __obzenflow_ai_map_reduce_attachment_contract {
    (name: $name:literal, chunk: $($rest:tt)+) => {
        compile_error!(
            "ai_map_reduce!: the legacy `chunk:` role surface was removed; \
             use `Seed -> Out => { map: ..., reduce: ... }, chunking: ..., effects: ...`"
        )
    };
    (chunk: $($rest:tt)+) => {
        compile_error!(
            "ai_map_reduce!: the legacy `chunk:` role surface was removed; \
             use `Seed -> Out => { map: ..., reduce: ... }, chunking: ..., effects: ...`"
        )
    };

    (name: $name:literal, $($rest:tt)+) => {
        compile_error!(
            "ai_map_reduce!: explicit `name:` is not supported; the left-hand `stages:` \
             binding is the durable composite identity"
        )
    };
    ($($rest:tt)+) => {
        $crate::__obzenflow_ai_map_reduce_generated_contract!(
            name = "__obzenflow_binding_derived_name__",
            seed = (),
            $($rest)+
        )
    };
}

/// Shared stage-row lowering: the stage's types, an optional `with` clause
/// holding its middleware, `=>`, then the handler and its decorations.
#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_stage_row {
    (@sig $kind:ident $target:ident [$($sig:tt)*] uses $($rest:tt)*) => {
        compile_error!("`uses` was removed; a stage performs the effects its handler declares, as in `type AllowedEffects = effect_set![AuthorizePayment];`")
    };
    (@sig $kind:ident $target:ident [$($sig:tt)*] via $($rest:tt)*) => {
        compile_error!("`via` was removed; the handler holds the system it calls in a `#[system]` field, as in `#[system] risk_engine: System<dyn RiskEnginePort>`")
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
        $crate::__obzenflow_stage_row!(@decorations $kind $target $sig $mw [placeholder!()] [] [] $($rest)*)
    };
    (@handler $kind:ident $target:ident $sig:tt $mw:tt placeholder!($message:expr) $($rest:tt)*) => {
        $crate::__obzenflow_stage_row!(@decorations $kind $target $sig $mw [placeholder!($message)] [] [] $($rest)*)
    };
    (@handler $kind:ident $target:ident $sig:tt $mw:tt handler_set!($($set:tt)*) $($rest:tt)*) => {
        $crate::__obzenflow_stage_row!(@decorations $kind $target $sig $mw [handler_set!($($set)*)] [] [] $($rest)*)
    };
    (@handler $kind:ident $target:ident $sig:tt $mw:tt $head:ident $($rest:tt)*) => {
        $crate::__obzenflow_stage_row!(@path $kind $target $sig $mw [$head] $($rest)*)
    };
    (@handler $kind:ident $target:ident $sig:tt $mw:tt $($rest:tt)*) => {
        compile_error!("a stage handler is a name, optionally followed by its decorations; build values with `let` above `flow!`")
    };
    (@path $kind:ident $target:ident $sig:tt $mw:tt [$($path:tt)*] :: $next:ident $($rest:tt)*) => {
        $crate::__obzenflow_stage_row!(@path $kind $target $sig $mw [$($path)* :: $next] $($rest)*)
    };
    (@path $kind:ident $target:ident $sig:tt $mw:tt [$($path:tt)*] $($rest:tt)*) => {
        $crate::__obzenflow_stage_row!(@decorations $kind $target $sig $mw [$($path)*] [] [] $($rest)*)
    };

    (@decorations $kind:ident $target:ident $sig:tt $mw:tt $handler:tt $acks:tt $delivery:tt . system $($rest:tt)*) => {
        compile_error!("`.system(…)` was removed; the handler holds the system it calls in a `#[system]` field, and `fx.call(request)` performs the request through it")
    };
    (@decorations $kind:ident $target:ident $sig:tt $mw:tt $handler:tt [$($acks:tt)*] $delivery:tt . at_least_once :: < $effect:ty > () $($rest:tt)*) => {
        $crate::__obzenflow_stage_row!(@decorations $kind $target $sig $mw $handler [$($acks)* $crate::__private::EffectAcknowledgement::at_least_once::<$effect>(stringify!($effect)),] $delivery $($rest)*)
    };
    (@decorations $kind:ident $target:ident $sig:tt $mw:tt $handler:tt [$($acks:tt)*] $delivery:tt . transactional :: < $effect:ty > () $($rest:tt)*) => {
        $crate::__obzenflow_stage_row!(@decorations $kind $target $sig $mw $handler [$($acks)* $crate::__private::EffectAcknowledgement::transactional::<$effect>(stringify!($effect)),] $delivery $($rest)*)
    };
    (@decorations $kind:ident $target:ident $sig:tt $mw:tt $handler:tt $acks:tt $delivery:tt . at_least_once $($rest:tt)*) => {
        compile_error!("an acknowledgement names its effect, as in `.at_least_once::<ChargeCard>()`")
    };
    (@decorations $kind:ident $target:ident $sig:tt $mw:tt $handler:tt $acks:tt $delivery:tt . transactional $($rest:tt)*) => {
        compile_error!("an acknowledgement names its effect, as in `.transactional::<WriteLedger>()`")
    };
    (@decorations $kind:ident $target:ident $sig:tt $mw:tt $handler:tt $acks:tt [] . idempotent () $($rest:tt)*) => {
        $crate::__obzenflow_stage_row!(@decorations $kind $target $sig $mw $handler $acks [idempotent] $($rest)*)
    };
    (@decorations $kind:ident $target:ident $sig:tt $mw:tt $handler:tt $acks:tt [] . non_idempotent () $($rest:tt)*) => {
        $crate::__obzenflow_stage_row!(@decorations $kind $target $sig $mw $handler $acks [non_idempotent] $($rest)*)
    };
    (@decorations $kind:ident $target:ident $sig:tt $mw:tt $handler:tt $acks:tt [$delivery:ident] . idempotent () $($rest:tt)*) => {
        compile_error!("a sink classifies its redelivery once, with `.idempotent()` or `.non_idempotent()`")
    };
    (@decorations $kind:ident $target:ident $sig:tt $mw:tt $handler:tt $acks:tt [$delivery:ident] . non_idempotent () $($rest:tt)*) => {
        compile_error!("a sink classifies its redelivery once, with `.idempotent()` or `.non_idempotent()`")
    };
    (@decorations $kind:ident $target:ident $sig:tt $mw:tt $handler:tt $acks:tt $delivery:tt with $($rest:tt)*) => {
        compile_error!("`with` comes before `=>`; write `with …` after the stage's types")
    };
    (@decorations $kind:ident $target:ident $sig:tt $mw:tt $handler:tt $acks:tt $delivery:tt , delivery : $($rest:tt)*) => {
        compile_error!("`delivery:` was removed; classify the sink after its handler, as in `=> shipping_handoff.idempotent()`")
    };
    (@decorations $kind:ident $target:ident $sig:tt $mw:tt $handler:tt $acks:tt $delivery:tt , backpressure : $($rest:tt)*) => {
        compile_error!("`backpressure:` was removed; set backpressure in configuration, under `[runtime.backpressure.flow]` or `[runtime.backpressure.stages.<stage>]`")
    };
    (@decorations $kind:ident $target:ident $sig:tt $mw:tt $handler:tt $acks:tt $delivery:tt , emit_interval $($rest:tt)*) => {
        compile_error!("`emit_interval` was removed from stage rows; a stateful handler declares its interval with `emit_interval_hint()`")
    };
    (@decorations $kind:ident $target:ident $sig:tt $mw:tt $handler:tt $acks:tt $delivery:tt $(,)?) => {
        $crate::__obzenflow_stage_row!(@emit $kind $target $sig $mw $handler $acks $delivery)
    };
    (@decorations $kind:ident $target:ident $sig:tt $mw:tt $handler:tt $acks:tt $delivery:tt $($rest:tt)+) => {
        compile_error!("a stage handler is a name, optionally followed by its decorations; build values with `let` above `flow!`")
    };

    (@emit plain $target:ident [$($sig:tt)*] [$($mw:tt)*] [$($handler:tt)*] [] []) => {
        $crate::$target!($($sig)* => $($handler)* @attachments [$($mw)*])
    };
    (@emit sink $target:ident [$($sig:tt)*] [$($mw:tt)*] [$($handler:tt)*] [] [$($delivery:ident)?]) => {
        $crate::$target!($($sig)* => $($handler)* @attachments [$($mw)*] $(@delivery $delivery)?)
    };
    (@emit effectful $target:ident [$($sig:tt)*] [$($mw:tt)*] [$($handler:tt)*] [$($acks:tt)*] []) => {
        $crate::$target!($($sig)* => $($handler)* @attachments [$($mw)*] @acks [$($acks)*])
    };
    (@emit $kind:ident $target:ident $sig:tt $mw:tt $handler:tt [$($acks:tt)+] $delivery:tt) => {
        compile_error!("`.at_least_once::<E>()` and `.transactional::<E>()` acknowledge an effect an effectful handler performs; only `effectful_transform!` and `effectful_stateful!` rows acknowledge effects")
    };
    (@emit $kind:ident $target:ident $sig:tt $mw:tt $handler:tt $acks:tt [$delivery:ident]) => {
        compile_error!("`.idempotent()` and `.non_idempotent()` classify a sink's redelivery; only `sink!` rows take them")
    };
}

/// Middleware syntax lowering kept for `inference!` and `ai_map_reduce!`,
/// whose rows still name their effect.
#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_attachment_syntax {
    (@before $target:ident, [$($out:tt)*], with { $($members:tt)* } $($rest:tt)*) => {{
        $crate::__obzenflow_flat_attachment_group!($($members)*);
        $crate::__obzenflow_attachment_syntax!(@before $target, [$($out)* with { $($members)* }], $($rest)*)
    }};
    (@before $target:ident, [$($out:tt)*], => $($rest:tt)*) => {
        $crate::__obzenflow_attachment_syntax!(@after $target, [$($out)* =>], $($rest)*)
    };
    (@before $target:ident, [$($out:tt)*], $next:tt $($rest:tt)*) => {
        $crate::__obzenflow_attachment_syntax!(@before $target, [$($out)* $next], $($rest)*)
    };
    (@finish $target:ident, [$($out:tt)*], ,) => { $crate::$target!($($out)*) };
    (@finish $target:ident, [$($out:tt)*],) => { $crate::$target!($($out)*) };
    (@finish $target:ident, [$($out:tt)*], $next:tt $($rest:tt)*) => {
        $crate::__obzenflow_attachment_syntax!(@finish $target, [$($out)* $next], $($rest)*)
    };
    (@after $target:ident, [$($out:tt)*], @ $($rest:tt)*) => {
        compile_error!("expected a named implementation optionally followed by `with a` or `with { a, b }`")
    };
    (@after $target:ident, [$($out:tt)*], with {}) => {
        compile_error!("middleware groups must be non-empty; omit `with` when no middleware is attached")
    };
    (@after $target:ident, [$($out:tt)*], with {} $($rest:tt)+) => {
        compile_error!("middleware groups must be non-empty; omit `with` when no middleware is attached")
    };
    (@after $target:ident, [$($out:tt)*], with { $($members:tt)* } $($rest:tt)*) => {{
        $crate::__obzenflow_flat_attachment_group!($($members)*);
        $crate::__obzenflow_attachment_syntax!(@group $target, [$($out)*], { $($members)* } $($rest)*)
    }};
    (@group $target:ident, [$($out:tt)*], { $($members:expr),+ $(,)? } $($rest:tt)*) => {
        $crate::__obzenflow_attachment_syntax!(@finish $target, [$($out)* @attachments [$($members),+]], $($rest)*)
    };
    (@after $target:ident, [$($out:tt)*], with $member:expr, $($rest:tt)*) => {
        $crate::__obzenflow_attachment_syntax!(@finish $target, [$($out)* @attachments [$member]], , $($rest)*)
    };
    (@after $target:ident, [$($out:tt)*], with $member:expr) => {
        $crate::$target!($($out)* @attachments [$member])
    };
    (@after $target:ident, [$($out:tt)*], ,) => { $crate::$target!($($out)*) };
    (@after $target:ident, [$($out:tt)*], $next:tt $($rest:tt)*) => {
        $crate::__obzenflow_attachment_syntax!(@after $target, [$($out)* $next], $($rest)*)
    };
    (@after $target:ident, [$($out:tt)*],) => { $crate::$target!($($out)*) };
    (@before $target:ident, [$($out:tt)*],) => { $crate::$target!($($out)*) };
}

#[macro_export]
macro_rules! ai_map_reduce {
    ($($tokens:tt)*) => { $crate::__obzenflow_attachment_syntax!(@before __obzenflow_ai_map_reduce_attachment_contract, [], $($tokens)*) };
}

#[macro_export]
macro_rules! async_infinite_source {
    ($($tokens:tt)*) => { $crate::__obzenflow_stage_row!(@sig plain __obzenflow_async_infinite_source_attachment_contract [] $($tokens)*) };
}

#[macro_export]
macro_rules! async_source {
    ($($tokens:tt)*) => { $crate::__obzenflow_stage_row!(@sig plain __obzenflow_async_source_attachment_contract [] $($tokens)*) };
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
/// # use obzenflow_runtime::effects::{Call, CallRequest, EffectContext, EffectError, EffectHandler, Effects, Idempotent, StageCompletion, System};
/// # use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Fact;
/// # impl obzenflow_core::TypedPayload for Fact { const EVENT_TYPE: &'static str = "group.fact"; }
/// # #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// # trait GroupPort: Send + Sync {}
/// # #[derive(Clone, Debug, serde::Serialize)]
/// # struct FirstRequest;
/// # #[async_trait]
/// # impl CallRequest for FirstRequest {
/// #     const EFFECT_TYPE: &'static str = "group.first";
/// #     type Safety = Idempotent;
/// #     type Port = dyn GroupPort;
/// #     type Outcome = Fact;
/// #     async fn call(&self, _: &dyn GroupPort, _: &mut EffectContext) -> Result<Fact, EffectError> { Ok(Fact) }
/// # }
/// # #[derive(Clone, Debug, serde::Serialize)]
/// # struct SecondRequest;
/// # #[async_trait]
/// # impl CallRequest for SecondRequest {
/// #     const EFFECT_TYPE: &'static str = "group.second";
/// #     type Safety = Idempotent;
/// #     type Port = dyn GroupPort;
/// #     type Outcome = Fact;
/// #     async fn call(&self, _: &dyn GroupPort, _: &mut EffectContext) -> Result<Fact, EffectError> { Ok(Fact) }
/// # }
/// # type First = Call<FirstRequest>;
/// # type Second = Call<SecondRequest>;
/// # #[derive(Clone, Debug, EffectHandler)]
/// # #[effect_handler(effects = obzenflow_runtime::effects)]
/// # struct Handler {
/// #     #[system]
/// #     group: System<dyn GroupPort>,
/// # }
/// # #[async_trait]
/// # impl EffectfulTransformHandler for Handler {
/// #     type Input = Fact;
/// #     type Output = Fact;
/// #     type AllowedEffects = obzenflow_runtime::effect_set![First, Second];
/// #     async fn process(&self, _: Fact, _: &mut Effects<Fact, Self::AllowedEffects>) -> Result<StageCompletion<Fact>, HandlerError> { unimplemented!() }
/// # }
/// # fn build(handler: Handler) {
/// let first_limit = rate_limit(10.0).for_effect::<First>();
/// let stage = obzenflow_dsl::effectful_transform!(
///     Fact -> Fact with first_limit => handler
/// );
/// # }
/// ```
///
/// Middleware groups are flat; a nested group does not compile.
///
/// ```compile_fail
/// # use async_trait::async_trait;
/// # use obzenflow_adapters::middleware::rate_limit;
/// # use obzenflow_runtime::effects::{Call, CallRequest, EffectContext, EffectError, EffectHandler, Effects, Idempotent, StageCompletion, System};
/// # use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Fact;
/// # impl obzenflow_core::TypedPayload for Fact { const EVENT_TYPE: &'static str = "group.fact"; }
/// # #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// # trait GroupPort: Send + Sync {}
/// # #[derive(Clone, Debug, serde::Serialize)]
/// # struct FirstRequest;
/// # #[async_trait]
/// # impl CallRequest for FirstRequest {
/// #     const EFFECT_TYPE: &'static str = "group.first";
/// #     type Safety = Idempotent;
/// #     type Port = dyn GroupPort;
/// #     type Outcome = Fact;
/// #     async fn call(&self, _: &dyn GroupPort, _: &mut EffectContext) -> Result<Fact, EffectError> { Ok(Fact) }
/// # }
/// # type First = Call<FirstRequest>;
/// # #[derive(Clone, Debug, EffectHandler)]
/// # #[effect_handler(effects = obzenflow_runtime::effects)]
/// # struct Handler {
/// #     #[system]
/// #     group: System<dyn GroupPort>,
/// # }
/// # #[async_trait]
/// # impl EffectfulTransformHandler for Handler {
/// #     type Input = Fact;
/// #     type Output = Fact;
/// #     type AllowedEffects = obzenflow_runtime::effect_set![First];
/// #     async fn process(&self, _: Fact, _: &mut Effects<Fact, Self::AllowedEffects>) -> Result<StageCompletion<Fact>, HandlerError> { unimplemented!() }
/// # }
/// # fn build(handler: Handler) {
/// let stage = obzenflow_dsl::effectful_transform!(
///     Fact -> Fact with { { rate_limit(10.0) } } => handler
/// );
/// # }
/// ```
///
/// A handler that performs a request holds the system that answers it in a
/// `#[system]` field, so the row names only the handler and `fx.call` takes
/// only the request.
///
/// ```
/// # use async_trait::async_trait;
/// # use obzenflow_core::TypedPayload;
/// # use obzenflow_runtime::effects::{Call, CallRequest, EffectContext, EffectError, EffectHandler, Effects, Idempotent, StageCompletion, System, SystemError};
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
/// # use serde::{Deserialize, Serialize};
/// # use std::sync::Arc;
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Order;
/// # impl TypedPayload for Order { const EVENT_TYPE: &'static str = "row_doc.order"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Charged;
/// # impl TypedPayload for Charged { const EVENT_TYPE: &'static str = "row_doc.charged"; }
/// # #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// # trait CardPort: Send + Sync { fn charge(&self) -> Result<Charged, EffectError>; }
/// # struct Card;
/// # impl CardPort for Card { fn charge(&self) -> Result<Charged, EffectError> { Ok(Charged) } }
/// # #[derive(Clone, Debug, Serialize)]
/// # struct ChargeCardRequest;
/// # #[async_trait]
/// # impl CallRequest for ChargeCardRequest {
/// #     const EFFECT_TYPE: &'static str = "row_doc.charge_card";
/// #     type Safety = Idempotent;
/// #     type Port = dyn CardPort;
/// #     type Outcome = Charged;
/// #     async fn call(&self, card: &dyn CardPort, _: &mut EffectContext) -> Result<Charged, EffectError> { card.charge() }
/// # }
/// # type ChargeCard = Call<ChargeCardRequest>;
/// #[derive(Clone, Debug, EffectHandler)]
/// # #[effect_handler(effects = obzenflow_runtime::effects)]
/// struct Checkout {
///     #[system]
///     card: System<dyn CardPort>,
/// }
///
/// #[async_trait]
/// impl EffectfulTransformHandler for Checkout {
///     type Input = Order;
///     type Output = obzenflow_core::stage_fact_set![Charged];
///     type AllowedEffects = obzenflow_runtime::effect_set![ChargeCard];
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
/// # fn main() -> Result<(), SystemError> {
/// let card = System::eager("card", Arc::new(Card))?;
/// let checkout = Checkout { card };
/// let stage = obzenflow_dsl::effectful_transform!(Order -> Charged => checkout);
/// # Ok(())
/// # }
/// ```
///
/// A field left unmarked holds no system for the stage, so a named effect
/// without a `#[system]` field does not compile:
///
/// ```compile_fail,E0277
/// # use async_trait::async_trait;
/// # use obzenflow_core::TypedPayload;
/// # use obzenflow_runtime::effects::{Call, CallRequest, EffectContext, EffectError, EffectHandler, Effects, Idempotent, StageCompletion, System};
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Order;
/// # impl TypedPayload for Order { const EVENT_TYPE: &'static str = "row_doc.order"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Charged;
/// # impl TypedPayload for Charged { const EVENT_TYPE: &'static str = "row_doc.charged"; }
/// # trait CardPort: Send + Sync { fn charge(&self) -> Result<Charged, EffectError>; }
/// # #[derive(Clone, Debug, Serialize)]
/// # struct ChargeCardRequest;
/// # #[async_trait]
/// # impl CallRequest for ChargeCardRequest {
/// #     const EFFECT_TYPE: &'static str = "row_doc.charge_card";
/// #     type Safety = Idempotent;
/// #     type Port = dyn CardPort;
/// #     type Outcome = Charged;
/// #     async fn call(&self, card: &dyn CardPort, _: &mut EffectContext) -> Result<Charged, EffectError> { card.charge() }
/// # }
/// # type ChargeCard = Call<ChargeCardRequest>;
/// #[derive(Clone, Debug, EffectHandler)]
/// # #[effect_handler(effects = obzenflow_runtime::effects)]
/// struct Checkout {
///     card: System<dyn CardPort>,
/// }
/// # #[async_trait]
/// # impl EffectfulTransformHandler for Checkout {
/// #     type Input = Order;
/// #     type Output = obzenflow_core::stage_fact_set![Charged];
/// #     type AllowedEffects = obzenflow_runtime::effect_set![ChargeCard];
/// #     async fn process(&self, _: Order, fx: &mut Effects<Self::Output, Self::AllowedEffects>) -> Result<StageCompletion<Self::Output>, HandlerError> {
/// #         fx.call(ChargeCardRequest).await?;
/// #         Ok(fx.complete()?)
/// #     }
/// # }
/// # fn build(card: System<dyn CardPort>) {
/// let checkout = Checkout { card };
/// let stage = obzenflow_dsl::effectful_transform!(Order -> Charged => checkout);
/// # }
/// ```
///
/// Neither does a `#[system]` field that answers none of the handler's
/// effects:
///
/// ```compile_fail,E0277
/// # use async_trait::async_trait;
/// # use obzenflow_core::TypedPayload;
/// # use obzenflow_runtime::effects::{Call, CallRequest, EffectContext, EffectError, EffectHandler, Effects, Idempotent, StageCompletion, System};
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Order;
/// # impl TypedPayload for Order { const EVENT_TYPE: &'static str = "row_doc.order"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Charged;
/// # impl TypedPayload for Charged { const EVENT_TYPE: &'static str = "row_doc.charged"; }
/// # trait CardPort: Send + Sync { fn charge(&self) -> Result<Charged, EffectError>; }
/// # trait LedgerPort: Send + Sync {}
/// # #[derive(Clone, Debug, Serialize)]
/// # struct ChargeCardRequest;
/// # #[async_trait]
/// # impl CallRequest for ChargeCardRequest {
/// #     const EFFECT_TYPE: &'static str = "row_doc.charge_card";
/// #     type Safety = Idempotent;
/// #     type Port = dyn CardPort;
/// #     type Outcome = Charged;
/// #     async fn call(&self, card: &dyn CardPort, _: &mut EffectContext) -> Result<Charged, EffectError> { card.charge() }
/// # }
/// # type ChargeCard = Call<ChargeCardRequest>;
/// #[derive(Clone, Debug, EffectHandler)]
/// # #[effect_handler(effects = obzenflow_runtime::effects)]
/// struct Checkout {
///     #[system]
///     card: System<dyn CardPort>,
///     #[system]
///     ledger: System<dyn LedgerPort>,
/// }
/// # #[async_trait]
/// # impl EffectfulTransformHandler for Checkout {
/// #     type Input = Order;
/// #     type Output = obzenflow_core::stage_fact_set![Charged];
/// #     type AllowedEffects = obzenflow_runtime::effect_set![ChargeCard];
/// #     async fn process(&self, _: Order, fx: &mut Effects<Self::Output, Self::AllowedEffects>) -> Result<StageCompletion<Self::Output>, HandlerError> {
/// #         fx.call(ChargeCardRequest).await?;
/// #         Ok(fx.complete()?)
/// #     }
/// # }
/// # fn build(card: System<dyn CardPort>, ledger: System<dyn LedgerPort>) {
/// let checkout = Checkout { card, ledger };
/// let stage = obzenflow_dsl::effectful_transform!(Order -> Charged => checkout);
/// # }
/// ```
///
/// Nor do two `#[system]` fields for one port type:
///
/// ```compile_fail,E0283
/// # use async_trait::async_trait;
/// # use obzenflow_core::TypedPayload;
/// # use obzenflow_runtime::effects::{Call, CallRequest, EffectContext, EffectError, EffectHandler, Effects, Idempotent, StageCompletion, System};
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Order;
/// # impl TypedPayload for Order { const EVENT_TYPE: &'static str = "row_doc.order"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Charged;
/// # impl TypedPayload for Charged { const EVENT_TYPE: &'static str = "row_doc.charged"; }
/// # trait CardPort: Send + Sync { fn charge(&self) -> Result<Charged, EffectError>; }
/// # #[derive(Clone, Debug, Serialize)]
/// # struct ChargeCardRequest;
/// # #[async_trait]
/// # impl CallRequest for ChargeCardRequest {
/// #     const EFFECT_TYPE: &'static str = "row_doc.charge_card";
/// #     type Safety = Idempotent;
/// #     type Port = dyn CardPort;
/// #     type Outcome = Charged;
/// #     async fn call(&self, card: &dyn CardPort, _: &mut EffectContext) -> Result<Charged, EffectError> { card.charge() }
/// # }
/// # type ChargeCard = Call<ChargeCardRequest>;
/// #[derive(Clone, Debug, EffectHandler)]
/// # #[effect_handler(effects = obzenflow_runtime::effects)]
/// struct Checkout {
///     #[system]
///     card: System<dyn CardPort>,
///     #[system]
///     spare_card: System<dyn CardPort>,
/// }
/// # #[async_trait]
/// # impl EffectfulTransformHandler for Checkout {
/// #     type Input = Order;
/// #     type Output = obzenflow_core::stage_fact_set![Charged];
/// #     type AllowedEffects = obzenflow_runtime::effect_set![ChargeCard];
/// #     async fn process(&self, _: Order, fx: &mut Effects<Self::Output, Self::AllowedEffects>) -> Result<StageCompletion<Self::Output>, HandlerError> {
/// #         fx.call(ChargeCardRequest).await?;
/// #         Ok(fx.complete()?)
/// #     }
/// # }
/// # fn build(card: System<dyn CardPort>, spare_card: System<dyn CardPort>) {
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
/// # use obzenflow_runtime::effects::{Call, CallRequest, EffectContext, EffectError, EffectHandler, Effects, Idempotent, StageCompletion, System};
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Order;
/// # impl TypedPayload for Order { const EVENT_TYPE: &'static str = "row_doc.order"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Charged;
/// # impl TypedPayload for Charged { const EVENT_TYPE: &'static str = "row_doc.charged"; }
/// # #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// # trait CardPort: Send + Sync { fn charge(&self) -> Result<Charged, EffectError>; }
/// # #[derive(Clone, Debug, Serialize)]
/// # struct ChargeCardRequest;
/// # #[async_trait]
/// # impl CallRequest for ChargeCardRequest {
/// #     const EFFECT_TYPE: &'static str = "row_doc.charge_card";
/// #     type Safety = Idempotent;
/// #     type Port = dyn CardPort;
/// #     type Outcome = Charged;
/// #     async fn call(&self, card: &dyn CardPort, _: &mut EffectContext) -> Result<Charged, EffectError> { card.charge() }
/// # }
/// # type ChargeCard = Call<ChargeCardRequest>;
/// # #[derive(Clone, Debug, EffectHandler)]
/// # #[effect_handler(effects = obzenflow_runtime::effects)]
/// # struct Checkout {
/// #     #[system]
/// #     card: System<dyn CardPort>,
/// # }
/// # #[async_trait]
/// # impl EffectfulTransformHandler for Checkout {
/// #     type Input = Order;
/// #     type Output = obzenflow_core::stage_fact_set![Charged];
/// #     type AllowedEffects = obzenflow_runtime::effect_set![ChargeCard];
/// #     async fn process(&self, _: Order, fx: &mut Effects<Self::Output, Self::AllowedEffects>) -> Result<StageCompletion<Self::Output>, HandlerError> {
/// #         fx.call(ChargeCardRequest).await?;
/// #         Ok(fx.complete()?)
/// #     }
/// # }
/// # fn build(checkout: Checkout) {
/// let stage = obzenflow_dsl::effectful_transform!(Order -> Charged uses ChargeCard => checkout);
/// # }
/// ```
///
/// ```compile_fail
/// # use async_trait::async_trait;
/// # use obzenflow_core::TypedPayload;
/// # use obzenflow_runtime::effects::{Call, CallRequest, EffectContext, EffectError, EffectHandler, Effects, Idempotent, StageCompletion, System};
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Order;
/// # impl TypedPayload for Order { const EVENT_TYPE: &'static str = "row_doc.order"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Charged;
/// # impl TypedPayload for Charged { const EVENT_TYPE: &'static str = "row_doc.charged"; }
/// # #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// # trait CardPort: Send + Sync { fn charge(&self) -> Result<Charged, EffectError>; }
/// # #[derive(Clone, Debug, Serialize)]
/// # struct ChargeCardRequest;
/// # #[async_trait]
/// # impl CallRequest for ChargeCardRequest {
/// #     const EFFECT_TYPE: &'static str = "row_doc.charge_card";
/// #     type Safety = Idempotent;
/// #     type Port = dyn CardPort;
/// #     type Outcome = Charged;
/// #     async fn call(&self, card: &dyn CardPort, _: &mut EffectContext) -> Result<Charged, EffectError> { card.charge() }
/// # }
/// # type ChargeCard = Call<ChargeCardRequest>;
/// # #[derive(Clone, Debug, EffectHandler)]
/// # #[effect_handler(effects = obzenflow_runtime::effects)]
/// # struct Checkout {
/// #     #[system]
/// #     card: System<dyn CardPort>,
/// # }
/// # #[async_trait]
/// # impl EffectfulTransformHandler for Checkout {
/// #     type Input = Order;
/// #     type Output = obzenflow_core::stage_fact_set![Charged];
/// #     type AllowedEffects = obzenflow_runtime::effect_set![ChargeCard];
/// #     async fn process(&self, _: Order, fx: &mut Effects<Self::Output, Self::AllowedEffects>) -> Result<StageCompletion<Self::Output>, HandlerError> {
/// #         fx.call(ChargeCardRequest).await?;
/// #         Ok(fx.complete()?)
/// #     }
/// # }
/// # fn build(checkout: Checkout, card: System<dyn CardPort>) {
/// let stage = obzenflow_dsl::effectful_transform!(Order -> Charged via card => checkout);
/// # }
/// ```
///
/// ```compile_fail
/// # use async_trait::async_trait;
/// # use obzenflow_core::TypedPayload;
/// # use obzenflow_runtime::effects::{Call, CallRequest, EffectContext, EffectError, EffectHandler, Effects, Idempotent, StageCompletion, System};
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Order;
/// # impl TypedPayload for Order { const EVENT_TYPE: &'static str = "row_doc.order"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Charged;
/// # impl TypedPayload for Charged { const EVENT_TYPE: &'static str = "row_doc.charged"; }
/// # #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// # trait CardPort: Send + Sync { fn charge(&self) -> Result<Charged, EffectError>; }
/// # #[derive(Clone, Debug, Serialize)]
/// # struct ChargeCardRequest;
/// # #[async_trait]
/// # impl CallRequest for ChargeCardRequest {
/// #     const EFFECT_TYPE: &'static str = "row_doc.charge_card";
/// #     type Safety = Idempotent;
/// #     type Port = dyn CardPort;
/// #     type Outcome = Charged;
/// #     async fn call(&self, card: &dyn CardPort, _: &mut EffectContext) -> Result<Charged, EffectError> { card.charge() }
/// # }
/// # type ChargeCard = Call<ChargeCardRequest>;
/// # #[derive(Clone, Debug, EffectHandler)]
/// # #[effect_handler(effects = obzenflow_runtime::effects)]
/// # struct Checkout {
/// #     #[system]
/// #     card: System<dyn CardPort>,
/// # }
/// # #[async_trait]
/// # impl EffectfulTransformHandler for Checkout {
/// #     type Input = Order;
/// #     type Output = obzenflow_core::stage_fact_set![Charged];
/// #     type AllowedEffects = obzenflow_runtime::effect_set![ChargeCard];
/// #     async fn process(&self, _: Order, fx: &mut Effects<Self::Output, Self::AllowedEffects>) -> Result<StageCompletion<Self::Output>, HandlerError> {
/// #         fx.call(ChargeCardRequest).await?;
/// #         Ok(fx.complete()?)
/// #     }
/// # }
/// # fn build(checkout: Checkout, card: System<dyn CardPort>) {
/// let stage = obzenflow_dsl::effectful_transform!(Order -> Charged => checkout.system(card));
/// # }
/// ```
///
/// ```compile_fail
/// # use async_trait::async_trait;
/// # use obzenflow_adapters::middleware::retry;
/// # use obzenflow_core::TypedPayload;
/// # use obzenflow_runtime::effects::{Call, CallRequest, EffectContext, EffectError, EffectHandler, Effects, Idempotent, StageCompletion, System};
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Order;
/// # impl TypedPayload for Order { const EVENT_TYPE: &'static str = "row_doc.order"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Charged;
/// # impl TypedPayload for Charged { const EVENT_TYPE: &'static str = "row_doc.charged"; }
/// # #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// # trait CardPort: Send + Sync { fn charge(&self) -> Result<Charged, EffectError>; }
/// # #[derive(Clone, Debug, Serialize)]
/// # struct ChargeCardRequest;
/// # #[async_trait]
/// # impl CallRequest for ChargeCardRequest {
/// #     const EFFECT_TYPE: &'static str = "row_doc.charge_card";
/// #     type Safety = Idempotent;
/// #     type Port = dyn CardPort;
/// #     type Outcome = Charged;
/// #     async fn call(&self, card: &dyn CardPort, _: &mut EffectContext) -> Result<Charged, EffectError> { card.charge() }
/// # }
/// # type ChargeCard = Call<ChargeCardRequest>;
/// # #[derive(Clone, Debug, EffectHandler)]
/// # #[effect_handler(effects = obzenflow_runtime::effects)]
/// # struct Checkout {
/// #     #[system]
/// #     card: System<dyn CardPort>,
/// # }
/// # #[async_trait]
/// # impl EffectfulTransformHandler for Checkout {
/// #     type Input = Order;
/// #     type Output = obzenflow_core::stage_fact_set![Charged];
/// #     type AllowedEffects = obzenflow_runtime::effect_set![ChargeCard];
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
/// # use obzenflow_runtime::effects::{Call, CallRequest, EffectContext, EffectError, EffectHandler, Effects, Idempotent, StageCompletion, System};
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Order;
/// # impl TypedPayload for Order { const EVENT_TYPE: &'static str = "row_doc.order"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Charged;
/// # impl TypedPayload for Charged { const EVENT_TYPE: &'static str = "row_doc.charged"; }
/// # #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// # trait CardPort: Send + Sync { fn charge(&self) -> Result<Charged, EffectError>; }
/// # #[derive(Clone, Debug, Serialize)]
/// # struct ChargeCardRequest;
/// # #[async_trait]
/// # impl CallRequest for ChargeCardRequest {
/// #     const EFFECT_TYPE: &'static str = "row_doc.charge_card";
/// #     type Safety = Idempotent;
/// #     type Port = dyn CardPort;
/// #     type Outcome = Charged;
/// #     async fn call(&self, card: &dyn CardPort, _: &mut EffectContext) -> Result<Charged, EffectError> { card.charge() }
/// # }
/// # type ChargeCard = Call<ChargeCardRequest>;
/// # #[derive(Clone, Debug, EffectHandler)]
/// # #[effect_handler(effects = obzenflow_runtime::effects)]
/// # struct Checkout {
/// #     #[system]
/// #     card: System<dyn CardPort>,
/// # }
/// # impl Checkout {
/// #     fn new(card: System<dyn CardPort>) -> Self { Self { card } }
/// # }
/// # #[async_trait]
/// # impl EffectfulTransformHandler for Checkout {
/// #     type Input = Order;
/// #     type Output = obzenflow_core::stage_fact_set![Charged];
/// #     type AllowedEffects = obzenflow_runtime::effect_set![ChargeCard];
/// #     async fn process(&self, _: Order, fx: &mut Effects<Self::Output, Self::AllowedEffects>) -> Result<StageCompletion<Self::Output>, HandlerError> {
/// #         fx.call(ChargeCardRequest).await?;
/// #         Ok(fx.complete()?)
/// #     }
/// # }
/// # fn build(card: System<dyn CardPort>) {
/// let stage = obzenflow_dsl::effectful_transform!(Order -> Charged => Checkout::new(card));
/// # }
/// ```
#[macro_export]
macro_rules! effectful_transform {
    ($($tokens:tt)*) => { $crate::__obzenflow_stage_row!(@sig effectful __obzenflow_effectful_transform_attachment_contract [] $($tokens)*) };
}

#[macro_export]
macro_rules! inference {
    ($($tokens:tt)*) => { $crate::__obzenflow_attachment_syntax!(@before __obzenflow_inference_attachment_contract, [], $($tokens)*) };
}

#[macro_export]
macro_rules! infinite_source {
    ($($tokens:tt)*) => { $crate::__obzenflow_stage_row!(@sig plain __obzenflow_infinite_source_attachment_contract [] $($tokens)*) };
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

/// Declares a typed sink. `.idempotent()` or `.non_idempotent()` after the
/// handler classifies its redelivery.
///
/// ```
/// # use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
/// # use obzenflow_runtime::stages::common::handlers::{InlineSink, SinkDescription, SinkWriteFailure};
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Fact;
/// # impl obzenflow_core::TypedPayload for Fact { const EVENT_TYPE: &'static str = "sink_doc.fact"; }
/// # #[derive(Clone, Debug)]
/// # struct Ledger;
/// # #[async_trait::async_trait]
/// # impl InlineSink for Ledger {
/// #     type Input = Fact;
/// #     fn describe(&self) -> SinkDescription { SinkDescription::method(DeliveryMethod::Noop) }
/// #     async fn write(&mut self, _: Fact) -> Result<(), SinkWriteFailure> { Ok(()) }
/// # }
/// let ledger = Ledger;
/// let sink = obzenflow_dsl::sink!(Fact => ledger.idempotent());
/// ```
///
/// The `delivery:` clause was removed.
///
/// ```compile_fail
/// # use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
/// # use obzenflow_runtime::stages::common::handlers::{InlineSink, SinkDescription, SinkWriteFailure};
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Fact;
/// # impl obzenflow_core::TypedPayload for Fact { const EVENT_TYPE: &'static str = "sink_doc.fact"; }
/// # #[derive(Clone, Debug)]
/// # struct Ledger;
/// # #[async_trait::async_trait]
/// # impl InlineSink for Ledger {
/// #     type Input = Fact;
/// #     fn describe(&self) -> SinkDescription { SinkDescription::method(DeliveryMethod::Noop) }
/// #     async fn write(&mut self, _: Fact) -> Result<(), SinkWriteFailure> { Ok(()) }
/// # }
/// let ledger = Ledger;
/// let sink = obzenflow_dsl::sink!(Fact => ledger, delivery: idempotent);
/// ```
#[macro_export]
macro_rules! sink {
    ($($tokens:tt)*) => { $crate::__obzenflow_stage_row!(@sig sink __obzenflow_sink_attachment_contract [] $($tokens)*) };
}

/// Declares a typed finite source with optional middleware, written after the
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
#[macro_export]
macro_rules! source {
    ($($tokens:tt)*) => { $crate::__obzenflow_stage_row!(@sig plain __obzenflow_source_attachment_contract [] $($tokens)*) };
}

/// Declares a typed stateful stage. Its handler declares any emit interval
/// through `emit_interval_hint()`; the row takes no interval clause.
///
/// ```
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
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
/// #     fn emit(&self, _: &()) -> Result<StatefulEmission<Fact>, HandlerError> {
/// #         Ok(StatefulEmission::RetainEpoch { outputs: Vec::new() })
/// #     }
/// #     fn post_emit(&self, _: &mut ()) {}
/// # }
/// let totals = Totals;
/// let stage = obzenflow_dsl::stateful!(Fact -> Fact => totals);
/// ```
///
/// ```compile_fail
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
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
/// #     fn emit(&self, _: &()) -> Result<StatefulEmission<Fact>, HandlerError> {
/// #         Ok(StatefulEmission::RetainEpoch { outputs: Vec::new() })
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

/// Shared attachment spelling for a generated composite's local role.
#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_role_attachments {
    ($head:ident $(:: $tail:ident)* with { $($middleware:tt)* } $(,)?) => {{
        $crate::__obzenflow_flat_attachment_group!($($middleware)*);
        $crate::__obzenflow_role_attachments!(@group $head $(:: $tail)* with { $($middleware)* })
    }};
    (@group $head:ident $(:: $tail:ident)* with { $($middleware:expr),+ $(,)? } $(,)?) => {
        ($head $(:: $tail)*, vec![$($crate::__private::capture_middleware($middleware)),+])
    };
    ($head:ident $(:: $tail:ident)* with $middleware:expr $(,)?) => {
        ($head $(:: $tail)*, vec![$crate::__private::capture_middleware($middleware)])
    };
    ($head:ident $(:: $tail:ident)* $(,)?) => {
        ($head $(:: $tail)*, Vec::<Box<dyn $crate::__private::MiddlewareFactory>>::new())
    };
    ($($invalid:tt)*) => { compile_error!("generated roles require a named implementation, optionally followed by `with a` or `with { a, b }`") };
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
