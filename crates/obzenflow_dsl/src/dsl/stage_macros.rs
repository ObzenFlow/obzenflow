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
//! ai!(In -> Out with breaker => handler)
//! ```
//!
//! Every stage macro takes one row
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
        $crate::__private::assert_handler_performs_no_ai_effect(&__handler);
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
        $crate::__private::assert_stateful_handler_performs_no_ai_effect(&__handler);
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

/// Shared stage-row lowering: the stage's types, an optional `with` clause
/// holding its middleware, `=>`, then the handler and its decorations.
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
    (@emit ai $target:ident [$($sig:tt)*] [$($mw:tt)*] [$($handler:tt)*] [] []) => {
        $crate::$target!($($sig)* => $($handler)* @attachments [$($mw)*])
    };
    (@emit ai $target:ident $sig:tt $mw:tt $handler:tt [$($acks:tt)+] $delivery:tt) => {
        compile_error!("an `ai!` row is itself the acknowledgement for its AI effect; remove `.at_least_once::<E>()`")
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

/// Declares an AI stage over a handler built with `ai::infer` or `ai::map`.
/// The row's arrow equals the handler's input and output, and middleware is
/// optional.
///
/// ```ignore
/// let digest = ai::map(roles.clone()).chunks(10).reduce(roles).on_eof();
/// let ai_breaker = circuit_breaker().consecutive_failures(5);
///
/// digest = ai!(FormattedStory -> HnDigestSummary with ai_breaker => digest);
/// ```
#[macro_export]
macro_rules! ai {
    ($($tokens:tt)*) => { $crate::__obzenflow_stage_row!(@sig ai __obzenflow_ai_attachment_contract [] $($tokens)*) };
}

/// Retired; build the handler with `ai::infer` and declare the stage with `ai!`.
#[macro_export]
macro_rules! inference {
    ($($tokens:tt)*) => {
        compile_error!("inference! and ai_map_reduce! were replaced by ai!; build the handler with ai::infer or ai::map")
    };
}

/// Retired; build the handler with `ai::map` and declare the stage with `ai!`.
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
            <$crate::__private::stage_fact_set![$($member),+] as $crate::__private::StageFactSet>::Members:
                $crate::dsl::typing::ArrowOutputsAreDeclaredByHandler<
                    <<H as $crate::dsl::ai_stage::AiStageHandler>::Out as $crate::__private::StageFactSet>::Members,
                    ArrowToHandlerProof,
                >,
            <<H as $crate::dsl::ai_stage::AiStageHandler>::Out as $crate::__private::StageFactSet>::Members:
                $crate::dsl::typing::HandlerOutputsAreDeclaredByArrow<
                    <$crate::__private::stage_fact_set![$($member),+] as $crate::__private::StageFactSet>::Members,
                    HandlerToArrowProof,
                >,
        {}

        __obzenflow_assert_ai_outputs::<_, _, _>(&$handler);
        const _: () = $crate::__private::assert_distinct_stage_fact_set::<
            $crate::__private::stage_fact_set![$($member),+],
        >();
    }};
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

/// A middleware group is a flat collection, not a nested execution stack.
#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_flat_attachment_group {
    () => { compile_error!("middleware groups must be non-empty; omit `with` when no middleware is attached") };
    ({ $($nested:tt)* } $($rest:tt)*) => { compile_error!("middleware groups must be flat; attach each definition directly") };
    ($member:expr, $($rest:tt)+) => { $crate::__obzenflow_flat_attachment_group!($($rest)+) };
    ($member:expr $(,)?) => {};
}
