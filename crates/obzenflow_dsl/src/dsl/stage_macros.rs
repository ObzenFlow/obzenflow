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
//! async_infinite_source!(Out => handler)
//! transform!(In -> Out => handler)
//! effectful_transform!(In -> Out uses Effect => handler, observers: [])
//! stateful!(In -> Out => handler)
//! effectful_stateful!(In -> Out uses Effect => handler, observers: [])
//! sink!(In => handler)
//! join!(catalog CatalogStage: Catalog, Stream -> Out => handler)
//! inference!(In -> Out uses at_least_once(ChatCompletion) via chat with policy => handler)
//! ai_map_reduce!(Seed -> Out => { /* named roles */ }, chunking: by_budget { /* ... */ })
//! ```
//!
//! Ordinary handler and role slots accept only a local name, unit value path,
//! or qualified value path made from identifier segments. Calls, builder
//! chains, closures, and struct literals are rejected with a diagnostic that
//! teaches the binding idiom. Builder-owned values are constructed as ordinary
//! Rust inside [`FlowDefinition::materialize`](crate::FlowDefinition::materialize),
//! immediately above the inner `flow!`, and only their names appear here.
//! `placeholder!()` and `placeholder!("reason")` remain the sketching forms.
//!
//! The decoration matrix covers binding-derived and explicit names plus each
//! family's applicable contract, control `with`, observer, backpressure, effect,
//! emit-interval, delivery, and catalog clauses. Async-source poll timeout is
//! configured on the handler and exposed through its `poll_timeout()` method;
//! it is not a stage-macro clause or positional tuple.
//!
//! The pre-FLOWIP-114c untyped forms (`source!(handler)`,
//! `transform!(handler)`, `sink!(handler)`, etc.) and the mixed-leg join arms
//! (`join!(catalog Ref: mixed, ...)`) were removed in the PR that operationalised
//! FLOWIP-114c. Authoring patterns that previously reached for an untyped
//! handler to demux events at runtime are now expressed as joins (two typed
//! inputs) or per-branch alignment transforms (homogeneous fan-in on an
//! envelope type); see `examples/multi_source_ingest_demo` for the canonical
//! pattern and FLOWIP-114c "How to handle heterogeneous fan-in" for the
//! rationale.
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

/// Teaching diagnostic for the retired authority-erasing stage clause.
#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_stage_middleware_removed {
    () => {
        compile_error!("'middleware:' has been removed; use 'observers:' for passive observer middleware and attach control middleware with the live I/O unit it protects (FLOWIP-115s)")
    };
}

/// Shared proof-carrying source admission used by all four source families.
#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_admit_typed_source {
    (factory = $factory:ident, output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = $handler:expr, source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        const _: () = ::obzenflow_core::assert_distinct_stage_fact_set::<
            ::obzenflow_core::stage_fact_set![$($member),+],
        >();
        let __source_policies: Vec<Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory>> =
            vec![$(Box::new($policy)),*];
        let __ingress_policy: Option<Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory>> =
            None $(.or_else(|| Some(Box::new($ingress) as Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory>)))?;
        let __observers: Vec<Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory>> =
            vec![$(Box::new($observer)),*];
        #[allow(unused_mut)]
        let mut __backpressure: Option<$crate::dsl::backpressure_clause::BackpressureClause> = None;
        $($( __backpressure = Some($bp); )?)?
        $crate::dsl::typing::$factory::<
            _,
            $out,
            ::obzenflow_core::stage_fact_set![$($member),+],
            _,
            _,
            _,
        >(
            $name,
            $handler,
            __source_policies,
            __ingress_policy,
            __observers,
            __backpressure,
        )
    }};
    (factory = $factory:ident, output = $out:ty, name = $name:literal, handler = $handler:expr, source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __source_policies: Vec<Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory>> =
            vec![$(Box::new($policy)),*];
        let __ingress_policy: Option<Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory>> =
            None $(.or_else(|| Some(Box::new($ingress) as Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory>)))?;
        let __observers: Vec<Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory>> =
            vec![$(Box::new($observer)),*];
        #[allow(unused_mut)]
        let mut __backpressure: Option<$crate::dsl::backpressure_clause::BackpressureClause> = None;
        $($( __backpressure = Some($bp); )?)?
        $crate::dsl::typing::$factory::<_, $out, $out, _, _, _>(
            $name,
            $handler,
            __source_policies,
            __ingress_policy,
            __observers,
            __backpressure,
        )
    }};
}

/// Shared structural admission for source placeholders. Placeholders never
/// poll a domain dependency or author a row, so they use the classified raw
/// substrate without exposing a raw-handler macro arm.
#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_admit_placeholder_source {
    (factory = $factory:ident, output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, message = $message:expr, source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        const _: () = ::obzenflow_core::assert_distinct_stage_fact_set::<
            ::obzenflow_core::stage_fact_set![$($member),+],
        >();
        let __source_policies: Vec<Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory>> =
            vec![$(Box::new($policy)),*];
        let __ingress_policy: Option<Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory>> =
            None $(.or_else(|| Some(Box::new($ingress) as Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory>)))?;
        let __observers: Vec<Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory>> =
            vec![$(Box::new($observer)),*];
        #[allow(unused_mut)]
        let mut __backpressure: Option<$crate::dsl::backpressure_clause::BackpressureClause> = None;
        $($( __backpressure = Some($bp); )?)?
        $crate::dsl::typing::$factory::<
            $out,
            ::obzenflow_core::stage_fact_set![$($member),+],
        >(
            $name,
            $message,
            __source_policies,
            __ingress_policy,
            __observers,
            __backpressure,
        )
    }};
    (factory = $factory:ident, output = $out:ty, name = $name:literal, message = $message:expr, source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __source_policies: Vec<Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory>> =
            vec![$(Box::new($policy)),*];
        let __ingress_policy: Option<Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory>> =
            None $(.or_else(|| Some(Box::new($ingress) as Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory>)))?;
        let __observers: Vec<Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory>> =
            vec![$(Box::new($observer)),*];
        #[allow(unused_mut)]
        let mut __backpressure: Option<$crate::dsl::backpressure_clause::BackpressureClause> = None;
        $($( __backpressure = Some($bp); )?)?
        $crate::dsl::typing::$factory::<$out, $out>(
            $name,
            $message,
            __source_policies,
            __ingress_policy,
            __observers,
            __backpressure,
        )
    }};
}

// ============================================================================
// source!  +  __obzenflow_source_typed!
// ============================================================================

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_source_typed {
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = $handler:tt, middleware = [] $(, backpressure = [$($bp:expr)?])?) => {
        compile_error!("source! takes control middleware in a 'with [...]' clause on the feed it protects (FLOWIP-115s)")
    };
    (output = $out:ty, name = $name:literal, handler = $handler:tt, middleware = [] $(, backpressure = [$($bp:expr)?])?) => {
        compile_error!("source! takes control middleware in a 'with [...]' clause on the feed it protects (FLOWIP-115s)")
    };
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = $handler:expr, middleware = [$($legacy:expr),*] $(, backpressure = [$($bp:expr)?])?) => {
        compile_error!("source! takes control middleware in a 'with [...]' clause on the feed it protects (FLOWIP-115s)")
    };
    (output = $out:ty, name = $name:literal, handler = $handler:expr, middleware = [$($legacy:expr),*] $(, backpressure = [$($bp:expr)?])?) => {
        compile_error!("source! takes control middleware in a 'with [...]' clause on the feed it protects (FLOWIP-115s)")
    };
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!(), source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        $crate::__obzenflow_admit_placeholder_source!(factory = placeholder_finite_source_descriptor, output = $out, output_contract = [$($member),+], name = $name, message = None, source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*] $(, backpressure = [$($bp)?])?)
    }};
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!($msg:expr), source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        $crate::__obzenflow_admit_placeholder_source!(factory = placeholder_finite_source_descriptor, output = $out, output_contract = [$($member),+], name = $name, message = Some($msg), source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*] $(, backpressure = [$($bp)?])?)
    }};
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = $handler:expr, source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        $crate::__obzenflow_admit_typed_source!(factory = typed_finite_source_descriptor, output = $out, output_contract = [$($member),+], name = $name, handler = $handler, source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*] $(, backpressure = [$($bp)?])?)
    }};
    (output = $out:ty, name = $name:literal, handler = placeholder!(), source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        $crate::__obzenflow_admit_placeholder_source!(factory = placeholder_finite_source_descriptor, output = $out, name = $name, message = None, source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*] $(, backpressure = [$($bp)?])?)
    }};
    (output = $out:ty, name = $name:literal, handler = placeholder!($msg:expr), source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        $crate::__obzenflow_admit_placeholder_source!(factory = placeholder_finite_source_descriptor, output = $out, name = $name, message = Some($msg), source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*] $(, backpressure = [$($bp)?])?)
    }};
    (output = $out:ty, name = $name:literal, handler = $handler:expr, source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        $crate::__obzenflow_admit_typed_source!(factory = typed_finite_source_descriptor, output = $out, name = $name, handler = $handler, source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*] $(, backpressure = [$($bp)?])?)
    }};
}

/// Create a finite source stage descriptor.
#[macro_export]
macro_rules! source {
    ({ $($out:ty),+ $(,)? } => $handler:expr, middleware: [$($mw:expr),* $(,)?] $($rest:tt)*) => {
        $crate::__obzenflow_stage_middleware_removed!()
    };
    ($out:ty => $handler:expr, middleware: [$($mw:expr),* $(,)?] $($rest:tt)*) => {
        $crate::__obzenflow_stage_middleware_removed!()
    };
    (name: $name:literal, { $($out:ty),+ $(,)? } => $handler:expr, middleware: [$($mw:expr),* $(,)?] $($rest:tt)*) => {
        $crate::__obzenflow_stage_middleware_removed!()
    };
    (name: $name:literal, $out:ty => $handler:expr, middleware: [$($mw:expr),* $(,)?] $($rest:tt)*) => {
        $crate::__obzenflow_stage_middleware_removed!()
    };
    ({ $($out:ty),+ $(,)? } => $handler:expr, ingress with [$($policy:expr),* $(,)?] $($rest:tt)*) => {
        compile_error!("an ingress 'with' takes one policy expression in this release; write 'ingress with <policy>'; ordered ingress chains await FLOWIP-115t (FLOWIP-115s)")
    };
    ($out:ty => $handler:expr, ingress with [$($policy:expr),* $(,)?] $($rest:tt)*) => {
        compile_error!("an ingress 'with' takes one policy expression in this release; write 'ingress with <policy>'; ordered ingress chains await FLOWIP-115t (FLOWIP-115s)")
    };
    (name: $name:literal, { $($out:ty),+ $(,)? } => $handler:expr, ingress with [$($policy:expr),* $(,)?] $($rest:tt)*) => {
        compile_error!("an ingress 'with' takes one policy expression in this release; write 'ingress with <policy>'; ordered ingress chains await FLOWIP-115t (FLOWIP-115s)")
    };
    (name: $name:literal, $out:ty => $handler:expr, ingress with [$($policy:expr),* $(,)?] $($rest:tt)*) => {
        compile_error!("an ingress 'with' takes one policy expression in this release; write 'ingress with <policy>'; ordered ingress chains await FLOWIP-115t (FLOWIP-115s)")
    };
    ({ $($out:ty),+ $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* with [$($source_policy:expr),* $(,)?], ingress with [$($policy:expr),* $(,)?] $($rest:tt)*) => {
        compile_error!("an ingress 'with' takes one policy expression in this release; write 'ingress with <policy>'; ordered ingress chains await FLOWIP-115t (FLOWIP-115s)")
    };
    ($out:ty => $handler_head:ident $(:: $handler_tail:ident)* with [$($source_policy:expr),* $(,)?], ingress with [$($policy:expr),* $(,)?] $($rest:tt)*) => {
        compile_error!("an ingress 'with' takes one policy expression in this release; write 'ingress with <policy>'; ordered ingress chains await FLOWIP-115t (FLOWIP-115s)")
    };
    (name: $name:literal, { $($out:ty),+ $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* with [$($source_policy:expr),* $(,)?], ingress with [$($policy:expr),* $(,)?] $($rest:tt)*) => {
        compile_error!("an ingress 'with' takes one policy expression in this release; write 'ingress with <policy>'; ordered ingress chains await FLOWIP-115t (FLOWIP-115s)")
    };
    (name: $name:literal, $out:ty => $handler_head:ident $(:: $handler_tail:ident)* with [$($source_policy:expr),* $(,)?], ingress with [$($policy:expr),* $(,)?] $($rest:tt)*) => {
        compile_error!("an ingress 'with' takes one policy expression in this release; write 'ingress with <policy>'; ordered ingress chains await FLOWIP-115t (FLOWIP-115s)")
    };
    // FLOWIP-115s: grammar positions retain authority structurally.
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!() $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = "__obzenflow_binding_derived_name__", handler = placeholder!(), source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?)
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr) $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = "__obzenflow_binding_derived_name__", handler = placeholder!($msg), source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?)
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = "__obzenflow_binding_derived_name__", handler = $handler_head $(:: $handler_tail)*, source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?)
    };
    ($out:ty => placeholder!() $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = placeholder!(), source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?)
    };
    ($out:ty => placeholder!($msg:expr) $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = placeholder!($msg), source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?)
    };
    ($out:ty => $handler_head:ident $(:: $handler_tail:ident)* $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = $handler_head $(:: $handler_tail)*, source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?)
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!() $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = placeholder!(), source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?)
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr) $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = placeholder!($msg), source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?)
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = $handler_head $(:: $handler_tail)*, source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?)
    };
    (name: $name:literal, $out:ty => placeholder!() $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_source_typed!(output = $out, name = $name, handler = placeholder!(), source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?)
    };
    (name: $name:literal, $out:ty => placeholder!($msg:expr) $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_source_typed!(output = $out, name = $name, handler = placeholder!($msg), source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?)
    };
    (name: $name:literal, $out:ty => $handler_head:ident $(:: $handler_tail:ident)* $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_source_typed!(output = $out, name = $name, handler = $handler_head $(:: $handler_tail)*, source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?)
    };
    // ── typed (binding-derived name) ──
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!() $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!(),
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr) $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!($msg),
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!(), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!(),
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!($msg),
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = "__obzenflow_binding_derived_name__",
            handler = $handler_head $(:: $handler_tail)*,
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = "__obzenflow_binding_derived_name__",
            handler = $handler_head $(:: $handler_tail)*,
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    ($out:ty => placeholder!() $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!(),
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    ($out:ty => placeholder!($msg:expr) $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!($msg),
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    ($out:ty => placeholder!(), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!(),
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    ($out:ty => placeholder!($msg:expr), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!($msg),
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    ($out:ty => $handler_head:ident $(:: $handler_tail:ident)* $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = $handler_head $(:: $handler_tail)*,
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    ($out:ty => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = $handler_head $(:: $handler_tail)*,
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };

    // ── typed (explicit name override) ──
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!() $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = placeholder!(), middleware = [] $(, backpressure = [$bp])?)
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr) $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = placeholder!($msg), middleware = [] $(, backpressure = [$bp])?)
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!(), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = placeholder!(), middleware = [$($mw),*] $(, backpressure = [$bp])?)
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = placeholder!($msg), middleware = [$($mw),*] $(, backpressure = [$bp])?)
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = $handler_head $(:: $handler_tail)*, middleware = [] $(, backpressure = [$bp])?)
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = $handler_head $(:: $handler_tail)*, middleware = [$($mw),*] $(, backpressure = [$bp])?)
    };
    (name: $name:literal, $out:ty => placeholder!() $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_source_typed!(output = $out, name = $name, handler = placeholder!(), middleware = [] $(, backpressure = [$bp])?)
    };
    (name: $name:literal, $out:ty => placeholder!($msg:expr) $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_source_typed!(output = $out, name = $name, handler = placeholder!($msg), middleware = [] $(, backpressure = [$bp])?)
    };
    (name: $name:literal, $out:ty => placeholder!(), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_source_typed!(output = $out, name = $name, handler = placeholder!(), middleware = [$($mw),*] $(, backpressure = [$bp])?)
    };
    (name: $name:literal, $out:ty => placeholder!($msg:expr), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_source_typed!(output = $out, name = $name, handler = placeholder!($msg), middleware = [$($mw),*] $(, backpressure = [$bp])?)
    };
    (name: $name:literal, $out:ty => $handler_head:ident $(:: $handler_tail:ident)* $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_source_typed!(output = $out, name = $name, handler = $handler_head $(:: $handler_tail)*, middleware = [] $(, backpressure = [$bp])?)
    };
    (name: $name:literal, $out:ty => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_source_typed!(output = $out, name = $name, handler = $handler_head $(:: $handler_tail)*, middleware = [$($mw),*] $(, backpressure = [$bp])?)
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => $handler:expr $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "source!",
            "let source = MySource::new(...); events = source!(Event => source);"
        )
    };
    ($out:ty => $handler:expr $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "source!",
            "let source = MySource::new(...); events = source!(Event => source);"
        )
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => $handler:expr $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "source!",
            "let source = MySource::new(...); events = source!(name: \"events\", Event => source);"
        )
    };
    (name: $name:literal, $out:ty => $handler:expr $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
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
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = $handler:expr, middleware = [$($legacy:expr),*] $(, backpressure = [$($bp:expr)?])?) => {
        compile_error!("async_source! takes control middleware in a 'with [...]' clause on the feed it protects (FLOWIP-115s)")
    };
    (output = $out:ty, name = $name:literal, handler = $handler:expr, middleware = [$($legacy:expr),*] $(, backpressure = [$($bp:expr)?])?) => {
        compile_error!("async_source! takes control middleware in a 'with [...]' clause on the feed it protects (FLOWIP-115s)")
    };
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!(), source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        $crate::__obzenflow_admit_placeholder_source!(factory = placeholder_async_finite_source_descriptor, output = $out, output_contract = [$($member),+], name = $name, message = None, source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*] $(, backpressure = [$($bp)?])?)
    }};
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!($msg:expr), source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        $crate::__obzenflow_admit_placeholder_source!(factory = placeholder_async_finite_source_descriptor, output = $out, output_contract = [$($member),+], name = $name, message = Some($msg), source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*] $(, backpressure = [$($bp)?])?)
    }};
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = $handler:expr, source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        $crate::__obzenflow_admit_typed_source!(factory = typed_async_finite_source_descriptor, output = $out, output_contract = [$($member),+], name = $name, handler = $handler, source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*] $(, backpressure = [$($bp)?])?)
    }};
    (output = $out:ty, name = $name:literal, handler = placeholder!(), source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        $crate::__obzenflow_admit_placeholder_source!(factory = placeholder_async_finite_source_descriptor, output = $out, name = $name, message = None, source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*] $(, backpressure = [$($bp)?])?)
    }};
    (output = $out:ty, name = $name:literal, handler = placeholder!($msg:expr), source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        $crate::__obzenflow_admit_placeholder_source!(factory = placeholder_async_finite_source_descriptor, output = $out, name = $name, message = Some($msg), source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*] $(, backpressure = [$($bp)?])?)
    }};
    (output = $out:ty, name = $name:literal, handler = $handler:expr, source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        $crate::__obzenflow_admit_typed_source!(factory = typed_async_finite_source_descriptor, output = $out, name = $name, handler = $handler, source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*] $(, backpressure = [$($bp)?])?)
    }};
}

/// Create an async finite source stage descriptor.
#[macro_export]
macro_rules! async_source {
    ({ $($out:ty),+ $(,)? } => $handler:expr, middleware: [$($mw:expr),* $(,)?] $($rest:tt)*) => {
        $crate::__obzenflow_stage_middleware_removed!()
    };
    ($out:ty => $handler:expr, middleware: [$($mw:expr),* $(,)?] $($rest:tt)*) => {
        $crate::__obzenflow_stage_middleware_removed!()
    };
    (name: $name:literal, { $($out:ty),+ $(,)? } => $handler:expr, middleware: [$($mw:expr),* $(,)?] $($rest:tt)*) => {
        $crate::__obzenflow_stage_middleware_removed!()
    };
    (name: $name:literal, $out:ty => $handler:expr, middleware: [$($mw:expr),* $(,)?] $($rest:tt)*) => {
        $crate::__obzenflow_stage_middleware_removed!()
    };
    ({ $($out:ty),+ $(,)? } => $handler:expr, ingress with [$($policy:expr),* $(,)?] $($rest:tt)*) => {
        compile_error!("an ingress 'with' takes one policy expression in this release; write 'ingress with <policy>'; ordered ingress chains await FLOWIP-115t (FLOWIP-115s)")
    };
    ($out:ty => $handler:expr, ingress with [$($policy:expr),* $(,)?] $($rest:tt)*) => {
        compile_error!("an ingress 'with' takes one policy expression in this release; write 'ingress with <policy>'; ordered ingress chains await FLOWIP-115t (FLOWIP-115s)")
    };
    (name: $name:literal, { $($out:ty),+ $(,)? } => $handler:expr, ingress with [$($policy:expr),* $(,)?] $($rest:tt)*) => {
        compile_error!("an ingress 'with' takes one policy expression in this release; write 'ingress with <policy>'; ordered ingress chains await FLOWIP-115t (FLOWIP-115s)")
    };
    (name: $name:literal, $out:ty => $handler:expr, ingress with [$($policy:expr),* $(,)?] $($rest:tt)*) => {
        compile_error!("an ingress 'with' takes one policy expression in this release; write 'ingress with <policy>'; ordered ingress chains await FLOWIP-115t (FLOWIP-115s)")
    };
    ({ $($out:ty),+ $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* with [$($source_policy:expr),* $(,)?], ingress with [$($policy:expr),* $(,)?] $($rest:tt)*) => {
        compile_error!("an ingress 'with' takes one policy expression in this release; write 'ingress with <policy>'; ordered ingress chains await FLOWIP-115t (FLOWIP-115s)")
    };
    ($out:ty => $handler_head:ident $(:: $handler_tail:ident)* with [$($source_policy:expr),* $(,)?], ingress with [$($policy:expr),* $(,)?] $($rest:tt)*) => {
        compile_error!("an ingress 'with' takes one policy expression in this release; write 'ingress with <policy>'; ordered ingress chains await FLOWIP-115t (FLOWIP-115s)")
    };
    (name: $name:literal, { $($out:ty),+ $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* with [$($source_policy:expr),* $(,)?], ingress with [$($policy:expr),* $(,)?] $($rest:tt)*) => {
        compile_error!("an ingress 'with' takes one policy expression in this release; write 'ingress with <policy>'; ordered ingress chains await FLOWIP-115t (FLOWIP-115s)")
    };
    (name: $name:literal, $out:ty => $handler_head:ident $(:: $handler_tail:ident)* with [$($source_policy:expr),* $(,)?], ingress with [$($policy:expr),* $(,)?] $($rest:tt)*) => {
        compile_error!("an ingress 'with' takes one policy expression in this release; write 'ingress with <policy>'; ordered ingress chains await FLOWIP-115t (FLOWIP-115s)")
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!() $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_async_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = "__obzenflow_binding_derived_name__", handler = placeholder!(), source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?)
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr) $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_async_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = "__obzenflow_binding_derived_name__", handler = placeholder!($msg), source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?)
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_async_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = "__obzenflow_binding_derived_name__", handler = $handler_head $(:: $handler_tail)*, source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?)
    };
    ($out:ty => placeholder!() $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = placeholder!(), source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?)
    };
    ($out:ty => placeholder!($msg:expr) $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = placeholder!($msg), source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?)
    };
    ($out:ty => $handler_head:ident $(:: $handler_tail:ident)* $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = $handler_head $(:: $handler_tail)*, source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?)
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!() $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_async_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = placeholder!(), source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?)
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr) $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_async_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = placeholder!($msg), source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?)
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_async_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = $handler_head $(:: $handler_tail)*, source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?)
    };
    (name: $name:literal, $out:ty => placeholder!() $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = placeholder!(), source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?)
    };
    (name: $name:literal, $out:ty => placeholder!($msg:expr) $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = placeholder!($msg), source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?)
    };
    (name: $name:literal, $out:ty => $handler_head:ident $(:: $handler_tail:ident)* $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = $handler_head $(:: $handler_tail)*, source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?)
    };
    // ── typed (binding-derived name) ──
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!() $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = "__obzenflow_binding_derived_name__", handler = placeholder!(), middleware = [] $(, backpressure = [$bp])?)
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr) $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = "__obzenflow_binding_derived_name__", handler = placeholder!($msg), middleware = [] $(, backpressure = [$bp])?)
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!(), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = "__obzenflow_binding_derived_name__", handler = placeholder!(), middleware = [$($mw),*] $(, backpressure = [$bp])?)
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = "__obzenflow_binding_derived_name__", handler = placeholder!($msg), middleware = [$($mw),*] $(, backpressure = [$bp])?)
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = "__obzenflow_binding_derived_name__", handler = $handler_head $(:: $handler_tail)*, middleware = [] $(, backpressure = [$bp])?)
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = "__obzenflow_binding_derived_name__", handler = $handler_head $(:: $handler_tail)*, middleware = [$($mw),*] $(, backpressure = [$bp])?)
    };
    ($out:ty => placeholder!() $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = placeholder!(), middleware = [] $(, backpressure = [$bp])?)
    };
    ($out:ty => placeholder!($msg:expr) $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = placeholder!($msg), middleware = [] $(, backpressure = [$bp])?)
    };
    ($out:ty => placeholder!(), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = placeholder!(), middleware = [$($mw),*] $(, backpressure = [$bp])?)
    };
    ($out:ty => placeholder!($msg:expr), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = placeholder!($msg), middleware = [$($mw),*] $(, backpressure = [$bp])?)
    };
    ($out:ty => $handler_head:ident $(:: $handler_tail:ident)* $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = $handler_head $(:: $handler_tail)*, middleware = [] $(, backpressure = [$bp])?)
    };
    ($out:ty => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = $handler_head $(:: $handler_tail)*, middleware = [$($mw),*] $(, backpressure = [$bp])?)
    };

    // ── typed (explicit name override) ──
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!() $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = placeholder!(), middleware = [] $(, backpressure = [$bp])?)
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr) $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = placeholder!($msg), middleware = [] $(, backpressure = [$bp])?)
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!(), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = placeholder!(), middleware = [$($mw),*] $(, backpressure = [$bp])?)
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = placeholder!($msg), middleware = [$($mw),*] $(, backpressure = [$bp])?)
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = $handler_head $(:: $handler_tail)*, middleware = [] $(, backpressure = [$bp])?)
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = $handler_head $(:: $handler_tail)*, middleware = [$($mw),*] $(, backpressure = [$bp])?)
    };
    (name: $name:literal, $out:ty => placeholder!() $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = placeholder!(), middleware = [] $(, backpressure = [$bp])?)
    };
    (name: $name:literal, $out:ty => placeholder!($msg:expr) $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = placeholder!($msg), middleware = [] $(, backpressure = [$bp])?)
    };
    (name: $name:literal, $out:ty => placeholder!(), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = placeholder!(), middleware = [$($mw),*] $(, backpressure = [$bp])?)
    };
    (name: $name:literal, $out:ty => placeholder!($msg:expr), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = placeholder!($msg), middleware = [$($mw),*] $(, backpressure = [$bp])?)
    };
    (name: $name:literal, $out:ty => $handler_head:ident $(:: $handler_tail:ident)* $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = $handler_head $(:: $handler_tail)*, middleware = [] $(, backpressure = [$bp])?)
    };
    (name: $name:literal, $out:ty => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = $handler_head $(:: $handler_tail)*, middleware = [$($mw),*] $(, backpressure = [$bp])?)
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => ($handler:expr, $poll_timeout:expr) $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_timeout_diagnostic!(
            "async_source!",
            "let source = MySource::builder().poll_timeout(timeout).build()?; events = async_source!(Event => source);"
        )
    };
    ($out:ty => ($handler:expr, $poll_timeout:expr) $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_timeout_diagnostic!(
            "async_source!",
            "let source = MySource::builder().poll_timeout(timeout).build()?; events = async_source!(Event => source);"
        )
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => ($handler:expr, $poll_timeout:expr) $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_timeout_diagnostic!(
            "async_source!",
            "let source = MySource::builder().poll_timeout(timeout).build()?; events = async_source!(Event => source);"
        )
    };
    (name: $name:literal, $out:ty => ($handler:expr, $poll_timeout:expr) $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_timeout_diagnostic!(
            "async_source!",
            "let source = MySource::builder().poll_timeout(timeout).build()?; events = async_source!(Event => source);"
        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => $handler:expr $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "async_source!",
            "let source = MyAsyncSource::new(...); events = async_source!(Event => source);"
        )
    };
    ($out:ty => $handler:expr $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "async_source!",
            "let source = MyAsyncSource::new(...); events = async_source!(Event => source);"
        )
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => $handler:expr $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "async_source!",
            "let source = MyAsyncSource::new(...); events = async_source!(name: \"events\", Event => source);"
        )
    };
    (name: $name:literal, $out:ty => $handler:expr $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
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
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = $handler:expr, middleware = [$($legacy:expr),*] $(, backpressure = [$($bp:expr)?])?) => {
        compile_error!("infinite_source! takes control middleware in a 'with [...]' clause on the feed it protects (FLOWIP-115s)")
    };
    (output = $out:ty, name = $name:literal, handler = $handler:expr, middleware = [$($legacy:expr),*] $(, backpressure = [$($bp:expr)?])?) => {
        compile_error!("infinite_source! takes control middleware in a 'with [...]' clause on the feed it protects (FLOWIP-115s)")
    };
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!(), source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        $crate::__obzenflow_admit_placeholder_source!(factory = placeholder_infinite_source_descriptor, output = $out, output_contract = [$($member),+], name = $name, message = None, source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*] $(, backpressure = [$($bp)?])?)
    }};
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!($msg:expr), source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        $crate::__obzenflow_admit_placeholder_source!(factory = placeholder_infinite_source_descriptor, output = $out, output_contract = [$($member),+], name = $name, message = Some($msg), source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*] $(, backpressure = [$($bp)?])?)
    }};
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = $handler:expr, source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        $crate::__obzenflow_admit_typed_source!(factory = typed_infinite_source_descriptor, output = $out, output_contract = [$($member),+], name = $name, handler = $handler, source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*] $(, backpressure = [$($bp)?])?)
    }};
    (output = $out:ty, name = $name:literal, handler = placeholder!(), source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        $crate::__obzenflow_admit_placeholder_source!(factory = placeholder_infinite_source_descriptor, output = $out, name = $name, message = None, source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*] $(, backpressure = [$($bp)?])?)
    }};
    (output = $out:ty, name = $name:literal, handler = placeholder!($msg:expr), source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        $crate::__obzenflow_admit_placeholder_source!(factory = placeholder_infinite_source_descriptor, output = $out, name = $name, message = Some($msg), source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*] $(, backpressure = [$($bp)?])?)
    }};
    (output = $out:ty, name = $name:literal, handler = $handler:expr, source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        $crate::__obzenflow_admit_typed_source!(factory = typed_infinite_source_descriptor, output = $out, name = $name, handler = $handler, source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*] $(, backpressure = [$($bp)?])?)
    }};
}

/// Create an infinite source stage descriptor.
#[macro_export]
macro_rules! infinite_source {
    ({ $($out:ty),+ $(,)? } => $handler:expr, middleware: [$($mw:expr),* $(,)?] $($rest:tt)*) => {
        $crate::__obzenflow_stage_middleware_removed!()
    };
    ($out:ty => $handler:expr, middleware: [$($mw:expr),* $(,)?] $($rest:tt)*) => {
        $crate::__obzenflow_stage_middleware_removed!()
    };
    (name: $name:literal, { $($out:ty),+ $(,)? } => $handler:expr, middleware: [$($mw:expr),* $(,)?] $($rest:tt)*) => {
        $crate::__obzenflow_stage_middleware_removed!()
    };
    (name: $name:literal, $out:ty => $handler:expr, middleware: [$($mw:expr),* $(,)?] $($rest:tt)*) => {
        $crate::__obzenflow_stage_middleware_removed!()
    };
    ({ $($out:ty),+ $(,)? } => $handler:expr, ingress with [$($policy:expr),* $(,)?] $($rest:tt)*) => {
        compile_error!("an ingress 'with' takes one policy expression in this release; write 'ingress with <policy>'; ordered ingress chains await FLOWIP-115t (FLOWIP-115s)")
    };
    ($out:ty => $handler:expr, ingress with [$($policy:expr),* $(,)?] $($rest:tt)*) => {
        compile_error!("an ingress 'with' takes one policy expression in this release; write 'ingress with <policy>'; ordered ingress chains await FLOWIP-115t (FLOWIP-115s)")
    };
    (name: $name:literal, { $($out:ty),+ $(,)? } => $handler:expr, ingress with [$($policy:expr),* $(,)?] $($rest:tt)*) => {
        compile_error!("an ingress 'with' takes one policy expression in this release; write 'ingress with <policy>'; ordered ingress chains await FLOWIP-115t (FLOWIP-115s)")
    };
    (name: $name:literal, $out:ty => $handler:expr, ingress with [$($policy:expr),* $(,)?] $($rest:tt)*) => {
        compile_error!("an ingress 'with' takes one policy expression in this release; write 'ingress with <policy>'; ordered ingress chains await FLOWIP-115t (FLOWIP-115s)")
    };
    ({ $($out:ty),+ $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* with [$($source_policy:expr),* $(,)?], ingress with [$($policy:expr),* $(,)?] $($rest:tt)*) => {
        compile_error!("an ingress 'with' takes one policy expression in this release; write 'ingress with <policy>'; ordered ingress chains await FLOWIP-115t (FLOWIP-115s)")
    };
    ($out:ty => $handler_head:ident $(:: $handler_tail:ident)* with [$($source_policy:expr),* $(,)?], ingress with [$($policy:expr),* $(,)?] $($rest:tt)*) => {
        compile_error!("an ingress 'with' takes one policy expression in this release; write 'ingress with <policy>'; ordered ingress chains await FLOWIP-115t (FLOWIP-115s)")
    };
    (name: $name:literal, { $($out:ty),+ $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* with [$($source_policy:expr),* $(,)?], ingress with [$($policy:expr),* $(,)?] $($rest:tt)*) => {
        compile_error!("an ingress 'with' takes one policy expression in this release; write 'ingress with <policy>'; ordered ingress chains await FLOWIP-115t (FLOWIP-115s)")
    };
    (name: $name:literal, $out:ty => $handler_head:ident $(:: $handler_tail:ident)* with [$($source_policy:expr),* $(,)?], ingress with [$($policy:expr),* $(,)?] $($rest:tt)*) => {
        compile_error!("an ingress 'with' takes one policy expression in this release; write 'ingress with <policy>'; ordered ingress chains await FLOWIP-115t (FLOWIP-115s)")
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!() $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => { $crate::__obzenflow_infinite_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = "__obzenflow_binding_derived_name__", handler = placeholder!(), source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?) };
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr) $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => { $crate::__obzenflow_infinite_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = "__obzenflow_binding_derived_name__", handler = placeholder!($msg), source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?) };
    ({ $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => { $crate::__obzenflow_infinite_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = "__obzenflow_binding_derived_name__", handler = $handler_head $(:: $handler_tail)*, source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?) };
    ($out:ty => placeholder!() $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => { $crate::__obzenflow_infinite_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = placeholder!(), source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?) };
    ($out:ty => placeholder!($msg:expr) $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => { $crate::__obzenflow_infinite_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = placeholder!($msg), source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?) };
    ($out:ty => $handler_head:ident $(:: $handler_tail:ident)* $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => { $crate::__obzenflow_infinite_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = $handler_head $(:: $handler_tail)*, source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?) };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!() $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => { $crate::__obzenflow_infinite_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = placeholder!(), source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?) };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr) $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => { $crate::__obzenflow_infinite_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = placeholder!($msg), source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?) };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => { $crate::__obzenflow_infinite_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = $handler_head $(:: $handler_tail)*, source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?) };
    (name: $name:literal, $out:ty => placeholder!() $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => { $crate::__obzenflow_infinite_source_typed!(output = $out, name = $name, handler = placeholder!(), source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?) };
    (name: $name:literal, $out:ty => placeholder!($msg:expr) $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => { $crate::__obzenflow_infinite_source_typed!(output = $out, name = $name, handler = placeholder!($msg), source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?) };
    (name: $name:literal, $out:ty => $handler_head:ident $(:: $handler_tail:ident)* $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => { $crate::__obzenflow_infinite_source_typed!(output = $out, name = $name, handler = $handler_head $(:: $handler_tail)*, source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?) };
    // ── typed (binding-derived name) ──
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!() $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!(),
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr) $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!($msg),
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!(), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!(),
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!($msg),
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = "__obzenflow_binding_derived_name__",
            handler = $handler_head $(:: $handler_tail)*,
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = "__obzenflow_binding_derived_name__",
            handler = $handler_head $(:: $handler_tail)*,
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    ($out:ty => placeholder!() $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!(),
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    ($out:ty => placeholder!($msg:expr) $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!($msg),
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    ($out:ty => placeholder!(), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!(),
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    ($out:ty => placeholder!($msg:expr), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!($msg),
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    ($out:ty => $handler_head:ident $(:: $handler_tail:ident)* $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = $handler_head $(:: $handler_tail)*,
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    ($out:ty => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = $handler_head $(:: $handler_tail)*,
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };

    // ── typed (explicit name override) ──
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!() $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!(),
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr) $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!($msg),
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!(), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!(),
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!($msg),
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (name: $name:literal, $out:ty => placeholder!() $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = placeholder!(),
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (name: $name:literal, $out:ty => placeholder!($msg:expr) $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = placeholder!($msg),
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (name: $name:literal, $out:ty => placeholder!(), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = placeholder!(),
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (name: $name:literal, $out:ty => placeholder!($msg:expr), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = placeholder!($msg),
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (name: $name:literal, $out:ty => $handler_head:ident $(:: $handler_tail:ident)* $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (name: $name:literal, $out:ty => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => $handler:expr $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "infinite_source!",
            "let source = MyInfiniteSource::new(...); events = infinite_source!(Event => source);"
        )
    };
    ($out:ty => $handler:expr $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "infinite_source!",
            "let source = MyInfiniteSource::new(...); events = infinite_source!(Event => source);"
        )
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => $handler:expr $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "infinite_source!",
            "let source = MyInfiniteSource::new(...); events = infinite_source!(name: \"events\", Event => source);"
        )
    };
    (name: $name:literal, $out:ty => $handler:expr $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
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
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = $handler:expr, middleware = [$($legacy:expr),*] $(, backpressure = [$($bp:expr)?])?) => {
        compile_error!("async_infinite_source! takes control middleware in a 'with [...]' clause on the feed it protects (FLOWIP-115s)")
    };
    (output = $out:ty, name = $name:literal, handler = $handler:expr, middleware = [$($legacy:expr),*] $(, backpressure = [$($bp:expr)?])?) => {
        compile_error!("async_infinite_source! takes control middleware in a 'with [...]' clause on the feed it protects (FLOWIP-115s)")
    };
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!(), source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        $crate::__obzenflow_admit_placeholder_source!(factory = placeholder_async_infinite_source_descriptor, output = $out, output_contract = [$($member),+], name = $name, message = None, source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*] $(, backpressure = [$($bp)?])?)
    }};
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!($msg:expr), source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        $crate::__obzenflow_admit_placeholder_source!(factory = placeholder_async_infinite_source_descriptor, output = $out, output_contract = [$($member),+], name = $name, message = Some($msg), source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*] $(, backpressure = [$($bp)?])?)
    }};
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = $handler:expr, source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        $crate::__obzenflow_admit_typed_source!(factory = typed_async_infinite_source_descriptor, output = $out, output_contract = [$($member),+], name = $name, handler = $handler, source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*] $(, backpressure = [$($bp)?])?)
    }};
    (output = $out:ty, name = $name:literal, handler = placeholder!(), source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        $crate::__obzenflow_admit_placeholder_source!(factory = placeholder_async_infinite_source_descriptor, output = $out, name = $name, message = None, source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*] $(, backpressure = [$($bp)?])?)
    }};
    (output = $out:ty, name = $name:literal, handler = placeholder!($msg:expr), source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        $crate::__obzenflow_admit_placeholder_source!(factory = placeholder_async_infinite_source_descriptor, output = $out, name = $name, message = Some($msg), source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*] $(, backpressure = [$($bp)?])?)
    }};
    (output = $out:ty, name = $name:literal, handler = $handler:expr, source_policies = [$($policy:expr),*], ingress_policy = [$($ingress:expr)?], observers = [$($observer:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        $crate::__obzenflow_admit_typed_source!(factory = typed_async_infinite_source_descriptor, output = $out, name = $name, handler = $handler, source_policies = [$($policy),*], ingress_policy = [$($ingress)?], observers = [$($observer),*] $(, backpressure = [$($bp)?])?)
    }};
}

/// Create an async infinite source stage descriptor.
#[macro_export]
macro_rules! async_infinite_source {
    ({ $($out:ty),+ $(,)? } => $handler:expr, middleware: [$($mw:expr),* $(,)?] $($rest:tt)*) => {
        $crate::__obzenflow_stage_middleware_removed!()
    };
    ($out:ty => $handler:expr, middleware: [$($mw:expr),* $(,)?] $($rest:tt)*) => {
        $crate::__obzenflow_stage_middleware_removed!()
    };
    (name: $name:literal, { $($out:ty),+ $(,)? } => $handler:expr, middleware: [$($mw:expr),* $(,)?] $($rest:tt)*) => {
        $crate::__obzenflow_stage_middleware_removed!()
    };
    (name: $name:literal, $out:ty => $handler:expr, middleware: [$($mw:expr),* $(,)?] $($rest:tt)*) => {
        $crate::__obzenflow_stage_middleware_removed!()
    };
    ({ $($out:ty),+ $(,)? } => $handler:expr, ingress with [$($policy:expr),* $(,)?] $($rest:tt)*) => {
        compile_error!("an ingress 'with' takes one policy expression in this release; write 'ingress with <policy>'; ordered ingress chains await FLOWIP-115t (FLOWIP-115s)")
    };
    ($out:ty => $handler:expr, ingress with [$($policy:expr),* $(,)?] $($rest:tt)*) => {
        compile_error!("an ingress 'with' takes one policy expression in this release; write 'ingress with <policy>'; ordered ingress chains await FLOWIP-115t (FLOWIP-115s)")
    };
    (name: $name:literal, { $($out:ty),+ $(,)? } => $handler:expr, ingress with [$($policy:expr),* $(,)?] $($rest:tt)*) => {
        compile_error!("an ingress 'with' takes one policy expression in this release; write 'ingress with <policy>'; ordered ingress chains await FLOWIP-115t (FLOWIP-115s)")
    };
    (name: $name:literal, $out:ty => $handler:expr, ingress with [$($policy:expr),* $(,)?] $($rest:tt)*) => {
        compile_error!("an ingress 'with' takes one policy expression in this release; write 'ingress with <policy>'; ordered ingress chains await FLOWIP-115t (FLOWIP-115s)")
    };
    ({ $($out:ty),+ $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* with [$($source_policy:expr),* $(,)?], ingress with [$($policy:expr),* $(,)?] $($rest:tt)*) => {
        compile_error!("an ingress 'with' takes one policy expression in this release; write 'ingress with <policy>'; ordered ingress chains await FLOWIP-115t (FLOWIP-115s)")
    };
    ($out:ty => $handler_head:ident $(:: $handler_tail:ident)* with [$($source_policy:expr),* $(,)?], ingress with [$($policy:expr),* $(,)?] $($rest:tt)*) => {
        compile_error!("an ingress 'with' takes one policy expression in this release; write 'ingress with <policy>'; ordered ingress chains await FLOWIP-115t (FLOWIP-115s)")
    };
    (name: $name:literal, { $($out:ty),+ $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* with [$($source_policy:expr),* $(,)?], ingress with [$($policy:expr),* $(,)?] $($rest:tt)*) => {
        compile_error!("an ingress 'with' takes one policy expression in this release; write 'ingress with <policy>'; ordered ingress chains await FLOWIP-115t (FLOWIP-115s)")
    };
    (name: $name:literal, $out:ty => $handler_head:ident $(:: $handler_tail:ident)* with [$($source_policy:expr),* $(,)?], ingress with [$($policy:expr),* $(,)?] $($rest:tt)*) => {
        compile_error!("an ingress 'with' takes one policy expression in this release; write 'ingress with <policy>'; ordered ingress chains await FLOWIP-115t (FLOWIP-115s)")
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!() $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => { $crate::__obzenflow_async_infinite_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = "__obzenflow_binding_derived_name__", handler = placeholder!(), source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?) };
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr) $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => { $crate::__obzenflow_async_infinite_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = "__obzenflow_binding_derived_name__", handler = placeholder!($msg), source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?) };
    ({ $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => { $crate::__obzenflow_async_infinite_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = "__obzenflow_binding_derived_name__", handler = $handler_head $(:: $handler_tail)*, source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?) };
    ($out:ty => placeholder!() $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => { $crate::__obzenflow_async_infinite_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = placeholder!(), source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?) };
    ($out:ty => placeholder!($msg:expr) $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => { $crate::__obzenflow_async_infinite_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = placeholder!($msg), source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?) };
    ($out:ty => $handler_head:ident $(:: $handler_tail:ident)* $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => { $crate::__obzenflow_async_infinite_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = $handler_head $(:: $handler_tail)*, source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?) };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!() $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => { $crate::__obzenflow_async_infinite_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = placeholder!(), source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?) };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr) $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => { $crate::__obzenflow_async_infinite_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = placeholder!($msg), source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?) };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => { $crate::__obzenflow_async_infinite_source_typed!(output = $first, output_contract = [$first $(, $member)*], name = $name, handler = $handler_head $(:: $handler_tail)*, source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?) };
    (name: $name:literal, $out:ty => placeholder!() $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => { $crate::__obzenflow_async_infinite_source_typed!(output = $out, name = $name, handler = placeholder!(), source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?) };
    (name: $name:literal, $out:ty => placeholder!($msg:expr) $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => { $crate::__obzenflow_async_infinite_source_typed!(output = $out, name = $name, handler = placeholder!($msg), source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?) };
    (name: $name:literal, $out:ty => $handler_head:ident $(:: $handler_tail:ident)* $(with [$($policy:expr),* $(,)?])? $(, ingress with $ingress:expr)? $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?) => { $crate::__obzenflow_async_infinite_source_typed!(output = $out, name = $name, handler = $handler_head $(:: $handler_tail)*, source_policies = [$($($policy),*)?], ingress_policy = [$($ingress)?], observers = [$($($observer),*)?] $(, backpressure = [$bp])?) };
    // ── typed (binding-derived name) ──
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!() $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!(),
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr) $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!($msg),
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!(), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!(),
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!($msg),
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = "__obzenflow_binding_derived_name__",
            handler = $handler_head $(:: $handler_tail)*,
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = "__obzenflow_binding_derived_name__",
            handler = $handler_head $(:: $handler_tail)*,
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    ($out:ty => placeholder!() $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!(),
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    ($out:ty => placeholder!($msg:expr) $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!($msg),
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    ($out:ty => placeholder!(), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!(),
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    ($out:ty => placeholder!($msg:expr), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!($msg),
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    ($out:ty => $handler_head:ident $(:: $handler_tail:ident)* $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = $handler_head $(:: $handler_tail)*,
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    ($out:ty => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = $handler_head $(:: $handler_tail)*,
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };

    // ── typed (explicit name override) ──
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!() $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!(),
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr) $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!($msg),
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!(), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!(),
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!($msg),
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (name: $name:literal, $out:ty => placeholder!() $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = placeholder!(),
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (name: $name:literal, $out:ty => placeholder!($msg:expr) $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = placeholder!($msg),
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (name: $name:literal, $out:ty => placeholder!(), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = placeholder!(),
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (name: $name:literal, $out:ty => placeholder!($msg:expr), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = placeholder!($msg),
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (name: $name:literal, $out:ty => $handler_head:ident $(:: $handler_tail:ident)* $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (name: $name:literal, $out:ty => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => ($handler:expr, $poll_timeout:expr) $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_timeout_diagnostic!(
            "async_infinite_source!",
            "let source = MySource::builder().poll_timeout(timeout).build()?; events = async_infinite_source!(Event => source);"
        )
    };
    ($out:ty => ($handler:expr, $poll_timeout:expr) $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_timeout_diagnostic!(
            "async_infinite_source!",
            "let source = MySource::builder().poll_timeout(timeout).build()?; events = async_infinite_source!(Event => source);"
        )
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => ($handler:expr, $poll_timeout:expr) $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_timeout_diagnostic!(
            "async_infinite_source!",
            "let source = MySource::builder().poll_timeout(timeout).build()?; events = async_infinite_source!(Event => source);"
        )
    };
    (name: $name:literal, $out:ty => ($handler:expr, $poll_timeout:expr) $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_timeout_diagnostic!(
            "async_infinite_source!",
            "let source = MySource::builder().poll_timeout(timeout).build()?; events = async_infinite_source!(Event => source);"
        )
    };
    ({ $first:ty $(, $member:ty)* $(,)? } => $handler:expr $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "async_infinite_source!",
            "let source = MyAsyncInfiniteSource::new(...); events = async_infinite_source!(Event => source);"
        )
    };
    ($out:ty => $handler:expr $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "async_infinite_source!",
            "let source = MyAsyncInfiniteSource::new(...); events = async_infinite_source!(Event => source);"
        )
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => $handler:expr $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "async_infinite_source!",
            "let source = MyAsyncInfiniteSource::new(...); events = async_infinite_source!(name: \"events\", Event => source);"
        )
    };
    (name: $name:literal, $out:ty => $handler:expr $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
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
    (input = exact($in:ty), output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!(), middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        const _: () = ::obzenflow_core::assert_distinct_stage_fact_set::<
            ::obzenflow_core::stage_fact_set![$($member),+],
        >();
        let __observers: Vec<Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory>> =
            vec![$(Box::new($mw)),*];
        #[allow(unused_mut)]
        let mut __backpressure: Option<$crate::dsl::backpressure_clause::BackpressureClause> = None;
        $($( __backpressure = Some($bp); )?)?
        $crate::dsl::typing::placeholder_transform_descriptor::<
            $in,
            $out,
            ::obzenflow_core::stage_fact_set![$($member),+],
            _,
        >($name, None, __observers, __backpressure)
    }};
    (input = exact($in:ty), output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!($msg:expr), middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        const _: () = ::obzenflow_core::assert_distinct_stage_fact_set::<
            ::obzenflow_core::stage_fact_set![$($member),+],
        >();
        let __observers: Vec<Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory>> =
            vec![$(Box::new($mw)),*];
        #[allow(unused_mut)]
        let mut __backpressure: Option<$crate::dsl::backpressure_clause::BackpressureClause> = None;
        $($( __backpressure = Some($bp); )?)?
        $crate::dsl::typing::placeholder_transform_descriptor::<
            $in,
            $out,
            ::obzenflow_core::stage_fact_set![$($member),+],
            _,
        >($name, Some($msg), __observers, __backpressure)
    }};
    // -- exact input, real handler, explicit output contract --
    // FLOWIP-120b Option B keeps the flat output contract in the arrow's
    // type-signature position; the handler-side carrier is never named in the
    // flow.
    (input = exact($in:ty), output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __handler = $handler;
        const _: () = ::obzenflow_core::assert_distinct_stage_fact_set::<
            ::obzenflow_core::stage_fact_set![$($member),+],
        >();
        let __observers: Vec<Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory>> =
            vec![$(Box::new($mw)),*];
        #[allow(unused_mut)]
        let mut __backpressure: Option<$crate::dsl::backpressure_clause::BackpressureClause> = None;
        $($( __backpressure = Some($bp); )?)?
        $crate::dsl::typing::typed_transform_descriptor::<
            _,
            $in,
            $out,
            ::obzenflow_core::stage_fact_set![$($member),+],
            _,
            _,
            _,
        >($name, __handler, __observers, __backpressure)
    }};
    // ── exact input, placeholder ──
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = placeholder!(), middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __observers: Vec<Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory>> =
            vec![$(Box::new($mw)),*];
        #[allow(unused_mut)]
        let mut __backpressure: Option<$crate::dsl::backpressure_clause::BackpressureClause> = None;
        $($( __backpressure = Some($bp); )?)?
        $crate::dsl::typing::placeholder_transform_descriptor::<$in, $out, $out, _>(
            $name,
            None,
            __observers,
            __backpressure,
        )
    }};
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = placeholder!($msg:expr), middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __observers: Vec<Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory>> =
            vec![$(Box::new($mw)),*];
        #[allow(unused_mut)]
        let mut __backpressure: Option<$crate::dsl::backpressure_clause::BackpressureClause> = None;
        $($( __backpressure = Some($bp); )?)?
        $crate::dsl::typing::placeholder_transform_descriptor::<$in, $out, $out, _>(
            $name,
            Some($msg),
            __observers,
            __backpressure,
        )
    }};
    // ── exact input, real handler ──
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __handler = $handler;
        let __observers: Vec<Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory>> =
            vec![$(Box::new($mw)),*];
        #[allow(unused_mut)]
        let mut __backpressure: Option<$crate::dsl::backpressure_clause::BackpressureClause> = None;
        $($( __backpressure = Some($bp); )?)?
        $crate::dsl::typing::typed_transform_descriptor::<
            _,
            $in,
            $out,
            $out,
            _,
            _,
            _,
        >($name, __handler, __observers, __backpressure)
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_transform_exact_contract {
    (name = $name:literal, $($rest:tt)+) => {
        $crate::__obzenflow_transform_exact_contract!(@collect name = $name, in = (), $($rest)+)
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $($out:ty),+ $(,)? } => $handler:expr, middleware: [$($mw:expr),* $(,)?] $($rest:tt)*) => {
        $crate::__obzenflow_stage_middleware_removed!()
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty, outputs: [$($member:ty),+ $(,)?] => $handler:expr, middleware: [$($mw:expr),* $(,)?] $($rest:tt)*) => {
        $crate::__obzenflow_stage_middleware_removed!()
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler:expr, middleware: [$($mw:expr),* $(,)?] $($rest:tt)*) => {
        $crate::__obzenflow_stage_middleware_removed!()
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!() $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!(),
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr) $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!($msg),
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!(), observers: [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!(),
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr), observers: [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!($msg),
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*, observers: [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty, outputs: [$($member:ty),+ $(,)?] => placeholder!() $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $out,
            output_contract = [$($member),+],
            name = $name,
            handler = placeholder!(),
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty, outputs: [$($member:ty),+ $(,)?] => placeholder!($msg:expr) $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $out,
            output_contract = [$($member),+],
            name = $name,
            handler = placeholder!($msg),
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty, outputs: [$($member:ty),+ $(,)?] => placeholder!(), observers: [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $out,
            output_contract = [$($member),+],
            name = $name,
            handler = placeholder!(),
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty, outputs: [$($member:ty),+ $(,)?] => placeholder!($msg:expr), observers: [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $out,
            output_contract = [$($member),+],
            name = $name,
            handler = placeholder!($msg),
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty, outputs: [$($member:ty),+ $(,)?] => $handler_head:ident $(:: $handler_tail:ident)* $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $out,
            output_contract = [$($member),+],
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty, outputs: [$($member:ty),+ $(,)?] => $handler_head:ident $(:: $handler_tail:ident)*, observers: [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $out,
            output_contract = [$($member),+],
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!() $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!(),
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!($msg:expr) $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!($msg),
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!(), observers: [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!(),
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!($msg:expr), observers: [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!($msg),
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler_head:ident $(:: $handler_tail:ident)* $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler_head:ident $(:: $handler_tail:ident)*, observers: [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => $handler:expr $(, observers: [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "transform!",
            "let handler = MyTransform::new(...); output = transform!(Input -> Output => handler);"
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty, outputs: [$($member:ty),+ $(,)?] => $handler:expr $(, observers: [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "transform!",
            "let handler = MyTransform::new(...); output = transform!(Input -> Output => handler);"
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler:expr $(, observers: [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "transform!",
            "let handler = MyTransform::new(...); output = transform!(Input -> Output => handler);"
        )
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
macro_rules! transform {
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
// effectful_transform!
// ============================================================================

/// Entry parser for a `uses` clause with inline per-effect policy
/// attachments: `Effect with policy`. Each effect position accepts exactly
/// one aggregate policy expression.
///
/// Entry type tokens accumulate one token at a time until `with` or `,`;
/// wrap a generic effect type containing top-level commas in parentheses.
#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_effect_entries {
    // Generated AI surfaces accept one constrained row, but lower it through
    // the same declaration and attachment rules as ordinary effectful stages.
    (@generated_chat surface = $surface:tt, row = {
        at_least_once(ChatCompletion)
            via $binding:ident
            with { $($policy:tt)* }
        $(,)?
    }) => {
        compile_error!("an effect's 'with' takes one policy expression; write 'with <policy>'; braced policy sets await FLOWIP-132b (FLOWIP-115s)")
    };
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
            with [$($policy:expr),* $(,)?]
        $(,)?
    }) => {
        compile_error!(
            "an effect's 'with' takes one policy expression; write 'with <policy>'; braced policy sets await FLOWIP-132b (FLOWIP-115s)"
        )
    };
    (@generated_chat surface = $surface:tt, row = {
        at_least_once(ChatCompletion)
            via $binding:ident
            with $policy:expr
        $(,)?
    }) => {{
        let __chat_binding: ::obzenflow_runtime::effects::EffectBinding<
            ::obzenflow_adapters::ai::ChatCompletion,
        > = $crate::__obzenflow_clone_ai_chat_contract!($surface, $binding);
        let mut __chat_declarations: Vec<
            ::obzenflow_runtime::effects::EffectDeclaration,
        > = Vec::new();
        let mut __chat_policy_attachments: Vec<
            $crate::dsl::stage_descriptor::EffectPolicyAttachment,
        > = Vec::new();
        $crate::__obzenflow_effect_entries!(
            @entry __chat_declarations,
            __chat_policy_attachments,
            [],
            at_least_once(::obzenflow_adapters::ai::ChatCompletion)
                via __chat_binding
                with $policy
        );
        $crate::dsl::ai_effect::GeneratedChatEffectRow {
            binding: __chat_binding,
            declarations: __chat_declarations,
            policy_attachments: __chat_policy_attachments,
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
             with <EffectResilience>`"
        ))
    };

    // Preserve row diagnostics before the non-path role diagnostic without
    // maintaining a second generated-row parser.
    (@generated_chat_then surface = $surface:tt, row = {
        at_least_once(ChatCompletion)
            via $binding:ident
            with { $($policy:tt)* }
        $(,)?
    }, then = { $($then:tt)* }) => {
        $crate::__obzenflow_effect_entries!(
            @generated_chat surface = $surface,
            row = { at_least_once(ChatCompletion) via $binding with { $($policy)* } }
        )
    };
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
            with [$($policy:expr),* $(,)?]
        $(,)?
    }, then = { $($then:tt)* }) => {
        $crate::__obzenflow_effect_entries!(
            @generated_chat surface = $surface,
            row = { at_least_once(ChatCompletion) via $binding with [$($policy),*] }
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

    // ── end of input ───────────────────────────────────────────────────
    (@entry $effects:ident, $atts:ident, [],) => {};
    (@entry $effects:ident, $atts:ident, [$($acc:tt)+],) => {
        $effects.push(::obzenflow_runtime::effects::declare_effect_without_binding::<$($acc)+>());
    };

    // ── paid non-idempotent acknowledgement entries ──────────────────
    (@entry $effects:ident, $atts:ident, [], at_least_once($effect:ty) via $binding:ident with $policy:expr, $($rest:tt)*) => {
        $effects.push(::obzenflow_runtime::effects::declare_named_at_least_once_effect::<$effect, _>(&$binding));
        $crate::__obzenflow_effect_entries!(@attach $atts, $effect, $policy);
        $crate::__obzenflow_effect_entries!(@entry $effects, $atts, [], $($rest)*);
    };
    (@entry $effects:ident, $atts:ident, [], at_least_once($effect:ty) via $binding:ident with $policy:expr) => {
        $effects.push(::obzenflow_runtime::effects::declare_named_at_least_once_effect::<$effect, _>(&$binding));
        $crate::__obzenflow_effect_entries!(@attach $atts, $effect, $policy);
    };
    (@entry $effects:ident, $atts:ident, [], at_least_once($effect:ty) via $binding:ident, $($rest:tt)*) => {
        $effects.push(::obzenflow_runtime::effects::declare_named_at_least_once_effect::<$effect, _>(&$binding));
        $crate::__obzenflow_effect_entries!(@entry $effects, $atts, [], $($rest)*);
    };
    (@entry $effects:ident, $atts:ident, [], at_least_once($effect:ty) via $binding:ident) => {
        $effects.push(::obzenflow_runtime::effects::declare_named_at_least_once_effect::<$effect, _>(&$binding));
    };
    (@entry $effects:ident, $atts:ident, [], at_least_once($effect:ty) with [$($policy:expr),* $(,)?] $($rest:tt)*) => {
        compile_error!("an effect's 'with' takes one policy expression; write 'with <policy>'; braced policy sets await FLOWIP-132b (FLOWIP-115s)")
    };
    (@entry $effects:ident, $atts:ident, [], at_least_once($effect:ty) with { $($policy:tt)* } $($rest:tt)*) => {
        compile_error!("an effect's 'with' takes one policy expression; write 'with <policy>'; braced policy sets await FLOWIP-132b (FLOWIP-115s)")
    };
    (@entry $effects:ident, $atts:ident, [], at_least_once($effect:ty) with $policy:expr, $($rest:tt)*) => {
        $effects.push(::obzenflow_runtime::effects::declare_at_least_once_without_binding::<$effect>());
        $crate::__obzenflow_effect_entries!(@attach $atts, $effect, $policy);
        $crate::__obzenflow_effect_entries!(@entry $effects, $atts, [], $($rest)*);
    };
    (@entry $effects:ident, $atts:ident, [], at_least_once($effect:ty) with $policy:expr) => {
        $effects.push(::obzenflow_runtime::effects::declare_at_least_once_without_binding::<$effect>());
        $crate::__obzenflow_effect_entries!(@attach $atts, $effect, $policy);
    };
    (@entry $effects:ident, $atts:ident, [], at_least_once($effect:ty), $($rest:tt)*) => {
        $effects.push(::obzenflow_runtime::effects::declare_at_least_once_without_binding::<$effect>());
        $crate::__obzenflow_effect_entries!(@entry $effects, $atts, [], $($rest)*);
    };
    (@entry $effects:ident, $atts:ident, [], at_least_once($effect:ty)) => {
        $effects.push(::obzenflow_runtime::effects::declare_at_least_once_without_binding::<$effect>());
    };

    // ── transactional entries (recognized at entry start) ─────────────
    (@entry $effects:ident, $atts:ident, [], transactional($effect:ty) via $binding:ident with [$($policy:expr),* $(,)?] $($rest:tt)*) => {
        compile_error!("an effect's 'with' takes one policy expression; write 'with <policy>'; braced policy sets await FLOWIP-132b (FLOWIP-115s)")
    };
    (@entry $effects:ident, $atts:ident, [], transactional($effect:ty) via $binding:ident with { $($policy:tt)* } $($rest:tt)*) => {
        compile_error!("an effect's 'with' takes one policy expression; write 'with <policy>'; braced policy sets await FLOWIP-132b (FLOWIP-115s)")
    };
    (@entry $effects:ident, $atts:ident, [], transactional($effect:ty) via $binding:ident with $policy:expr, $($rest:tt)*) => {
        $effects.push(::obzenflow_runtime::effects::declare_transactional_effect::<$effect, _>(&$binding));
        $crate::__obzenflow_effect_entries!(@attach $atts, $effect, $policy);
        $crate::__obzenflow_effect_entries!(@entry $effects, $atts, [], $($rest)*);
    };
    (@entry $effects:ident, $atts:ident, [], transactional($effect:ty) via $binding:ident with $policy:expr) => {
        $effects.push(::obzenflow_runtime::effects::declare_transactional_effect::<$effect, _>(&$binding));
        $crate::__obzenflow_effect_entries!(@attach $atts, $effect, $policy);
    };
    (@entry $effects:ident, $atts:ident, [], transactional($effect:ty) via $binding:ident, $($rest:tt)*) => {
        $effects.push(::obzenflow_runtime::effects::declare_transactional_effect::<$effect, _>(&$binding));
        $crate::__obzenflow_effect_entries!(@entry $effects, $atts, [], $($rest)*);
    };
    (@entry $effects:ident, $atts:ident, [], transactional($effect:ty) via $binding:ident) => {
        $effects.push(::obzenflow_runtime::effects::declare_transactional_effect::<$effect, _>(&$binding));
    };
    (@entry $effects:ident, $atts:ident, [], transactional($effect:ty, $executor:expr) $($rest:tt)*) => {
        compile_error!("transactional executors are typed bindings; write `transactional(Effect) via binding`");
    };

    // ── named ordinary entries ─────────────────────────────────────────
    (@entry $effects:ident, $atts:ident, [$($acc:tt)+], via $binding:ident with $policy:expr, $($rest:tt)*) => {
        $effects.push(::obzenflow_runtime::effects::declare_named_effect::<$($acc)+, _>(&$binding));
        $crate::__obzenflow_effect_entries!(@attach $atts, $($acc)+, $policy);
        $crate::__obzenflow_effect_entries!(@entry $effects, $atts, [], $($rest)*);
    };
    (@entry $effects:ident, $atts:ident, [$($acc:tt)+], via $binding:ident with $policy:expr) => {
        $effects.push(::obzenflow_runtime::effects::declare_named_effect::<$($acc)+, _>(&$binding));
        $crate::__obzenflow_effect_entries!(@attach $atts, $($acc)+, $policy);
    };
    (@entry $effects:ident, $atts:ident, [$($acc:tt)+], via $binding:ident, $($rest:tt)*) => {
        $effects.push(::obzenflow_runtime::effects::declare_named_effect::<$($acc)+, _>(&$binding));
        $crate::__obzenflow_effect_entries!(@entry $effects, $atts, [], $($rest)*);
    };
    (@entry $effects:ident, $atts:ident, [$($acc:tt)+], via $binding:ident) => {
        $effects.push(::obzenflow_runtime::effects::declare_named_effect::<$($acc)+, _>(&$binding));
    };

    // ── bare `with` attachment terminator ──────────────────────────────
    (@entry $effects:ident, $atts:ident, [$($acc:tt)+], with [$($policy:expr),* $(,)?] $($rest:tt)*) => {
        compile_error!("an effect's 'with' takes one policy expression; write 'with <policy>'; braced policy sets await FLOWIP-132b (FLOWIP-115s)")
    };
    (@entry $effects:ident, $atts:ident, [$($acc:tt)+], with { $($policy:tt)* } $($rest:tt)*) => {
        compile_error!("an effect's 'with' takes one policy expression; write 'with <policy>'; braced policy sets await FLOWIP-132b (FLOWIP-115s)")
    };
    (@entry $effects:ident, $atts:ident, [$($acc:tt)+], with $policy:expr, $($rest:tt)*) => {
        $effects.push(::obzenflow_runtime::effects::declare_effect_without_binding::<$($acc)+>());
        $crate::__obzenflow_effect_entries!(@attach $atts, $($acc)+, $policy);
        $crate::__obzenflow_effect_entries!(@entry $effects, $atts, [], $($rest)*);
    };
    (@entry $effects:ident, $atts:ident, [$($acc:tt)+], with $policy:expr) => {
        $effects.push(::obzenflow_runtime::effects::declare_effect_without_binding::<$($acc)+>());
        $crate::__obzenflow_effect_entries!(@attach $atts, $($acc)+, $policy);
    };

    // ── comma terminator ────────────────────────────────────────────────
    (@entry $effects:ident, $atts:ident, [$($acc:tt)+], , $($rest:tt)*) => {
        $effects.push(::obzenflow_runtime::effects::declare_effect_without_binding::<$($acc)+>());
        $crate::__obzenflow_effect_entries!(@entry $effects, $atts, [], $($rest)*);
    };

    // ── accumulate one type token ───────────────────────────────────────
    (@entry $effects:ident, $atts:ident, [$($acc:tt)*], $next:tt $($rest:tt)*) => {
        $crate::__obzenflow_effect_entries!(@entry $effects, $atts, [$($acc)* $next], $($rest)*);
    };

    // ── attachment construction ─────────────────────────────────────────
    (@attach $atts:ident, $effect:ty, $policy:expr) => {{
        let __effect_type: &'static str =
            <$effect as ::obzenflow_runtime::effects::Effect>::EFFECT_TYPE;
        let __factory: Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory> = $policy;
        $atts.push($crate::dsl::stage_descriptor::EffectPolicyAttachment {
            effect_type: __effect_type,
            factory: __factory,
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

/// Early teaching diagnostic for the common identifier-only `uses` clause. The
/// general const guard still compares stable `EFFECT_TYPE`s for qualified and
/// generic types; this gate prevents an exact duplicate from leaking the
/// membership-proof implementation before that guard can speak.
#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_effect_duplicate_gate {
    (effects = [$($effect:ident),+ $(,)?], then = [$($then:tt)*]) => {
        $crate::__obzenflow_effect_duplicate_gate!(
            @next seen = [], remaining = [$($effect),+], then = [$($then)*]
        )
    };
    (effects = [$($effects:tt)*], then = [$($then:tt)*]) => {
        $($then)*
    };
    (@next seen = [$($seen:ident),*], remaining = [], then = [$($then:tt)*]) => {
        $($then)*
    };
    (@next seen = [$($seen:ident),*], remaining = [$candidate:ident $(, $rest:ident)*], then = [$($then:tt)*]) => {
        $crate::__obzenflow_effect_duplicate_gate!(
            @search
            all = [$($seen),*],
            search = [$($seen),*],
            candidate = $candidate,
            remaining = [$($rest),*],
            then = [$($then)*]
        )
    };
    (@search all = [$($all:ident),*], search = [], candidate = $candidate:ident, remaining = [$($rest:ident),*], then = [$($then:tt)*]) => {
        $crate::__obzenflow_effect_duplicate_gate!(
            @next
            seen = [$($all,)* $candidate],
            remaining = [$($rest),*],
            then = [$($then)*]
        )
    };
    (@search all = [$($all:ident),*], search = [$head:ident $(, $tail:ident)*], candidate = $candidate:ident, remaining = [$($rest:ident),*], then = [$($then:tt)*]) => {{
        macro_rules! __obzenflow_compare_effect_identifier {
            ($head) => {
                compile_error!(concat!(
                    "effect declaration `",
                    stringify!($candidate),
                    "` appears more than once; each effect type may occur only once"
                ))
            };
            ($_other:ident) => {
                $crate::__obzenflow_effect_duplicate_gate!(
                    @search
                    all = [$($all),*],
                    search = [$($tail),*],
                    candidate = $candidate,
                    remaining = [$($rest),*],
                    then = [$($then)*]
                )
            };
        }
        __obzenflow_compare_effect_identifier!($candidate)
    }};
}

/// Fail before handler/type-contract expansion when an ordinary `uses` clause
/// uses a delimiter reserved by FLOWIP-115s. Keeping this as a token scanner
/// lets qualified and generic effect types pass through without reconstructing
/// a Rust type grammar in the public macro.
#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_effect_policy_syntax_gate {
    (effects = [$($effects:tt)*], then = [$($then:tt)*]) => {
        $crate::__obzenflow_effect_policy_syntax_gate!(
            @scan then = [
                $crate::__obzenflow_effect_duplicate_gate!(
                    effects = [$($effects)*],
                    then = [$($then)*]
                )
            ],
            $($effects)*
        )
    };
    (@scan then = [$($then:tt)*],) => {
        $($then)*
    };
    (@scan then = [$($then:tt)*], with [$($policy:tt)*] $($rest:tt)*) => {
        compile_error!("an effect's 'with' takes one policy expression; write 'with <policy>'; braced policy sets await FLOWIP-132b (FLOWIP-115s)")
    };
    (@scan then = [$($then:tt)*], with { $($policy:tt)* } $($rest:tt)*) => {
        compile_error!("an effect's 'with' takes one policy expression; write 'with <policy>'; braced policy sets await FLOWIP-132b (FLOWIP-115s)")
    };
    (@scan then = [$($then:tt)*], $next:tt $($rest:tt)*) => {
        $crate::__obzenflow_effect_policy_syntax_gate!(
            @scan then = [$($then)*], $($rest)*
        )
    };
}

/// Type-only mirror of [`__obzenflow_effect_entries!`]. Policy expressions
/// and transactional executors are deliberately ignored; only concrete
/// effect request types enter the handler capability proof.
#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_effect_manifest_types {
    (@entry [$($types:ty,)*], [],) => {
        ::obzenflow_runtime::effect_set![$($types),*]
    };
    (@entry [$($types:ty,)*], [$($acc:tt)+],) => {
        ::obzenflow_runtime::effect_set![$($types,)* $($acc)+]
    };

    (@entry [$($types:ty,)*], [], at_least_once($effect:ty) via $binding:ident with $policy:expr, $($rest:tt)*) => {
        $crate::__obzenflow_effect_manifest_types!(@entry [$($types,)* $effect,], [], $($rest)*)
    };
    (@entry [$($types:ty,)*], [], at_least_once($effect:ty) via $binding:ident with $policy:expr) => {
        ::obzenflow_runtime::effect_set![$($types,)* $effect]
    };
    (@entry [$($types:ty,)*], [], at_least_once($effect:ty) via $binding:ident, $($rest:tt)*) => {
        $crate::__obzenflow_effect_manifest_types!(@entry [$($types,)* $effect,], [], $($rest)*)
    };
    (@entry [$($types:ty,)*], [], at_least_once($effect:ty) via $binding:ident) => {
        ::obzenflow_runtime::effect_set![$($types,)* $effect]
    };

    (@entry [$($types:ty,)*], [], at_least_once($effect:ty) with $policy:expr, $($rest:tt)*) => {
        $crate::__obzenflow_effect_manifest_types!(@entry [$($types,)* $effect,], [], $($rest)*)
    };
    (@entry [$($types:ty,)*], [], at_least_once($effect:ty) with $policy:expr) => {
        ::obzenflow_runtime::effect_set![$($types,)* $effect]
    };
    (@entry [$($types:ty,)*], [], at_least_once($effect:ty), $($rest:tt)*) => {
        $crate::__obzenflow_effect_manifest_types!(@entry [$($types,)* $effect,], [], $($rest)*)
    };
    (@entry [$($types:ty,)*], [], at_least_once($effect:ty)) => {
        ::obzenflow_runtime::effect_set![$($types,)* $effect]
    };

    (@entry [$($types:ty,)*], [], transactional($effect:ty) via $binding:ident with $policy:expr, $($rest:tt)*) => {
        $crate::__obzenflow_effect_manifest_types!(@entry [$($types,)* $effect,], [], $($rest)*)
    };
    (@entry [$($types:ty,)*], [], transactional($effect:ty) via $binding:ident with $policy:expr) => {
        ::obzenflow_runtime::effect_set![$($types,)* $effect]
    };
    (@entry [$($types:ty,)*], [], transactional($effect:ty) via $binding:ident, $($rest:tt)*) => {
        $crate::__obzenflow_effect_manifest_types!(@entry [$($types,)* $effect,], [], $($rest)*)
    };
    (@entry [$($types:ty,)*], [], transactional($effect:ty) via $binding:ident) => {
        ::obzenflow_runtime::effect_set![$($types,)* $effect]
    };
    // Keep the type-only proof coherent while the value parser emits the
    // single migration diagnostic for the retired string/expression executor
    // form. Without these arms, the invalid entry is reinterpreted as a type
    // and obscures the teaching error with unrelated trait failures.
    (@entry [$($types:ty,)*], [], transactional($effect:ty, $executor:expr), $($rest:tt)*) => {
        $crate::__obzenflow_effect_manifest_types!(@entry [$($types,)* $effect,], [], $($rest)*)
    };
    (@entry [$($types:ty,)*], [], transactional($effect:ty, $executor:expr)) => {
        ::obzenflow_runtime::effect_set![$($types,)* $effect]
    };

    (@entry [$($types:ty,)*], [$($acc:tt)+], via $binding:ident with $policy:expr, $($rest:tt)*) => {
        $crate::__obzenflow_effect_manifest_types!(@entry [$($types,)* $($acc)+,], [], $($rest)*)
    };
    (@entry [$($types:ty,)*], [$($acc:tt)+], via $binding:ident with $policy:expr) => {
        ::obzenflow_runtime::effect_set![$($types,)* $($acc)+]
    };
    (@entry [$($types:ty,)*], [$($acc:tt)+], via $binding:ident, $($rest:tt)*) => {
        $crate::__obzenflow_effect_manifest_types!(@entry [$($types,)* $($acc)+,], [], $($rest)*)
    };
    (@entry [$($types:ty,)*], [$($acc:tt)+], via $binding:ident) => {
        ::obzenflow_runtime::effect_set![$($types,)* $($acc)+]
    };

    (@entry [$($types:ty,)*], [$($acc:tt)+], with $policy:expr, $($rest:tt)*) => {
        $crate::__obzenflow_effect_manifest_types!(@entry [$($types,)* $($acc)+,], [], $($rest)*)
    };
    (@entry [$($types:ty,)*], [$($acc:tt)+], with $policy:expr) => {
        ::obzenflow_runtime::effect_set![$($types,)* $($acc)+]
    };
    (@entry [$($types:ty,)*], [$($acc:tt)+], , $($rest:tt)*) => {
        $crate::__obzenflow_effect_manifest_types!(@entry [$($types,)* $($acc)+,], [], $($rest)*)
    };
    (@entry [$($types:ty,)*], [$($acc:tt)*], $next:tt $($rest:tt)*) => {
        $crate::__obzenflow_effect_manifest_types!(@entry [$($types,)*], [$($acc)* $next], $($rest)*)
    };
    ($($entries:tt)*) => {
        $crate::__obzenflow_effect_manifest_types!(@entry [], [], $($entries)*)
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_assert_effectful_transform_contract {
    ($handler:ident, $in:ty, [$($member:ty),+], [$($effects:tt)*]) => {{
        fn __obzenflow_assert_effectful_transform_contract<
            H,
            ArrowToHandlerProof,
            HandlerToArrowProof,
            ManifestToHandlerProof,
            HandlerToManifestProof,
        >(_: &H)
        where
            H: ::obzenflow_runtime::stages::EffectfulTransformHandler,
            <H as ::obzenflow_runtime::stages::EffectfulTransformHandler>::Input:
                $crate::dsl::typing::EffectfulTransformInputMatchesArrow<$in>,
            <::obzenflow_core::stage_fact_set![$($member),+] as ::obzenflow_core::StageFactSet>::Members:
                $crate::dsl::typing::ArrowOutputsAreDeclaredByHandler<
                    <<H as ::obzenflow_runtime::stages::EffectfulTransformHandler>::Output as ::obzenflow_core::StageFactSet>::Members,
                    ArrowToHandlerProof,
                >,
            <<H as ::obzenflow_runtime::stages::EffectfulTransformHandler>::Output as ::obzenflow_core::StageFactSet>::Members:
                $crate::dsl::typing::HandlerOutputsAreDeclaredByArrow<
                    <::obzenflow_core::stage_fact_set![$($member),+] as ::obzenflow_core::StageFactSet>::Members,
                    HandlerToArrowProof,
                >,
            <$crate::__obzenflow_effect_manifest_types!($($effects)*) as ::obzenflow_runtime::effects::EffectSet>::Members:
                $crate::dsl::typing::ManifestEffectsAreAllowedByHandler<
                    <<H as ::obzenflow_runtime::stages::EffectfulTransformHandler>::AllowedEffects as ::obzenflow_runtime::effects::EffectSet>::Members,
                    ManifestToHandlerProof,
                >,
            <<H as ::obzenflow_runtime::stages::EffectfulTransformHandler>::AllowedEffects as ::obzenflow_runtime::effects::EffectSet>::Members:
                $crate::dsl::typing::HandlerEffectsAreDeclaredByManifest<
                    <$crate::__obzenflow_effect_manifest_types!($($effects)*) as ::obzenflow_runtime::effects::EffectSet>::Members,
                    HandlerToManifestProof,
                >,
        {}

        __obzenflow_assert_effectful_transform_contract::<_, _, _, _, _>(&$handler);
        const _: () = ::obzenflow_core::assert_distinct_stage_fact_set::<
            ::obzenflow_core::stage_fact_set![$($member),+],
        >();
        const _: () = ::obzenflow_runtime::effects::assert_distinct_effect_set::<
            $crate::__obzenflow_effect_manifest_types!($($effects)*),
        >();
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_assert_effectful_stateful_contract {
    ($handler:ident, $in:ty, [$($member:ty),+], [$($effects:tt)*]) => {{
        fn __obzenflow_assert_effectful_stateful_contract<
            H,
            ArrowToHandlerProof,
            HandlerToArrowProof,
            ManifestToHandlerProof,
            HandlerToManifestProof,
        >(_: &H)
        where
            H: ::obzenflow_runtime::stages::EffectfulStatefulHandler,
            <H as ::obzenflow_runtime::stages::EffectfulStatefulHandler>::Input:
                $crate::dsl::typing::EffectfulStatefulInputMatchesArrow<$in>,
            <::obzenflow_core::stage_fact_set![$($member),+] as ::obzenflow_core::StageFactSet>::Members:
                $crate::dsl::typing::ArrowOutputsAreDeclaredByHandler<
                    <<H as ::obzenflow_runtime::stages::EffectfulStatefulHandler>::Output as ::obzenflow_core::StageFactSet>::Members,
                    ArrowToHandlerProof,
                >,
            <<H as ::obzenflow_runtime::stages::EffectfulStatefulHandler>::Output as ::obzenflow_core::StageFactSet>::Members:
                $crate::dsl::typing::HandlerOutputsAreDeclaredByArrow<
                    <::obzenflow_core::stage_fact_set![$($member),+] as ::obzenflow_core::StageFactSet>::Members,
                    HandlerToArrowProof,
                >,
            <$crate::__obzenflow_effect_manifest_types!($($effects)*) as ::obzenflow_runtime::effects::EffectSet>::Members:
                $crate::dsl::typing::ManifestEffectsAreAllowedByHandler<
                    <<H as ::obzenflow_runtime::stages::EffectfulStatefulHandler>::AllowedEffects as ::obzenflow_runtime::effects::EffectSet>::Members,
                    ManifestToHandlerProof,
                >,
            <<H as ::obzenflow_runtime::stages::EffectfulStatefulHandler>::AllowedEffects as ::obzenflow_runtime::effects::EffectSet>::Members:
                $crate::dsl::typing::HandlerEffectsAreDeclaredByManifest<
                    <$crate::__obzenflow_effect_manifest_types!($($effects)*) as ::obzenflow_runtime::effects::EffectSet>::Members,
                    HandlerToManifestProof,
                >,
        {}

        __obzenflow_assert_effectful_stateful_contract::<_, _, _, _, _>(&$handler);
        const _: () = ::obzenflow_core::assert_distinct_stage_fact_set::<
            ::obzenflow_core::stage_fact_set![$($member),+],
        >();
        const _: () = ::obzenflow_runtime::effects::assert_distinct_effect_set::<
            $crate::__obzenflow_effect_manifest_types!($($effects)*),
        >();
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_effectful_transform_untyped {
    (name = $name:literal, handler = $handler:expr, effects = [$($effects:tt)*], middleware = [$($mw:expr),* $(,)?] $(, backpressure = [$($bp:expr)?])?) => {{
        use $crate::dsl::stage_descriptor::{EffectfulTransformDescriptor, StageDescriptor};
        let __observers: Vec<Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory>> =
            vec![$(Box::new($mw)),*];
        let mut __obzenflow_effects: Vec<::obzenflow_runtime::effects::EffectDeclaration> =
            Vec::new();
        let mut __obzenflow_attachments: Vec<
            $crate::dsl::stage_descriptor::EffectPolicyAttachment,
        > = Vec::new();
        $crate::__obzenflow_effect_entries!(
            @entry __obzenflow_effects, __obzenflow_attachments, [], $($effects)*
        );
        Box::new(EffectfulTransformDescriptor::new(
            $name,
            $handler,
            __obzenflow_effects,
            __observers,
            __obzenflow_attachments,
            {
                #[allow(unused_mut)]
                let mut __bp: Option<$crate::dsl::backpressure_clause::BackpressureClause> = None;
                $($( __bp = Some($bp); )?)?
                __bp
            },
        )) as Box<dyn StageDescriptor>
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_effectful_transform_typed {
    (input = exact($in:ty), output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = $handler:expr, effects = [$($effects:tt)*], middleware = [$($mw:expr),* $(,)?] $(, backpressure = [$($bp:expr)?])?) => {{
        let __handler = $handler;
        $crate::__obzenflow_assert_effectful_transform_contract!(
            __handler, $in, [$($member),+], [$($effects)*]
        );
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::exact_payload::<$in>(),
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            false,
            None,
        )
        .with_additional_output_contract($crate::__obzenflow_output_contract_members!($($member),+));
        let __descriptor = $crate::__obzenflow_effectful_transform_untyped!(
            name = $name,
            handler = __handler,
            effects = [$($effects)*],
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = $handler:expr, effects = [$($effects:tt)*], middleware = [$($mw:expr),* $(,)?] $(, backpressure = [$($bp:expr)?])?) => {{
        let __handler = $handler;
        $crate::__obzenflow_assert_effectful_transform_contract!(
            __handler, $in, [$out], [$($effects)*]
        );
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::exact_payload::<$in>(),
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            false,
            None,
        );
        let __descriptor = $crate::__obzenflow_effectful_transform_untyped!(
            name = $name,
            handler = __handler,
            effects = [$($effects)*],
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_effectful_transform_row_contract {
    (name = $name:literal, input = [$($in:tt)+], effects = [], $($rest:tt)*) => {
        compile_error!("empty effect sets are not a purity marker; write `Input -> Output => handler`")
    };
    (name = $name:literal, input = [$($in:tt)+], effects = [$($effects:tt)+], output = { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*, observers: [$($observer:expr),* $(,)?] $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_effect_policy_syntax_gate!(effects = [$($effects)+], then = [
            $crate::__obzenflow_effectful_transform_typed!(
                input = exact($($in)+),
                output = $first,
                output_contract = [$first $(, $member)*],
                name = $name,
                handler = $handler_head $(:: $handler_tail)*,
                effects = [$($effects)+],
                middleware = [$($observer),*]
                $(, backpressure = [$bp])?
            )
        ])
    };
    (name = $name:literal, input = [$($in:tt)+], effects = [$($effects:tt)+], output = { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_effectful_transform_row_contract!(
            name = $name,
            input = [$($in)+],
            effects = [$($effects)+],
            output = { $first $(, $member)* } => $handler_head $(:: $handler_tail)*,
            observers: []
            $(, backpressure: $bp)?
        )
    };
    (name = $name:literal, input = [$($in:tt)+], effects = [$($effects:tt)+], output = $out:ty => $handler_head:ident $(:: $handler_tail:ident)*, observers: [$($observer:expr),* $(,)?] $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_effect_policy_syntax_gate!(effects = [$($effects)+], then = [
            $crate::__obzenflow_effectful_transform_typed!(
                input = exact($($in)+),
                output = $out,
                name = $name,
                handler = $handler_head $(:: $handler_tail)*,
                effects = [$($effects)+],
                middleware = [$($observer),*]
                $(, backpressure = [$bp])?
            )
        ])
    };
    (name = $name:literal, input = [$($in:tt)+], effects = [$($effects:tt)+], output = $out:ty => $handler_head:ident $(:: $handler_tail:ident)* $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_effectful_transform_row_contract!(
            name = $name,
            input = [$($in)+],
            effects = [$($effects)+],
            output = $out => $handler_head $(:: $handler_tail)*,
            observers: []
            $(, backpressure: $bp)?
        )
    };
    (name = $name:literal, input = [$($in:tt)+], effects = [$($effects:tt)+], output = $($rest:tt)*) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "effectful_transform!",
            "let handler = MyEffectfulTransform::new(...); output = effectful_transform!(Input -> Output uses Effect => handler, observers: [...]);"
        )
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_effectful_transform_exact_contract {
    (name = $name:literal, $($rest:tt)+) => {
        $crate::__obzenflow_effectful_transform_exact_contract!(@collect name = $name, in = (), $($rest)+)
    };
    // Retired arrow-embedded rows receive one teaching diagnostic rather than
    // falling through as a malformed output type.
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $($effects:tt)* } { $first:ty $(, $member:ty)* $(,)? } => $($rest:tt)+) => {
        compile_error!("effectful_transform!: arrow-embedded effect rows were removed; write `Input -> Output uses Effect => handler`")
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $($effects:tt)* } $out:ty => $($rest:tt)+) => {
        compile_error!("effectful_transform!: arrow-embedded effect rows were removed; write `Input -> Output uses Effect => handler`")
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $($rest:tt)+) => {
        $crate::__obzenflow_effectful_transform_exact_contract!(
            @output
            name = $name,
            input = [$($in)+],
            output = [],
            $($rest)+
        )
    };

    // The complete output contract is collected before the capability
    // clause. This avoids placing an arbitrary identifier after a `ty`
    // fragment and lets scalar and braced output contracts share one parser.
    (@output name = $name:literal, input = [$($in:tt)+], output = [$($out:tt)+], uses { $($effects:tt)* } => $($rest:tt)+) => {
        $crate::__obzenflow_multi_effect_uses_gate!(effects = [$($effects)*], then = [
            $crate::__obzenflow_effectful_transform_row_contract!(
                name = $name,
                input = [$($in)+],
                effects = [$($effects)*],
                output = $($out)+ => $($rest)+
            )
        ])
    };
    (@output name = $name:literal, input = [$($in:tt)+], output = [$($out:tt)+], uses $($rest:tt)+) => {
        $crate::__obzenflow_effectful_transform_exact_contract!(
            @effect
            name = $name,
            input = [$($in)+],
            output = [$($out)+],
            effects = [],
            $($rest)+
        )
    };
    (@output name = $name:literal, input = [$($in:tt)+], output = [$($out:tt)+], => $($rest:tt)+) => {
        $crate::__obzenflow_effectful_transform_exact_contract!(
            @pure
            name = $name,
            input = [$($in)+],
            output = [$($out)+],
            rest = [$($rest)+]
        )
    };
    (@output name = $name:literal, input = [$($in:tt)+], output = [$($out:tt)*], $tok:tt $($rest:tt)+) => {
        $crate::__obzenflow_effectful_transform_exact_contract!(
            @output
            name = $name,
            input = [$($in)+],
            output = [$($out)* $tok],
            $($rest)+
        )
    };
    (@output name = $name:literal, input = [$($in:tt)+], output = [$($out:tt)*], $($rest:tt)*) => {
        compile_error!("effectful_transform!: expected `Input -> Output uses Effect => handler`")
    };

    // A bare `uses` clause collects exactly one entry up to the existing
    // signature-to-handler boundary.
    (@effect name = $name:literal, input = [$($in:tt)+], output = [$($out:tt)+], effects = [$($effects:tt)*], => $($rest:tt)+) => {
        $crate::__obzenflow_single_effect_uses_gate!(effects = [$($effects)*], then = [
            $crate::__obzenflow_effectful_transform_row_contract!(
                name = $name,
                input = [$($in)+],
                effects = [$($effects)*],
                output = $($out)+ => $($rest)+
            )
        ])
    };
    (@effect name = $name:literal, input = [$($in:tt)+], output = [$($out:tt)+], effects = [$($effects:tt)*], $tok:tt $($rest:tt)+) => {
        $crate::__obzenflow_effectful_transform_exact_contract!(
            @effect
            name = $name,
            input = [$($in)+],
            output = [$($out)+],
            effects = [$($effects)* $tok],
            $($rest)+
        )
    };
    (@effect name = $name:literal, input = [$($in:tt)+], output = [$($out:tt)+], effects = [$($effects:tt)*], $($rest:tt)*) => {
        compile_error!("effectful_transform!: expected `=> handler` after the `uses` clause")
    };

    // Effect-free `effectful_transform!` remains valid. These arms also keep
    // the existing path-only handler contract and curated diagnostic.
    (@pure name = $name:literal, input = [$($in:tt)+], output = [{ $first:ty $(, $member:ty)* $(,)? }], rest = [$handler:expr, effects: [$($effects:tt)*] $($rest:tt)*]) => {
        compile_error!("effectful_transform!: detached `effects: [...]` was removed; write `Input -> Output uses Effect => handler`")
    };
    (@pure name = $name:literal, input = [$($in:tt)+], output = [$out:ty], rest = [$handler:expr, effects: [$($effects:tt)*] $($rest:tt)*]) => {
        compile_error!("effectful_transform!: detached `effects: [...]` was removed; write `Input -> Output uses Effect => handler`")
    };
    (@pure name = $name:literal, input = [$($in:tt)+], output = [{ $first:ty $(, $member:ty)* $(,)? }], rest = [$handler_head:ident $(:: $handler_tail:ident)*, observers: [$($observer:expr),* $(,)?] $(, backpressure: $bp:expr)? $(,)?]) => {
        $crate::__obzenflow_effectful_transform_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            effects = [],
            middleware = [$($observer),*]
            $(, backpressure = [$bp])?
        )
    };
    (@pure name = $name:literal, input = [$($in:tt)+], output = [{ $first:ty $(, $member:ty)* $(,)? }], rest = [$handler_head:ident $(:: $handler_tail:ident)* $(, backpressure: $bp:expr)? $(,)?]) => {
        $crate::__obzenflow_effectful_transform_exact_contract!(
            @pure
            name = $name,
            input = [$($in)+],
            output = [{ $first $(, $member)* }],
            rest = [$handler_head $(:: $handler_tail)*, observers: [] $(, backpressure: $bp)?]
        )
    };
    (@pure name = $name:literal, input = [$($in:tt)+], output = [$out:ty], rest = [$handler_head:ident $(:: $handler_tail:ident)*, observers: [$($observer:expr),* $(,)?] $(, backpressure: $bp:expr)? $(,)?]) => {
        $crate::__obzenflow_effectful_transform_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            effects = [],
            middleware = [$($observer),*]
            $(, backpressure = [$bp])?
        )
    };
    (@pure name = $name:literal, input = [$($in:tt)+], output = [$out:ty], rest = [$handler_head:ident $(:: $handler_tail:ident)* $(, backpressure: $bp:expr)? $(,)?]) => {
        $crate::__obzenflow_effectful_transform_exact_contract!(
            @pure
            name = $name,
            input = [$($in)+],
            output = [$out],
            rest = [$handler_head $(:: $handler_tail)*, observers: [] $(, backpressure: $bp)?]
        )
    };
    (@pure name = $name:literal, input = [$($in:tt)+], output = [$($out:tt)+], rest = [$handler:expr $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?]) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "effectful_transform!",
            "let handler = MyEffectfulTransform::new(...); output = effectful_transform!(Input -> Output uses Effect => handler, observers: [...]);"
        )
    };
    (@collect name = $name:literal, in = (), -> $($rest:tt)*) => {
        compile_error!("effectful_transform!: expected an input type before `->`")
    };
    (@collect name = $name:literal, in = ($($in:tt)*), $tok:tt $($rest:tt)+) => {
        $crate::__obzenflow_effectful_transform_exact_contract!(
            @collect
            name = $name,
            in = ($($in)* $tok),
            $($rest)+
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), $($rest:tt)*) => {
        compile_error!("effectful_transform!: expected `-> OutputType => handler` after input type");
    };
}

#[macro_export]
macro_rules! effectful_transform {
    (name: $name:literal, $($rest:tt)+) => {
        $crate::__obzenflow_effectful_transform_exact_contract!(name = $name, $($rest)+)
    };
    ($($rest:tt)+) => {
        $crate::__obzenflow_effectful_transform_exact_contract!(
            name = "__obzenflow_binding_derived_name__",
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
            vec![$(Box::new($policy)),*],
            vec![$(Box::new($observer)),*],
        )
    }};
    (input = exact($in:ty), name = $name:literal, handler = placeholder!($msg:expr), sink_policies = [$($policy:expr),*], observers = [$($observer:expr),*]) => {{
        $crate::dsl::typing::placeholder_sink_descriptor::<$in>(
            $name,
            Some($msg),
            vec![$(Box::new($policy)),*],
            vec![$(Box::new($observer)),*],
        )
    }};

    // ── exact input, real handler (facade call anchoring) ──
    //
    // Like joins, sink facade helpers often need the contract type injected to avoid
    // turbofish/annotations at the call site (e.g., `sinks::json()` and `sinks::table(...)`).

    // ── exact input, real handler ──
    //
    (input = exact($in:ty), name = $name:literal, handler = $handler:expr, sink_policies = [$($policy:expr),*], observers = [$($observer:expr),*]) => {{
        let __handler = $handler;
        $crate::dsl::typing::typed_sink_descriptor::<_, $in>(
            $name,
            __handler,
            vec![$(Box::new($policy)),*],
            vec![$(Box::new($observer)),*],
        )
    }};
}

/// Lower the optional `delivery:` clause of `sink!` (FLOWIP-120n F16,
/// FLOWIP-120s). Routes through the sealed `SetSinkRedeliverySafety` trait so
/// the clause is accepted only for a `SinkConnector`, including the inline
/// tier, and wraps its description before the DSL snapshots it.
#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_sink_delivery {
    ($handler:expr) => {
        $handler
    };
    ($handler:expr, idempotent) => {
        ::obzenflow_runtime::stages::sink::SetSinkRedeliverySafety::safe_to_repeat($handler)
    };
    ($handler:expr, non_idempotent) => {
        ::obzenflow_runtime::stages::sink::SetSinkRedeliverySafety::duplicate_sensitive($handler)
    };
    ($handler:expr, $other:ident) => {
        compile_error!("sink!: `delivery:` accepts `idempotent` or `non_idempotent`")
    };
}

/// Render the closed literal set used by a config-selected `sink!` diagnostic.
#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_sink_selection_expected {
    ($only:literal) => {
        concat!("\"", $only, "\"")
    };
    ($first:literal, $second:literal) => {
        concat!("\"", $first, "\" or \"", $second, "\"")
    };
    ($first:literal, $second:literal, $($rest:literal),+) => {
        concat!(
            "one of \"",
            $first,
            "\", \"",
            $second,
            "\"",
            $(", \"", $rest, "\"")+
        )
    };
}

/// Create a sink stage descriptor.
///
/// Canonical grammar: `InputType => handler_path`, optional `with [...]`,
/// optional `delivery: idempotent | non_idempotent`, then optional
/// `observers: [ ... ]`. Construct closure-tier `SinkTyped`
/// adapters and sink facades in ordinary Rust inside the materialiser, then
/// pass the resulting binding by path.
///
/// The config-selected form is
/// `InputType => select(owned_string) { "key" => handler_constructor, ... }`.
/// It evaluates the selector once, constructs only the matching handler, and
/// checks every branch through the same exact input witness before erasure.
#[macro_export]
macro_rules! sink {
    // FLOWIP-010o B1: one owned, non-secret config key selects from a
    // compile-time-closed set. No selector or alternative set survives the
    // match into the ordinary sink descriptor.
    ($in:ty => select($selector:expr) {
        $($key:literal => $handler:expr),+ $(,)?
    }) => {{
        let __obzenflow_selected_key: ::std::string::String = $selector;
        #[deny(unreachable_patterns)]
        let __obzenflow_selected_sink: ::core::result::Result<
            _,
            $crate::dsl::FlowBuildError,
        > = match __obzenflow_selected_key.as_str() {
            $(
                $key => ::core::result::Result::Ok(
                    $crate::__obzenflow_sink_typed!(
                        input = exact($in),
                        name = "__obzenflow_binding_derived_name__",
                        handler = $handler,
                        sink_policies = [],
                        observers = []
                    )
                ),
            )+
            __obzenflow_invalid_key => ::core::result::Result::Err(
                $crate::dsl::FlowBuildError::InvalidSinkSelection {
                    selected: __obzenflow_invalid_key.to_owned(),
                    expected: $crate::__obzenflow_sink_selection_expected!($($key),+),
                }
            ),
        };
        __obzenflow_selected_sink
    }};
    ($in:ty => $handler:expr, middleware: [$($mw:expr),* $(,)?] $($rest:tt)*) => {
        $crate::__obzenflow_stage_middleware_removed!()
    };
    (name: $name:literal, $in:ty => $handler:expr, middleware: [$($mw:expr),* $(,)?] $($rest:tt)*) => {
        $crate::__obzenflow_stage_middleware_removed!()
    };
    // ── typed (binding-derived name): exact input ──
    ($in:ty => placeholder!() $(with [$($policy:expr),* $(,)?])? $(, observers: [$($observer:expr),* $(,)?])?) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = "__obzenflow_binding_derived_name__", handler = placeholder!(), sink_policies = [$($($policy),*)?], observers = [$($($observer),*)?])
    };
    ($in:ty => placeholder!($msg:expr) $(with [$($policy:expr),* $(,)?])? $(, observers: [$($observer:expr),* $(,)?])?) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = "__obzenflow_binding_derived_name__", handler = placeholder!($msg), sink_policies = [$($($policy),*)?], observers = [$($($observer),*)?])
    };
    (|$($closure:tt)*) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "sink!",
            "let output = SinkTyped::new(...); events = sink!(Event => output);"
        )
    };
    (move |$($closure:tt)*) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "sink!",
            "let output = SinkTyped::new(...); events = sink!(Event => output);"
        )
    };
    ($in:ty => |$($closure:tt)*) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "sink!",
            "let output = SinkTyped::new(...); events = sink!(Event => output);"
        )
    };
    ($in:ty => move |$($closure:tt)*) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "sink!",
            "let output = SinkTyped::new(...); events = sink!(Event => output);"
        )
    };
    ($in:ty => sinks::$factory:ident($($args:tt)*) $(, observers: [$($mw:expr),* $(,)?])?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "sink!",
            "let output = sinks::json::<Event>(); events = sink!(Event => output);"
        )
    };
    ($in:ty => $handler_head:ident $(:: $handler_tail:ident)*
        $(with [$($policy:expr),* $(,)?])?
        $(, delivery: $delivery:ident)?
        $(, observers: [$($observer:expr),* $(,)?])?
    ) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = "__obzenflow_binding_derived_name__", handler = $crate::__obzenflow_sink_delivery!($handler_head $(:: $handler_tail)* $(, $delivery)?), sink_policies = [$($($policy),*)?], observers = [$($($observer),*)?])
    };

    // ── typed (explicit name override): exact input ──
    (name: $name:literal, $in:ty => placeholder!() $(with [$($policy:expr),* $(,)?])? $(, observers: [$($observer:expr),* $(,)?])?) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = $name, handler = placeholder!(), sink_policies = [$($($policy),*)?], observers = [$($($observer),*)?])
    };
    (name: $name:literal, $in:ty => placeholder!($msg:expr) $(with [$($policy:expr),* $(,)?])? $(, observers: [$($observer:expr),* $(,)?])?) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = $name, handler = placeholder!($msg), sink_policies = [$($($policy),*)?], observers = [$($($observer),*)?])
    };
    (name: $name:literal, |$($closure:tt)*) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "sink!",
            "let output = SinkTyped::new(...); events = sink!(name: \"events\", Event => output);"
        )
    };
    (name: $name:literal, move |$($closure:tt)*) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "sink!",
            "let output = SinkTyped::new(...); events = sink!(name: \"events\", Event => output);"
        )
    };
    (name: $name:literal, $in:ty => |$($closure:tt)*) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "sink!",
            "let output = SinkTyped::new(...); events = sink!(name: \"events\", Event => output);"
        )
    };
    (name: $name:literal, $in:ty => move |$($closure:tt)*) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "sink!",
            "let output = SinkTyped::new(...); events = sink!(name: \"events\", Event => output);"
        )
    };
    (name: $name:literal, $in:ty => sinks::$factory:ident($($args:tt)*) $(, observers: [$($mw:expr),* $(,)?])?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "sink!",
            "let output = sinks::json::<Event>(); events = sink!(name: \"events\", Event => output);"
        )
    };
    (name: $name:literal, $in:ty => $handler_head:ident $(:: $handler_tail:ident)*
        $(with [$($policy:expr),* $(,)?])?
        $(, delivery: $delivery:ident)?
        $(, observers: [$($observer:expr),* $(,)?])?
    ) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = $name, handler = $crate::__obzenflow_sink_delivery!($handler_head $(:: $handler_tail)* $(, $delivery)?), sink_policies = [$($($policy),*)?], observers = [$($($observer),*)?])
    };

    // ── clause-order guardrails: `with`, then `delivery:`, then `observers:` ──
    ($in:ty => $handler:expr, observers: [$($mw:expr),* $(,)?], delivery: $delivery:ident) => {
        compile_error!("sink!: clause order is 'with [...]', then 'delivery:', then 'observers:'")
    };
    (name: $name:literal, $in:ty => $handler:expr, observers: [$($mw:expr),* $(,)?], delivery: $delivery:ident) => {
        compile_error!("sink!: clause order is 'with [...]', then 'delivery:', then 'observers:'")
    };
    ($in:ty => $handler:expr, delivery: idempotent $(, observers: [$($mw:expr),* $(,)?])?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "sink!",
            "let output = SinkTyped::new(...); events = sink!(Event => output);"
        )
    };
    ($in:ty => $handler:expr, delivery: non_idempotent $(, observers: [$($mw:expr),* $(,)?])?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "sink!",
            "let output = SinkTyped::new(...); events = sink!(Event => output);"
        )
    };
    ($in:ty => $handler:expr, delivery: $other:ident $(, observers: [$($mw:expr),* $(,)?])?) => {
        compile_error!("sink!: `delivery:` accepts `idempotent` or `non_idempotent`")
    };
    (name: $name:literal, $in:ty => $handler:expr, delivery: idempotent $(, observers: [$($mw:expr),* $(,)?])?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "sink!",
            "let output = SinkTyped::new(...); events = sink!(name: \"events\", Event => output);"
        )
    };
    (name: $name:literal, $in:ty => $handler:expr, delivery: non_idempotent $(, observers: [$($mw:expr),* $(,)?])?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "sink!",
            "let output = SinkTyped::new(...); events = sink!(name: \"events\", Event => output);"
        )
    };
    (name: $name:literal, $in:ty => $handler:expr, delivery: $other:ident $(, observers: [$($mw:expr),* $(,)?])?) => {
        compile_error!("sink!: `delivery:` accepts `idempotent` or `non_idempotent`")
    };
    ($in:ty => $handler:expr
        $(, delivery: $delivery:ident)?
        $(, observers: [$($mw:expr),* $(,)?])?
    ) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "sink!",
            "let output = SinkTyped::new(...); events = sink!(Event => output);"
        )
    };
    (name: $name:literal, $in:ty => $handler:expr
        $(, delivery: $delivery:ident)?
        $(, observers: [$($mw:expr),* $(,)?])?
    ) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "sink!",
            "let output = SinkTyped::new(...); events = sink!(name: \"events\", Event => output);"
        )
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
    (input = exact($in:ty), output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!(), emit = $emit:ident $(($interval:expr))?, middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        const _: () = ::obzenflow_core::assert_distinct_stage_fact_set::<
            ::obzenflow_core::stage_fact_set![$($member),+],
        >();
        let __observers: Vec<Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory>> =
            vec![$(Box::new($mw)),*];
        #[allow(unused_mut)]
        let mut __backpressure: Option<$crate::dsl::backpressure_clause::BackpressureClause> = None;
        $($( __backpressure = Some($bp); )?)?
        $crate::dsl::typing::placeholder_stateful_descriptor::<
            $in,
            $out,
            ::obzenflow_core::stage_fact_set![$($member),+],
            _,
        >(
            $name,
            None,
            $crate::__obzenflow_stateful_emit_option!($emit $(($interval))?),
            __observers,
            __backpressure,
        )
    }};
    (input = exact($in:ty), output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!($msg:expr), emit = $emit:ident $(($interval:expr))?, middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        const _: () = ::obzenflow_core::assert_distinct_stage_fact_set::<
            ::obzenflow_core::stage_fact_set![$($member),+],
        >();
        let __observers: Vec<Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory>> =
            vec![$(Box::new($mw)),*];
        #[allow(unused_mut)]
        let mut __backpressure: Option<$crate::dsl::backpressure_clause::BackpressureClause> = None;
        $($( __backpressure = Some($bp); )?)?
        $crate::dsl::typing::placeholder_stateful_descriptor::<
            $in,
            $out,
            ::obzenflow_core::stage_fact_set![$($member),+],
            _,
        >(
            $name,
            Some($msg),
            $crate::__obzenflow_stateful_emit_option!($emit $(($interval))?),
            __observers,
            __backpressure,
        )
    }};
    (input = exact($in:ty), output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = $handler:expr, emit = $emit:ident $(($interval:expr))?, middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __handler = $handler;
        const _: () = ::obzenflow_core::assert_distinct_stage_fact_set::<
            ::obzenflow_core::stage_fact_set![$($member),+],
        >();
        let __observers: Vec<Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory>> =
            vec![$(Box::new($mw)),*];
        #[allow(unused_mut)]
        let mut __backpressure: Option<$crate::dsl::backpressure_clause::BackpressureClause> = None;
        $($( __backpressure = Some($bp); )?)?
        $crate::dsl::typing::typed_stateful_descriptor::<
            _,
            $in,
            $out,
            ::obzenflow_core::stage_fact_set![$($member),+],
            _,
            _,
            _,
        >(
            $name,
            __handler,
            $crate::__obzenflow_stateful_emit_option!($emit $(($interval))?),
            __observers,
            __backpressure,
        )
    }};
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = placeholder!(), emit = $emit:ident $(($interval:expr))?, middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __observers: Vec<Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory>> =
            vec![$(Box::new($mw)),*];
        #[allow(unused_mut)]
        let mut __backpressure: Option<$crate::dsl::backpressure_clause::BackpressureClause> = None;
        $($( __backpressure = Some($bp); )?)?
        $crate::dsl::typing::placeholder_stateful_descriptor::<$in, $out, $out, _>(
            $name,
            None,
            $crate::__obzenflow_stateful_emit_option!($emit $(($interval))?),
            __observers,
            __backpressure,
        )
    }};
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = placeholder!($msg:expr), emit = $emit:ident $(($interval:expr))?, middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __observers: Vec<Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory>> =
            vec![$(Box::new($mw)),*];
        #[allow(unused_mut)]
        let mut __backpressure: Option<$crate::dsl::backpressure_clause::BackpressureClause> = None;
        $($( __backpressure = Some($bp); )?)?
        $crate::dsl::typing::placeholder_stateful_descriptor::<$in, $out, $out, _>(
            $name,
            Some($msg),
            $crate::__obzenflow_stateful_emit_option!($emit $(($interval))?),
            __observers,
            __backpressure,
        )
    }};
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = $handler:expr, emit = $emit:ident $(($interval:expr))?, middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __handler = $handler;
        let __observers: Vec<Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory>> =
            vec![$(Box::new($mw)),*];
        #[allow(unused_mut)]
        let mut __backpressure: Option<$crate::dsl::backpressure_clause::BackpressureClause> = None;
        $($( __backpressure = Some($bp); )?)?
        $crate::dsl::typing::typed_stateful_descriptor::<_, $in, $out, $out, _, _, _>(
            $name,
            __handler,
            $crate::__obzenflow_stateful_emit_option!($emit $(($interval))?),
            __observers,
            __backpressure,
        )
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_stateful_exact_contract {
    (name = $name:literal, $($rest:tt)+) => {
        $crate::__obzenflow_stateful_exact_contract!(@collect name = $name, in = (), $($rest)+)
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $($out:ty),+ $(,)? } => $handler:expr $(, emit_interval = $emit_interval:expr)?, middleware: [$($mw:expr),* $(,)?] $($rest:tt)*) => {
        $crate::__obzenflow_stage_middleware_removed!()
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler:expr $(, emit_interval = $emit_interval:expr)?, middleware: [$($mw:expr),* $(,)?] $($rest:tt)*) => {
        $crate::__obzenflow_stage_middleware_removed!()
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!() $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!(),
            emit = none,
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr) $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!($msg),
            emit = none,
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!(), observers: [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!(),
            emit = none,
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr), observers: [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!($msg),
            emit = none,
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!(), emit_interval = $emit_interval:expr $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!(),
            emit = some($emit_interval),
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr), emit_interval = $emit_interval:expr $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!($msg),
            emit = some($emit_interval),
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!(), emit_interval = $emit_interval:expr, observers: [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!(),
            emit = some($emit_interval),
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr), emit_interval = $emit_interval:expr, observers: [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!($msg),
            emit = some($emit_interval),
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            emit = none,
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*, observers: [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            emit = none,
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*, emit_interval = $emit_interval:expr $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            emit = some($emit_interval),
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*, emit_interval = $emit_interval:expr, observers: [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            emit = some($emit_interval),
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!() $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!(),
            emit = none,
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!($msg:expr) $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!($msg),
            emit = none,
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!(), observers: [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!(),
            emit = none,
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!($msg:expr), observers: [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!($msg),
            emit = none,
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!(), emit_interval = $emit_interval:expr $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!(),
            emit = some($emit_interval),
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!($msg:expr), emit_interval = $emit_interval:expr $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!($msg),
            emit = some($emit_interval),
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!(), emit_interval = $emit_interval:expr, observers: [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!(),
            emit = some($emit_interval),
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!($msg:expr), emit_interval = $emit_interval:expr, observers: [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!($msg),
            emit = some($emit_interval),
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler_head:ident $(:: $handler_tail:ident)* $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            emit = none,
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler_head:ident $(:: $handler_tail:ident)*, observers: [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            emit = none,
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler_head:ident $(:: $handler_tail:ident)*, emit_interval = $emit_interval:expr $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            emit = some($emit_interval),
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler_head:ident $(:: $handler_tail:ident)*, emit_interval = $emit_interval:expr, observers: [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            emit = some($emit_interval),
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => $handler:expr $(, emit_interval = $emit_interval:expr)? $(, observers: [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "stateful!",
            "let handler = MyStateful::new(...); output = stateful!(Input -> Output => handler);"
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler:expr $(, emit_interval = $emit_interval:expr)? $(, observers: [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "stateful!",
            "let handler = MyStateful::new(...); output = stateful!(Input -> Output => handler);"
        )
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
macro_rules! stateful {
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
macro_rules! __obzenflow_effectful_stateful_untyped {
    (name = $name:literal, handler = $handler:expr, effects = [$($effects:tt)*], middleware = [$($mw:expr),* $(,)?] $(, backpressure = [$($bp:expr)?])?) => {{
        use $crate::dsl::stage_descriptor::EffectfulStatefulDescriptor;
        let mut __obzenflow_effects: Vec<::obzenflow_runtime::effects::EffectDeclaration> =
            Vec::new();
        let mut __obzenflow_attachments: Vec<
            $crate::dsl::stage_descriptor::EffectPolicyAttachment,
        > = Vec::new();
        $crate::__obzenflow_effect_entries!(
            @entry __obzenflow_effects, __obzenflow_attachments, [], $($effects)*
        );
        let mut __desc = EffectfulStatefulDescriptor::new($name, $handler)
            .with_effect_row(__obzenflow_effects, __obzenflow_attachments)
            $(.with_observer($mw))*;
        {
            #[allow(unused_mut)]
            let mut __bp: Option<$crate::dsl::backpressure_clause::BackpressureClause> = None;
            $($( __bp = Some($bp); )?)?
            __desc.backpressure = __bp;
        }
        __desc.build()
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_effectful_stateful_row_contract {
    (name = $name:literal, input = [$($in:tt)+], effects = [], $($rest:tt)*) => {
        compile_error!("empty effect sets are not a purity marker; write `Input -> Output => handler`")
    };
    (name = $name:literal, input = [$($in:tt)+], effects = [$($effects:tt)+], output = { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*, observers: [$($observer:expr),* $(,)?] $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_effect_duplicate_gate!(effects = [$($effects)+], then = [
            $crate::__obzenflow_effectful_stateful_typed!(
                input = exact($($in)+),
                output = $first,
                output_contract = [$first $(, $member)*],
                name = $name,
                handler = $handler_head $(:: $handler_tail)*,
                effects = [$($effects)+],
                middleware = [$($observer),*]
                $(, backpressure = [$bp])?
            )
        ])
    };
    (name = $name:literal, input = [$($in:tt)+], effects = [$($effects:tt)+], output = { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)* $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_effectful_stateful_row_contract!(
            name = $name,
            input = [$($in)+],
            effects = [$($effects)+],
            output = { $first $(, $member)* } => $handler_head $(:: $handler_tail)*,
            observers: []
            $(, backpressure: $bp)?
        )
    };
    (name = $name:literal, input = [$($in:tt)+], effects = [$($effects:tt)+], output = $out:ty => $handler_head:ident $(:: $handler_tail:ident)*, observers: [$($observer:expr),* $(,)?] $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_effect_duplicate_gate!(effects = [$($effects)+], then = [
            $crate::__obzenflow_effectful_stateful_typed!(
                input = exact($($in)+),
                output = $out,
                name = $name,
                handler = $handler_head $(:: $handler_tail)*,
                effects = [$($effects)+],
                middleware = [$($observer),*]
                $(, backpressure = [$bp])?
            )
        ])
    };
    (name = $name:literal, input = [$($in:tt)+], effects = [$($effects:tt)+], output = $out:ty => $handler_head:ident $(:: $handler_tail:ident)* $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_effectful_stateful_row_contract!(
            name = $name,
            input = [$($in)+],
            effects = [$($effects)+],
            output = $out => $handler_head $(:: $handler_tail)*,
            observers: []
            $(, backpressure: $bp)?
        )
    };
    (name = $name:literal, input = [$($in:tt)+], effects = [$($effects:tt)+], output = $($rest:tt)*) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "effectful_stateful!",
            "let handler = MyEffectfulStateful::new(...); output = effectful_stateful!(Input -> Output uses Effect => handler, observers: [...]);"
        )
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_effectful_stateful_typed {
    (input = exact($in:ty), output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = $handler:expr, effects = [$($effects:tt)*], middleware = [$($mw:expr),* $(,)?] $(, backpressure = [$($bp:expr)?])?) => {{
        let __handler = $handler;
        $crate::__obzenflow_assert_effectful_stateful_contract!(
            __handler, $in, [$($member),+], [$($effects)*]
        );
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::exact_payload::<$in>(),
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            false,
            None,
        )
        .with_additional_output_contract($crate::__obzenflow_output_contract_members!($($member),+));
        let __descriptor = $crate::__obzenflow_effectful_stateful_untyped!(
            name = $name,
            handler = __handler,
            effects = [$($effects)*],
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = $handler:expr, effects = [$($effects:tt)*], middleware = [$($mw:expr),* $(,)?] $(, backpressure = [$($bp:expr)?])?) => {{
        let __handler = $handler;
        $crate::__obzenflow_assert_effectful_stateful_contract!(
            __handler, $in, [$out], [$($effects)*]
        );
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::exact_payload::<$in>(),
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            false,
            None,
        );
        let __descriptor = $crate::__obzenflow_effectful_stateful_untyped!(
            name = $name,
            handler = __handler,
            effects = [$($effects)*],
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_effectful_stateful_exact_contract {
    (name = $name:literal, $($rest:tt)+) => {
        $crate::__obzenflow_effectful_stateful_exact_contract!(@collect name = $name, in = (), $($rest)+)
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $($effects:tt)* } { $first:ty $(, $member:ty)* $(,)? } => $($rest:tt)+) => {
        compile_error!("effectful_stateful!: arrow-embedded effect rows were removed; write `Input -> Output uses Effect => handler`")
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $($effects:tt)* } $out:ty => $($rest:tt)+) => {
        compile_error!("effectful_stateful!: arrow-embedded effect rows were removed; write `Input -> Output uses Effect => handler`")
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $($rest:tt)+) => {
        $crate::__obzenflow_effectful_stateful_exact_contract!(
            @output
            name = $name,
            input = [$($in)+],
            output = [],
            $($rest)+
        )
    };

    (@output name = $name:literal, input = [$($in:tt)+], output = [$($out:tt)+], uses { $($effects:tt)* } => $($rest:tt)+) => {
        $crate::__obzenflow_multi_effect_uses_gate!(effects = [$($effects)*], then = [
            $crate::__obzenflow_effectful_stateful_row_contract!(
                name = $name,
                input = [$($in)+],
                effects = [$($effects)*],
                output = $($out)+ => $($rest)+
            )
        ])
    };
    (@output name = $name:literal, input = [$($in:tt)+], output = [$($out:tt)+], uses $($rest:tt)+) => {
        $crate::__obzenflow_effectful_stateful_exact_contract!(
            @effect
            name = $name,
            input = [$($in)+],
            output = [$($out)+],
            effects = [],
            $($rest)+
        )
    };
    (@output name = $name:literal, input = [$($in:tt)+], output = [$($out:tt)+], => $($rest:tt)+) => {
        $crate::__obzenflow_effectful_stateful_exact_contract!(
            @pure
            name = $name,
            input = [$($in)+],
            output = [$($out)+],
            rest = [$($rest)+]
        )
    };
    (@output name = $name:literal, input = [$($in:tt)+], output = [$($out:tt)*], $tok:tt $($rest:tt)+) => {
        $crate::__obzenflow_effectful_stateful_exact_contract!(
            @output
            name = $name,
            input = [$($in)+],
            output = [$($out)* $tok],
            $($rest)+
        )
    };
    (@output name = $name:literal, input = [$($in:tt)+], output = [$($out:tt)*], $($rest:tt)*) => {
        compile_error!("effectful_stateful!: expected `Input -> Output uses Effect => handler`")
    };

    (@effect name = $name:literal, input = [$($in:tt)+], output = [$($out:tt)+], effects = [$($effects:tt)*], => $($rest:tt)+) => {
        $crate::__obzenflow_single_effect_uses_gate!(effects = [$($effects)*], then = [
            $crate::__obzenflow_effectful_stateful_row_contract!(
                name = $name,
                input = [$($in)+],
                effects = [$($effects)*],
                output = $($out)+ => $($rest)+
            )
        ])
    };
    (@effect name = $name:literal, input = [$($in:tt)+], output = [$($out:tt)+], effects = [$($effects:tt)*], $tok:tt $($rest:tt)+) => {
        $crate::__obzenflow_effectful_stateful_exact_contract!(
            @effect
            name = $name,
            input = [$($in)+],
            output = [$($out)+],
            effects = [$($effects)* $tok],
            $($rest)+
        )
    };
    (@effect name = $name:literal, input = [$($in:tt)+], output = [$($out:tt)+], effects = [$($effects:tt)*], $($rest:tt)*) => {
        compile_error!("effectful_stateful!: expected `=> handler` after the `uses` clause")
    };

    (@pure name = $name:literal, input = [$($in:tt)+], output = [{ $first:ty $(, $member:ty)* $(,)? }], rest = [$handler:expr, effects: [$($effects:tt)*] $($rest:tt)*]) => {
        compile_error!("effectful_stateful!: detached `effects: [...]` was removed; write `Input -> Output uses Effect => handler`")
    };
    (@pure name = $name:literal, input = [$($in:tt)+], output = [$out:ty], rest = [$handler:expr, effects: [$($effects:tt)*] $($rest:tt)*]) => {
        compile_error!("effectful_stateful!: detached `effects: [...]` was removed; write `Input -> Output uses Effect => handler`")
    };
    (@pure name = $name:literal, input = [$($in:tt)+], output = [{ $first:ty $(, $member:ty)* $(,)? }], rest = [$handler_head:ident $(:: $handler_tail:ident)*, observers: [$($observer:expr),* $(,)?] $(, backpressure: $bp:expr)? $(,)?]) => {
        $crate::__obzenflow_effectful_stateful_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            effects = [],
            middleware = [$($observer),*]
            $(, backpressure = [$bp])?
        )
    };
    (@pure name = $name:literal, input = [$($in:tt)+], output = [{ $first:ty $(, $member:ty)* $(,)? }], rest = [$handler_head:ident $(:: $handler_tail:ident)* $(, backpressure: $bp:expr)? $(,)?]) => {
        $crate::__obzenflow_effectful_stateful_exact_contract!(
            @pure
            name = $name,
            input = [$($in)+],
            output = [{ $first $(, $member)* }],
            rest = [$handler_head $(:: $handler_tail)*, observers: [] $(, backpressure: $bp)?]
        )
    };
    (@pure name = $name:literal, input = [$($in:tt)+], output = [$out:ty], rest = [$handler_head:ident $(:: $handler_tail:ident)*, observers: [$($observer:expr),* $(,)?] $(, backpressure: $bp:expr)? $(,)?]) => {
        $crate::__obzenflow_effectful_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            effects = [],
            middleware = [$($observer),*]
            $(, backpressure = [$bp])?
        )
    };
    (@pure name = $name:literal, input = [$($in:tt)+], output = [$out:ty], rest = [$handler_head:ident $(:: $handler_tail:ident)* $(, backpressure: $bp:expr)? $(,)?]) => {
        $crate::__obzenflow_effectful_stateful_exact_contract!(
            @pure
            name = $name,
            input = [$($in)+],
            output = [$out],
            rest = [$handler_head $(:: $handler_tail)*, observers: [] $(, backpressure: $bp)?]
        )
    };
    (@pure name = $name:literal, input = [$($in:tt)+], output = [$($out:tt)+], rest = [$handler:expr $(, observers: [$($observer:expr),* $(,)?])? $(, backpressure: $bp:expr)? $(,)?]) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "effectful_stateful!",
            "let handler = MyEffectfulStateful::new(...); output = effectful_stateful!(Input -> Output uses Effect => handler, observers: [...]);"
        )
    };
    (@collect name = $name:literal, in = (), -> $($rest:tt)*) => {
        compile_error!("effectful_stateful!: expected an input type before `->`")
    };
    (@collect name = $name:literal, in = ($($in:tt)*), $tok:tt $($rest:tt)+) => {
        $crate::__obzenflow_effectful_stateful_exact_contract!(
            @collect
            name = $name,
            in = ($($in)* $tok),
            $($rest)+
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), $($rest:tt)*) => {
        compile_error!("effectful_stateful!: expected `-> OutputType => handler` after input type");
    };
}

#[macro_export]
macro_rules! effectful_stateful {
    (name: $name:literal, $($rest:tt)+) => {
        $crate::__obzenflow_effectful_stateful_exact_contract!(name = $name, $($rest)+)
    };
    ($($rest:tt)+) => {
        $crate::__obzenflow_effectful_stateful_exact_contract!(
            name = "__obzenflow_binding_derived_name__",
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
        const _: () = ::obzenflow_core::assert_distinct_stage_fact_set::<
            ::obzenflow_core::stage_fact_set![$($member),+],
        >();
        let __observers: Vec<Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory>> =
            vec![$(Box::new($mw)),*];
        $crate::dsl::typing::placeholder_join_descriptor::<
            $ref_ty,
            $str_ty,
            $out,
            ::obzenflow_core::stage_fact_set![$($member),+],
            _,
        >($name, stringify!($ref_var), None, __observers)
    }};
    (reference = exact, stream = exact, output = $out:ty,
     output_contract = [$($member:ty),+ $(,)?],
     ref_type = ($ref_ty:ty), stream_type = ($str_ty:ty),
     name = $name:literal, ref_var = $ref_var:ident, handler = placeholder!($msg:expr),
     middleware = [$($mw:expr),*]) => {{
        const _: () = ::obzenflow_core::assert_distinct_stage_fact_set::<
            ::obzenflow_core::stage_fact_set![$($member),+],
        >();
        let __observers: Vec<Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory>> =
            vec![$(Box::new($mw)),*];
        $crate::dsl::typing::placeholder_join_descriptor::<
            $ref_ty,
            $str_ty,
            $out,
            ::obzenflow_core::stage_fact_set![$($member),+],
            _,
        >($name, stringify!($ref_var), Some($msg), __observers)
    }};

    // -- placeholder --
    (reference = exact, stream = exact, output = $out:ty,
     ref_type = ($ref_ty:ty), stream_type = ($str_ty:ty),
     name = $name:literal, ref_var = $ref_var:ident, handler = placeholder!(),
     middleware = [$($mw:expr),*]) => {{
        let __observers: Vec<Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory>> =
            vec![$(Box::new($mw)),*];
        $crate::dsl::typing::placeholder_join_descriptor::<
            $ref_ty, $str_ty, $out, $out, _,
        >($name, stringify!($ref_var), None, __observers)
    }};
    (reference = exact, stream = exact, output = $out:ty,
     ref_type = ($ref_ty:ty), stream_type = ($str_ty:ty),
     name = $name:literal, ref_var = $ref_var:ident, handler = placeholder!($msg:expr),
     middleware = [$($mw:expr),*]) => {{
        let __observers: Vec<Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory>> =
            vec![$(Box::new($mw)),*];
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
        const _: () = ::obzenflow_core::assert_distinct_stage_fact_set::<
            ::obzenflow_core::stage_fact_set![$($member),+],
        >();
        let __observers: Vec<Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory>> =
            vec![$(Box::new($mw)),*];
        $crate::dsl::typing::typed_join_descriptor::<
            _,
            $ref_ty,
            $str_ty,
            $out,
            ::obzenflow_core::stage_fact_set![$($member),+],
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
        let __observers: Vec<Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory>> =
            vec![$(Box::new($mw)),*];
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
     -> { $($out:ty),+ $(,)? } => $handler:expr,
     middleware: [$($mw:expr),* $(,)?] $($rest:tt)*) => {
        $crate::__obzenflow_stage_middleware_removed!()
    };
    (@collect
     name = $name:literal,
     ref_var = $ref_var:ident,
     reference = $ref_hint:tt,
     ref_type = $ref_type:tt,
     stream = ($($stream:tt)+),
     -> $out:ty => $handler:expr,
     middleware: [$($mw:expr),* $(,)?] $($rest:tt)*) => {
        $crate::__obzenflow_stage_middleware_removed!()
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
     -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!(),
     observers: [$($mw:expr),*]) => {
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
     -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr),
     observers: [$($mw:expr),*]) => {
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
     -> { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*,
     observers: [$($mw:expr),*]) => {
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
     -> { $first:ty $(, $member:ty)* $(,)? } => $handler:expr
     $(, observers: [$($mw:expr),*])?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "join!",
            "let handler = joins::inner::<Reference, Stream, Output, _, _, _, _>(...); output = join!(catalog reference: Reference, Stream -> Output => handler);"
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
     -> $out:ty => placeholder!(),
     observers: [$($mw:expr),*]) => {
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
     -> $out:ty => placeholder!($msg:expr),
     observers: [$($mw:expr),*]) => {
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
     -> $out:ty => $handler_head:ident $(:: $handler_tail:ident)*,
     observers: [$($mw:expr),*]) => {
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
     stream = ($($stream:tt)+),
     -> $out:ty => $handler:expr
     $(, observers: [$($mw:expr),*])?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "join!",
            "let handler = joins::inner::<Reference, Stream, Output, _, _, _, _>(...); output = join!(catalog reference: Reference, Stream -> Output => handler);"
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
macro_rules! join {
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
        ::obzenflow_core::ai::OversizePolicy::Error
    };

    (decompose { max_depth: $max_depth:expr, exhaustion: fail $(,)? } $(,)?) => {
        ::obzenflow_core::ai::OversizePolicy::Rerender {
            max_depth: $max_depth,
            min_progress_tokens: ::obzenflow_core::ai::TokenCount::new(1),
            exhaustion: ::obzenflow_core::ai::OversizeExhaustion::Fail,
        }
    };
    (decompose { max_depth: $max_depth:expr, exhaustion: exclude $(,)? } $(,)?) => {
        ::obzenflow_core::ai::OversizePolicy::Rerender {
            max_depth: $max_depth,
            min_progress_tokens: ::obzenflow_core::ai::TokenCount::new(1),
            exhaustion: ::obzenflow_core::ai::OversizeExhaustion::Exclude,
        }
    };

    (decompose { max_depth: $max_depth:expr, min_progress_tokens: $min_progress_tokens:expr, exhaustion: fail $(,)? } $(,)?) => {
        ::obzenflow_core::ai::OversizePolicy::Rerender {
            max_depth: $max_depth,
            min_progress_tokens: $min_progress_tokens,
            exhaustion: ::obzenflow_core::ai::OversizeExhaustion::Fail,
        }
    };
    (decompose { max_depth: $max_depth:expr, min_progress_tokens: $min_progress_tokens:expr, exhaustion: exclude $(,)? } $(,)?) => {
        ::obzenflow_core::ai::OversizePolicy::Rerender {
            max_depth: $max_depth,
            min_progress_tokens: $min_progress_tokens,
            exhaustion: ::obzenflow_core::ai::OversizeExhaustion::Exclude,
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
        ::obzenflow_runtime::stages::transform::ChunkByBudgetBuilder::<$($seed_ty)+, $item_ty>::new()
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
        ::obzenflow_runtime::stages::transform::ChunkByBudgetBuilder::<$($seed_ty)+, $item_ty>::new()
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
        ::obzenflow_runtime::stages::transform::ChunkByBudgetBuilder::<$($seed_ty)+, $item_ty>::new()
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
             via <chat binding> with <EffectResilience> => handler`"
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
        rest = [$role_head:ident $(:: $role_tail:ident)* $(,)?]
    ) => {{
        let __chat_effect_row = $crate::__obzenflow_effect_entries!(
            @generated_chat surface = "inference!",
            row = { $($row)* }
        );
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
macro_rules! inference {
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
        finalise_role = ($finalise_role:expr),
        map_effect_row = ($map_effect_row:expr),
        finalise_effect_row = ($finalise_effect_row:expr)
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
            ($chunker, $map_role, $finalise_role),
            ($map_effect_row, $finalise_effect_row),
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
        finalise_role = ($finalise_role:expr),
        map_row = { $($map_row:tt)* },
        finalise_row = { $($finalise_row:tt)* },
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
        finalise_role = ($finalise_role:expr),
        map_row = { $($map_row:tt)* },
        finalise_row = { $($finalise_row:tt)* },
        chunking = { $($chunking:tt)+ }
    ) => {{
        let __map_effect_row = $crate::__obzenflow_effect_entries!(
            @generated_chat surface = "ai_map_reduce!",
            row = { $($map_row)* }
        );
        let __finalise_effect_row = $crate::__obzenflow_effect_entries!(
            @generated_chat surface = "ai_map_reduce!",
            row = { $($finalise_row)* }
        );
        let __chunker = $crate::__obzenflow_ai_map_reduce_chunker_by_budget!(
            seed_type = ($($seed_ty)+),
            item_type = ($item_ty),
            estimator: ::obzenflow_adapters::ai::ChatBindingMetadata::estimator(
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
            finalise_role = ($finalise_role),
            map_effect_row = (__map_effect_row),
            finalise_effect_row = (__finalise_effect_row)
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
        compile_error!("ai_map_reduce!: map role requires `uses at_least_once(ChatCompletion) via <chat binding> with <EffectResilience>`")
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
                -> { $($finalise_row:tt)* } $reduce_out_ty:ty => $($rest:tt)*
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
                finalise_row = [$($row)*],
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
        compile_error!("ai_map_reduce!: reduce role requires `uses at_least_once(ChatCompletion) via <chat binding> with <EffectResilience>`")
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
                finalise_row = [$($row)*],
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
        finalise_row = [$($finalise_row:tt)*],
        chunking = [$($chunking:tt)+],
        role = [$($finalise_role:tt)*],
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
            finalise_row = [$($finalise_row)*],
            chunking = [$($chunking)+],
            role = [$($finalise_role)* $token],
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
        finalise_row = [$($finalise_row:tt)*],
        chunking = [$($chunking:tt)+],
        role = [$($finalise_role:tt)+],
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
            finalise_row = [$($finalise_row)*],
            finalise_role = [$($finalise_role)+],
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
        map_role = [$map_role_head:ident $(:: $map_role_tail:ident)* $(,)?],
        reduce_seed_type = [$reduce_seed_ty:ty],
        reduce_partial_type = [$reduce_partial_ty:ty],
        reduce_out_type = [$reduce_out_ty:ty],
        finalise_row = [$($finalise_row:tt)*],
        finalise_role = [$finalise_role_head:ident $(:: $finalise_role_tail:ident)* $(,)?],
        chunking = [$($chunking:tt)+]
    ) => {
        $crate::__obzenflow_ai_map_reduce_build!(
            name = $name,
            seed_type = ($seed_ty),
            item_type = ($item_ty),
            partial_type = ($partial_ty),
            out_type = ($out_ty),
            reduce_seed_type = ($reduce_seed_ty),
            reduce_partial_type = ($reduce_partial_ty),
            reduce_out_type = ($reduce_out_ty),
            map_role = ($map_role_head $(:: $map_role_tail)*),
            finalise_role = ($finalise_role_head $(:: $finalise_role_tail)*),
            map_row = { $($map_row)* },
            finalise_row = { $($finalise_row)* },
            chunking = { $($chunking)+ }
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
        finalise_row = [$($finalise_row:tt)*],
        finalise_role = [$finalise_role_head:ident $(:: $finalise_role_tail:ident)* $(,)?],
        chunking = [$($chunking:tt)+]
    ) => {
        $crate::__obzenflow_effect_entries!(
            @generated_chat_then surface = "ai_map_reduce!",
            row = { $($map_row)* },
            then = {
                $crate::__obzenflow_effect_entries!(
                    @generated_chat_then surface = "ai_map_reduce!",
                    row = { $($finalise_row)* },
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
        finalise_row = [$($finalise_row:tt)*],
        finalise_role = [$finalise_role:expr $(,)?],
        chunking = [$($chunking:tt)+]
    ) => {
        $crate::__obzenflow_effect_entries!(
            @generated_chat_then surface = "ai_map_reduce!",
            row = { $($map_row)* },
            then = {
                $crate::__obzenflow_effect_entries!(
                    @generated_chat_then surface = "ai_map_reduce!",
                    row = { $($finalise_row)* },
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
        finalise_row = [$($finalise_row:tt)*],
        finalise_role = [$finalise_role:expr $(,)?],
        chunking = [$($chunking:tt)+]
    ) => {
        $crate::__obzenflow_effect_entries!(
            @generated_chat_then surface = "ai_map_reduce!",
            row = { $($map_row)* },
            then = {
                $crate::__obzenflow_effect_entries!(
                    @generated_chat_then surface = "ai_map_reduce!",
                    row = { $($finalise_row)* },
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
             via <chat binding> with <EffectResilience> => role` clauses on map and reduce"
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
macro_rules! ai_map_reduce {
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

#[cfg(test)]
mod backpressure_clause_macro_tests {
    use crate::dsl::backpressure_clause::enforced;

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    struct TestFact(u64);

    impl obzenflow_core::TypedPayload for TestFact {
        const EVENT_TYPE: &'static str = "bp_clause_test.fact";
    }

    #[test]
    fn source_macro_accepts_backpressure_clause_end_to_end() {
        // Full user-facing chain: source! -> classified placeholder admission.
        let descriptor = crate::source!(
            name: "s",
            TestFact => placeholder!(),
            observers: [],
            backpressure: enforced(1000)
        );
        assert_eq!(descriptor.name(), "s");
        assert!(
            descriptor.backpressure_clause().is_some(),
            "the clause survives the typing wrapper"
        );
    }

    #[test]
    fn source_macro_without_clause_leaves_backpressure_none() {
        let descriptor = crate::source!(
            name: "s_none",
            TestFact => placeholder!(),
            observers: []
        );
        assert!(descriptor.backpressure_clause().is_none());
    }

    // The syntax-section forms: transform! with a clause and no middleware
    // list, and stateful! likewise, must parse.
    #[test]
    fn transform_and_stateful_macros_accept_clause_without_middleware_list() {
        use crate::dsl::backpressure_clause::{enforced_from_config, track_only};

        let t = crate::transform!(
            name: "t",
            TestFact -> TestFact => placeholder!(),
            backpressure: enforced_from_config()
        );
        assert!(t.backpressure_clause().is_some());

        let s = crate::stateful!(
            name: "st",
            TestFact -> TestFact => placeholder!(),
            backpressure: track_only()
        );
        assert!(s.backpressure_clause().is_some());
    }
}
