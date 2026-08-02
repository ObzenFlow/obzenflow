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
//! async_transform!(In -> Out => handler)
//! effectful_transform!(In -> Out => handler, effects: [], middleware: [])
//! stateful!(In -> Out => handler)
//! effectful_stateful!(In -> Out => handler, effects: [], middleware: [])
//! sink!(In => handler)
//! join!(catalog CatalogStage: Catalog, Stream -> Out => handler)
//! inference!(In -> { /* effect row */ } Out => role)
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
//! family's applicable contract, middleware, backpressure, effect,
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
    ($surface:literal) => {
        compile_error!(concat!(
            $surface,
            ": poll timeout is handler configuration; timeout tuples are no longer accepted. ",
            "Configure the handler inside the materialiser, then pass its name. Example: ",
            "let source = MySource::builder().poll_timeout(timeout).build()?; ",
            "events = async_source!(Event => source);",
        ))
    };
}

// ============================================================================
// source!  +  __obzenflow_source_typed!
// ============================================================================

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_source_typed {
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!(), middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            true,
            None,
        )
        .with_additional_output_contract($crate::__obzenflow_output_contract_members!($($member),+));
        let __descriptor = $crate::__obzenflow_source_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderFiniteSource::<$out>::new(None),
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!($msg:expr), middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            true,
            Some(($msg).to_string()),
        )
        .with_additional_output_contract($crate::__obzenflow_output_contract_members!($($member),+));
        let __descriptor = $crate::__obzenflow_source_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderFiniteSource::<$out>::new(Some($msg)),
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __handler = $handler;
        // FLOWIP-114c PR D: assert_source_output dropped, see sink rationale.
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            false,
            None,
        )
        .with_additional_output_contract($crate::__obzenflow_output_contract_members!($($member),+));
        let __descriptor = $crate::__obzenflow_source_untyped!(
            name = $name,
            handler = __handler,
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (output = $out:ty, name = $name:literal, handler = placeholder!(), middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            true,
            None,
        );
        let __descriptor = $crate::__obzenflow_source_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderFiniteSource::<$out>::new(None),
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (output = $out:ty, name = $name:literal, handler = placeholder!($msg:expr), middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::__obzenflow_source_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderFiniteSource::<$out>::new(Some($msg)),
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (output = $out:ty, name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __handler = $handler;
        // FLOWIP-114c PR D: assert_source_output dropped, see sink rationale.
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            false,
            None,
        );
        let __descriptor = $crate::__obzenflow_source_untyped!(
            name = $name,
            handler = __handler,
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_source_untyped {
    (name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        use $crate::dsl::stage_descriptor::{FiniteSourceDescriptor, StageDescriptor};
        Box::new(FiniteSourceDescriptor {
            name: $name.to_string(),
            handler: $handler,
            middleware: vec![$(Box::new($mw)),*],
            backpressure: {
                #[allow(unused_mut)]
                let mut __bp: Option<$crate::dsl::backpressure_clause::BackpressureClause> = None;
                $($( __bp = Some($bp); )?)?
                __bp
            },
        }) as Box<dyn StageDescriptor>
    }};
}

/// Create a finite source stage descriptor.
#[macro_export]
macro_rules! source {
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
macro_rules! __obzenflow_async_source_untyped {
    (name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let mut __desc = $crate::dsl::stage_descriptor::AsyncFiniteSourceDescriptor::new($name, $handler)
            $(.with_middleware($mw))*;
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
macro_rules! __obzenflow_async_source_typed {
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!(), middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            true,
            None,
        )
        .with_additional_output_contract($crate::__obzenflow_output_contract_members!($($member),+));
        let __descriptor = $crate::__obzenflow_async_source_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderAsyncSource::<$out>::new(None),
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!($msg:expr), middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            true,
            Some(($msg).to_string()),
        )
        .with_additional_output_contract($crate::__obzenflow_output_contract_members!($($member),+));
        let __descriptor = $crate::__obzenflow_async_source_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderAsyncSource::<$out>::new(Some($msg)),
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __handler = $handler;
        // FLOWIP-114c PR D: assert_source_output dropped, see sink rationale.
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            false,
            None,
        )
        .with_additional_output_contract($crate::__obzenflow_output_contract_members!($($member),+));
        let __descriptor = $crate::__obzenflow_async_source_untyped!(
            name = $name,
            handler = __handler,
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (output = $out:ty, name = $name:literal, handler = placeholder!(), middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            true,
            None,
        );
        let __descriptor = $crate::__obzenflow_async_source_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderAsyncSource::<$out>::new(None),
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (output = $out:ty, name = $name:literal, handler = placeholder!($msg:expr), middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::__obzenflow_async_source_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderAsyncSource::<$out>::new(Some($msg)),
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (output = $out:ty, name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __handler = $handler;
        // FLOWIP-114c PR D: assert_source_output dropped, see sink rationale.
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            false,
            None,
        );
        let __descriptor = $crate::__obzenflow_async_source_untyped!(
            name = $name,
            handler = __handler,
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
}

/// Create an async finite source stage descriptor.
#[macro_export]
macro_rules! async_source {
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
        $crate::__obzenflow_async_source_timeout_diagnostic!("async_source!")
    };
    ($out:ty => ($handler:expr, $poll_timeout:expr) $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_timeout_diagnostic!("async_source!")
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => ($handler:expr, $poll_timeout:expr) $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_timeout_diagnostic!("async_source!")
    };
    (name: $name:literal, $out:ty => ($handler:expr, $poll_timeout:expr) $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_timeout_diagnostic!("async_source!")
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
macro_rules! __obzenflow_infinite_source_untyped {
    (name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        use $crate::dsl::stage_descriptor::{InfiniteSourceDescriptor, StageDescriptor};
        Box::new(InfiniteSourceDescriptor {
            name: $name.to_string(),
            handler: $handler,
            middleware: vec![$(Box::new($mw)),*],
            backpressure: {
                #[allow(unused_mut)]
                let mut __bp: Option<$crate::dsl::backpressure_clause::BackpressureClause> = None;
                $($( __bp = Some($bp); )?)?
                __bp
            },
        }) as Box<dyn StageDescriptor>
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_infinite_source_typed {
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!(), middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            true,
            None,
        )
        .with_additional_output_contract($crate::__obzenflow_output_contract_members!($($member),+));
        let __descriptor = $crate::__obzenflow_infinite_source_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderInfiniteSource::<$out>::new(None),
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!($msg:expr), middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            true,
            Some(($msg).to_string()),
        )
        .with_additional_output_contract($crate::__obzenflow_output_contract_members!($($member),+));
        let __descriptor = $crate::__obzenflow_infinite_source_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderInfiniteSource::<$out>::new(Some($msg)),
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __handler = $handler;
        // FLOWIP-114c PR D: assert_source_output dropped, see sink rationale.
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            false,
            None,
        )
        .with_additional_output_contract($crate::__obzenflow_output_contract_members!($($member),+));
        let __descriptor = $crate::__obzenflow_infinite_source_untyped!(
            name = $name,
            handler = __handler,
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (output = $out:ty, name = $name:literal, handler = placeholder!(), middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            true,
            None,
        );
        let __descriptor = $crate::__obzenflow_infinite_source_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderInfiniteSource::<$out>::new(None),
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (output = $out:ty, name = $name:literal, handler = placeholder!($msg:expr), middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::__obzenflow_infinite_source_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderInfiniteSource::<$out>::new(Some($msg)),
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (output = $out:ty, name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __handler = $handler;
        // FLOWIP-114c PR D: assert_source_output dropped, see sink rationale.
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            false,
            None,
        );
        let __descriptor = $crate::__obzenflow_infinite_source_untyped!(
            name = $name,
            handler = __handler,
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
}

/// Create an infinite source stage descriptor.
#[macro_export]
macro_rules! infinite_source {
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
macro_rules! __obzenflow_async_infinite_source_untyped {
    (name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let mut __desc = $crate::dsl::stage_descriptor::AsyncInfiniteSourceDescriptor::new($name, $handler)
            $(.with_middleware($mw))*;
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
macro_rules! __obzenflow_async_infinite_source_typed {
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!(), middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            true,
            None,
        )
        .with_additional_output_contract($crate::__obzenflow_output_contract_members!($($member),+));
        let __descriptor = $crate::__obzenflow_async_infinite_source_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderAsyncSource::<$out>::new(None),
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!($msg:expr), middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            true,
            Some(($msg).to_string()),
        )
        .with_additional_output_contract($crate::__obzenflow_output_contract_members!($($member),+));
        let __descriptor = $crate::__obzenflow_async_infinite_source_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderAsyncSource::<$out>::new(Some($msg)),
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __handler = $handler;
        // FLOWIP-114c PR D: assert_source_output dropped, see sink rationale.
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            false,
            None,
        )
        .with_additional_output_contract($crate::__obzenflow_output_contract_members!($($member),+));
        let __descriptor = $crate::__obzenflow_async_infinite_source_untyped!(
            name = $name,
            handler = __handler,
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (output = $out:ty, name = $name:literal, handler = placeholder!(), middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            true,
            None,
        );
        let __descriptor = $crate::__obzenflow_async_infinite_source_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderAsyncSource::<$out>::new(None),
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (output = $out:ty, name = $name:literal, handler = placeholder!($msg:expr), middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::__obzenflow_async_infinite_source_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderAsyncSource::<$out>::new(Some($msg)),
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (output = $out:ty, name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __handler = $handler;
        // FLOWIP-114c PR D: assert_source_output dropped, see sink rationale.
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            false,
            None,
        );
        let __descriptor = $crate::__obzenflow_async_infinite_source_untyped!(
            name = $name,
            handler = __handler,
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
}

/// Create an async infinite source stage descriptor.
#[macro_export]
macro_rules! async_infinite_source {
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
        $crate::__obzenflow_async_source_timeout_diagnostic!("async_infinite_source!")
    };
    ($out:ty => ($handler:expr, $poll_timeout:expr) $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_timeout_diagnostic!("async_infinite_source!")
    };
    (name: $name:literal, { $first:ty $(, $member:ty)* $(,)? } => ($handler:expr, $poll_timeout:expr) $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_timeout_diagnostic!("async_infinite_source!")
    };
    (name: $name:literal, $out:ty => ($handler:expr, $poll_timeout:expr) $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_source_timeout_diagnostic!("async_infinite_source!")
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
macro_rules! __obzenflow_transform_untyped {
    (name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        use $crate::dsl::stage_descriptor::{StageDescriptor, TransformDescriptor};
        Box::new(TransformDescriptor {
            name: $name.to_string(),
            handler: $handler,
            middleware: vec![$(Box::new($mw)),*],
            backpressure: {
                #[allow(unused_mut)]
                let mut __bp: Option<$crate::dsl::backpressure_clause::BackpressureClause> = None;
                $($( __bp = Some($bp); )?)?
                __bp
            },
        }) as Box<dyn StageDescriptor>
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_transform_typed {
    // -- exact input, placeholder, explicit output contract --
    (input = exact($in:ty), output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!(), middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::exact_payload::<$in>(),
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            true,
            None,
        )
        .with_additional_output_contract($crate::__obzenflow_output_contract_members!($($member),+));
        let __descriptor = $crate::__obzenflow_transform_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderTransform::<$in, $out>::new(None),
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (input = exact($in:ty), output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!($msg:expr), middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::exact_payload::<$in>(),
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            true,
            Some(($msg).to_string()),
        )
        .with_additional_output_contract($crate::__obzenflow_output_contract_members!($($member),+));
        let __descriptor = $crate::__obzenflow_transform_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderTransform::<$in, $out>::new(Some($msg)),
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // -- exact input, real handler, explicit output contract --
    // FLOWIP-120b Option B keeps the flat output contract in the arrow's
    // type-signature position; the handler-side carrier is never named in the
    // flow.
    (input = exact($in:ty), output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __handler = $handler;
        fn __obzenflow_assert_output_contract_equality<
            __Handler,
            __ArrowToHandlerProof,
            __HandlerToArrowProof,
        >(
            _handler: &__Handler,
        )
        where
            __Handler: ::obzenflow_runtime::stages::common::handlers::TypedTransformHandler<
                Input = $in,
            >,
            <::obzenflow_core::stage_fact_set![$($member),+] as ::obzenflow_core::StageFactSet>::Members:
                $crate::dsl::typing::ArrowOutputsAreDeclaredByHandler<
                    <<__Handler as ::obzenflow_runtime::stages::common::handlers::TypedTransformHandler>::Output as ::obzenflow_core::StageFactSet>::Members,
                    __ArrowToHandlerProof,
                >,
            <<__Handler as ::obzenflow_runtime::stages::common::handlers::TypedTransformHandler>::Output as ::obzenflow_core::StageFactSet>::Members:
                $crate::dsl::typing::HandlerOutputsAreDeclaredByArrow<
                    <::obzenflow_core::stage_fact_set![$($member),+] as ::obzenflow_core::StageFactSet>::Members,
                    __HandlerToArrowProof,
                >,
        {
        }
        __obzenflow_assert_output_contract_equality::<_, _, _>(&__handler);
        const _: () = ::obzenflow_core::assert_distinct_stage_fact_set::<
            ::obzenflow_core::stage_fact_set![$($member),+],
        >();
        let __handler =
            ::obzenflow_runtime::stages::common::handlers::TypedTransformHandlerAdapter::new(
                __handler,
            );
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::exact_payload::<$in>(),
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            false,
            None,
        )
        .with_additional_output_contract($crate::__obzenflow_output_contract_members!($($member),+));
        let __descriptor = $crate::__obzenflow_transform_untyped!(name = $name, handler = __handler, middleware = [$($mw),*] $(, backpressure = [$($bp)?])?);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // ── exact input, placeholder ──
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = placeholder!(), middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::exact_payload::<$in>(),
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            true,
            None,
        );
        let __descriptor = $crate::__obzenflow_transform_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderTransform::<$in, $out>::new(None),
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = placeholder!($msg:expr), middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::exact_payload::<$in>(),
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::__obzenflow_transform_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderTransform::<$in, $out>::new(Some($msg)),
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // ── exact input, real handler ──
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __handler = $handler;
        let __handler =
            $crate::dsl::typing::BoundTransform::<$in, $out, _>::new(__handler);
        ::obzenflow_runtime::typing::assert_transform_contract::<_, $in, $out>(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::exact_payload::<$in>(),
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            false,
            None,
        );
        let __descriptor = $crate::__obzenflow_transform_untyped!(name = $name, handler = __handler, middleware = [$($mw),*] $(, backpressure = [$($bp)?])?);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_transform_exact_contract {
    (name = $name:literal, $($rest:tt)+) => {
        $crate::__obzenflow_transform_exact_contract!(@collect name = $name, in = (), $($rest)+)
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
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!(), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
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
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
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
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
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
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty, outputs: [$($member:ty),+ $(,)?] => placeholder!(), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
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
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty, outputs: [$($member:ty),+ $(,)?] => placeholder!($msg:expr), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
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
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty, outputs: [$($member:ty),+ $(,)?] => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
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
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!(), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!(),
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!($msg:expr), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
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
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => $handler:expr $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "transform!",
            "let handler = MyTransform::new(...); output = transform!(Input -> Output => handler);"
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty, outputs: [$($member:ty),+ $(,)?] => $handler:expr $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "transform!",
            "let handler = MyTransform::new(...); output = transform!(Input -> Output => handler);"
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler:expr $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
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
// async_transform!  +  __obzenflow_async_transform_typed!
// ============================================================================

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_async_transform_untyped {
    (name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        use $crate::dsl::stage_descriptor::{AsyncTransformDescriptor, StageDescriptor};
        Box::new(AsyncTransformDescriptor {
            name: $name.to_string(),
            handler: $handler,
            middleware: vec![$(Box::new($mw)),*],
            backpressure: {
                #[allow(unused_mut)]
                let mut __bp: Option<$crate::dsl::backpressure_clause::BackpressureClause> = None;
                $($( __bp = Some($bp); )?)?
                __bp
            },
        }) as Box<dyn StageDescriptor>
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_async_transform_typed {
    // -- exact input, placeholder, explicit output contract --
    (input = exact($in:ty), output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!(), middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::exact_payload::<$in>(),
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            true,
            None,
        )
        .with_additional_output_contract($crate::__obzenflow_output_contract_members!($($member),+));
        let __descriptor = $crate::__obzenflow_async_transform_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderAsyncTransform::<$in, $out>::new(None),
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (input = exact($in:ty), output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!($msg:expr), middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::exact_payload::<$in>(),
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            true,
            Some(($msg).to_string()),
        )
        .with_additional_output_contract($crate::__obzenflow_output_contract_members!($($member),+));
        let __descriptor = $crate::__obzenflow_async_transform_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderAsyncTransform::<$in, $out>::new(Some($msg)),
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // -- exact input, real handler, explicit output contract --
    (input = exact($in:ty), output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __handler = $handler;
        let __handler =
            $crate::dsl::typing::BoundAsyncTransform::<$in, $out, _>::new(__handler);
        ::obzenflow_runtime::typing::assert_transform_contract::<_, $in, $out>(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::exact_payload::<$in>(),
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            false,
            None,
        )
        .with_additional_output_contract($crate::__obzenflow_output_contract_members!($($member),+));
        let __descriptor = $crate::__obzenflow_async_transform_untyped!(name = $name, handler = __handler, middleware = [$($mw),*] $(, backpressure = [$($bp)?])?);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // ── exact input, placeholder ──
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = placeholder!(), middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::exact_payload::<$in>(),
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            true,
            None,
        );
        let __descriptor = $crate::__obzenflow_async_transform_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderAsyncTransform::<$in, $out>::new(None),
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = placeholder!($msg:expr), middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::exact_payload::<$in>(),
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::__obzenflow_async_transform_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderAsyncTransform::<$in, $out>::new(Some($msg)),
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // ── exact input, real handler ──
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __handler = $handler;
        let __handler =
            $crate::dsl::typing::BoundAsyncTransform::<$in, $out, _>::new(__handler);
        ::obzenflow_runtime::typing::assert_transform_contract::<_, $in, $out>(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::exact_payload::<$in>(),
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            false,
            None,
        );
        let __descriptor = $crate::__obzenflow_async_transform_untyped!(name = $name, handler = __handler, middleware = [$($mw),*] $(, backpressure = [$($bp)?])?);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_async_transform_exact_contract {
    (name = $name:literal, $($rest:tt)+) => {
        $crate::__obzenflow_async_transform_exact_contract!(@collect name = $name, in = (), $($rest)+)
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!() $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_transform_typed!(
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
        $crate::__obzenflow_async_transform_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!($msg),
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!(), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_transform_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = placeholder!(),
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_transform_typed!(
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
        $crate::__obzenflow_async_transform_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_transform_typed!(
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
        $crate::__obzenflow_async_transform_typed!(
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
        $crate::__obzenflow_async_transform_typed!(
            input = exact($($in)+),
            output = $out,
            output_contract = [$($member),+],
            name = $name,
            handler = placeholder!($msg),
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty, outputs: [$($member:ty),+ $(,)?] => placeholder!(), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_transform_typed!(
            input = exact($($in)+),
            output = $out,
            output_contract = [$($member),+],
            name = $name,
            handler = placeholder!(),
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty, outputs: [$($member:ty),+ $(,)?] => placeholder!($msg:expr), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_transform_typed!(
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
        $crate::__obzenflow_async_transform_typed!(
            input = exact($($in)+),
            output = $out,
            output_contract = [$($member),+],
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty, outputs: [$($member:ty),+ $(,)?] => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_transform_typed!(
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
        $crate::__obzenflow_async_transform_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!(),
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!($msg:expr) $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_transform_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!($msg),
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!(), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_transform_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!(),
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!($msg:expr), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_transform_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!($msg),
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler_head:ident $(:: $handler_tail:ident)* $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_transform_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = []
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_async_transform_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => $handler:expr $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "async_transform!",
            "let handler = MyAsyncTransform::new(...); output = async_transform!(Input -> Output => handler);"
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty, outputs: [$($member:ty),+ $(,)?] => $handler:expr $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "async_transform!",
            "let handler = MyAsyncTransform::new(...); output = async_transform!(Input -> Output => handler);"
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler:expr $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "async_transform!",
            "let handler = MyAsyncTransform::new(...); output = async_transform!(Input -> Output => handler);"
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)*), $tok:tt $($rest:tt)+) => {
        $crate::__obzenflow_async_transform_exact_contract!(
            @collect
            name = $name,
            in = ($($in)* $tok),
            $($rest)+
        )
    };
    (@collect name = $name:literal, in = (), -> $($rest:tt)*) => {
        compile_error!("async_transform!: expected `InputType -> OutputType => handler`");
    };
    (@collect name = $name:literal, in = ($($in:tt)+), $($rest:tt)*) => {
        compile_error!("async_transform!: expected `-> OutputType => handler` after input type");
    };
}

/// Create an async transform stage descriptor.
#[macro_export]
macro_rules! async_transform {
    // ── typed (exact input) ──
    (name: $name:literal, $($rest:tt)+) => {
        $crate::__obzenflow_async_transform_exact_contract!(name = $name, $($rest)+)
    };
    ($($rest:tt)+) => {
        $crate::__obzenflow_async_transform_exact_contract!(
            name = "__obzenflow_binding_derived_name__",
            $($rest)+
        )
    };
}

// ============================================================================
// effectful_transform!
// ============================================================================

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_effect_declarations_vec {
    (@push $effects:ident,) => {};
    (@push $effects:ident, transactional($effect:ty, $executor:expr) $(, $($rest:tt)*)?) => {{
        $effects.push(::obzenflow_runtime::effects::EffectDeclaration::transactional_effect::<$effect>($executor));
        $crate::__obzenflow_effect_declarations_vec!(@push $effects, $($($rest)*)?);
    }};
    (@push $effects:ident, $effect:ty $(, $($rest:tt)*)?) => {{
        $effects.push(::obzenflow_runtime::effects::EffectDeclaration::of::<$effect>());
        $crate::__obzenflow_effect_declarations_vec!(@push $effects, $($($rest)*)?);
    }};
    ($($effect_spec:tt)*) => {{
        let mut __obzenflow_effects = Vec::new();
        $crate::__obzenflow_effect_declarations_vec!(@push __obzenflow_effects, $($effect_spec)*);
        __obzenflow_effects
    }};
}

/// Entry parser for the `effects:` clause with inline per-effect policy
/// attachments (FLOWIP-120c H7): `Effect with [policy, ...]`. A declared
/// effect carries its policies in place, so the macro knows by position
/// which effect a builder guards; there is deliberately no shorthand for
/// one policy spec across all effects, because two guarded dependencies are
/// two policies.
///
/// Entry type tokens accumulate one token at a time until `with` or `,`;
/// wrap a generic effect type containing top-level commas in parentheses.
#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_effect_entries {
    // ── end of input ───────────────────────────────────────────────────
    (@entry $effects:ident, $atts:ident, [],) => {};
    (@entry $effects:ident, $atts:ident, [$($acc:tt)+],) => {
        $effects.push(::obzenflow_runtime::effects::EffectDeclaration::of::<$($acc)+>());
    };

    // ── transactional entries (recognized at entry start) ─────────────
    (@entry $effects:ident, $atts:ident, [], transactional($effect:ty, $executor:expr) with [$($policy:expr),* $(,)?], $($rest:tt)*) => {
        $effects.push(::obzenflow_runtime::effects::EffectDeclaration::transactional_effect::<$effect>($executor));
        $crate::__obzenflow_effect_entries!(@attach $atts, $effect, [$($policy),*]);
        $crate::__obzenflow_effect_entries!(@entry $effects, $atts, [], $($rest)*);
    };
    (@entry $effects:ident, $atts:ident, [], transactional($effect:ty, $executor:expr) with [$($policy:expr),* $(,)?]) => {
        $effects.push(::obzenflow_runtime::effects::EffectDeclaration::transactional_effect::<$effect>($executor));
        $crate::__obzenflow_effect_entries!(@attach $atts, $effect, [$($policy),*]);
    };
    (@entry $effects:ident, $atts:ident, [], transactional($effect:ty, $executor:expr), $($rest:tt)*) => {
        $effects.push(::obzenflow_runtime::effects::EffectDeclaration::transactional_effect::<$effect>($executor));
        $crate::__obzenflow_effect_entries!(@entry $effects, $atts, [], $($rest)*);
    };
    (@entry $effects:ident, $atts:ident, [], transactional($effect:ty, $executor:expr)) => {
        $effects.push(::obzenflow_runtime::effects::EffectDeclaration::transactional_effect::<$effect>($executor));
    };

    // ── `with [...]` attachment terminator ─────────────────────────────
    (@entry $effects:ident, $atts:ident, [$($acc:tt)+], with [$($policy:expr),* $(,)?], $($rest:tt)*) => {
        $effects.push(::obzenflow_runtime::effects::EffectDeclaration::of::<$($acc)+>());
        $crate::__obzenflow_effect_entries!(@attach $atts, $($acc)+, [$($policy),*]);
        $crate::__obzenflow_effect_entries!(@entry $effects, $atts, [], $($rest)*);
    };
    (@entry $effects:ident, $atts:ident, [$($acc:tt)+], with [$($policy:expr),* $(,)?]) => {
        $effects.push(::obzenflow_runtime::effects::EffectDeclaration::of::<$($acc)+>());
        $crate::__obzenflow_effect_entries!(@attach $atts, $($acc)+, [$($policy),*]);
    };

    // ── comma terminator ────────────────────────────────────────────────
    (@entry $effects:ident, $atts:ident, [$($acc:tt)+], , $($rest:tt)*) => {
        $effects.push(::obzenflow_runtime::effects::EffectDeclaration::of::<$($acc)+>());
        $crate::__obzenflow_effect_entries!(@entry $effects, $atts, [], $($rest)*);
    };

    // ── accumulate one type token ───────────────────────────────────────
    (@entry $effects:ident, $atts:ident, [$($acc:tt)*], $next:tt $($rest:tt)*) => {
        $crate::__obzenflow_effect_entries!(@entry $effects, $atts, [$($acc)* $next], $($rest)*);
    };

    // ── attachment construction ─────────────────────────────────────────
    (@attach $atts:ident, $effect:ty, [$($policy:expr),*]) => {{
        let __effect_type: &'static str =
            <$effect as ::obzenflow_runtime::effects::Effect>::EFFECT_TYPE;
        let mut __factories: Vec<Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory>> =
            Vec::new();
        $(
            {
                let __factory: Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory> =
                    $policy;
                __factories.push(__factory);
            }
        )*
        $atts.push($crate::dsl::stage_descriptor::EffectPolicyAttachment {
            effect_type: __effect_type,
            factories: __factories,
        });
    }};
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

    (@entry [$($types:ty,)*], [], transactional($effect:ty, $executor:expr) with [$($policy:expr),* $(,)?], $($rest:tt)*) => {
        $crate::__obzenflow_effect_manifest_types!(@entry [$($types,)* $effect,], [], $($rest)*)
    };
    (@entry [$($types:ty,)*], [], transactional($effect:ty, $executor:expr) with [$($policy:expr),* $(,)?]) => {
        ::obzenflow_runtime::effect_set![$($types,)* $effect]
    };
    (@entry [$($types:ty,)*], [], transactional($effect:ty, $executor:expr), $($rest:tt)*) => {
        $crate::__obzenflow_effect_manifest_types!(@entry [$($types,)* $effect,], [], $($rest)*)
    };
    (@entry [$($types:ty,)*], [], transactional($effect:ty, $executor:expr)) => {
        ::obzenflow_runtime::effect_set![$($types,)* $effect]
    };

    (@entry [$($types:ty,)*], [$($acc:tt)+], with [$($policy:expr),* $(,)?], $($rest:tt)*) => {
        $crate::__obzenflow_effect_manifest_types!(@entry [$($types,)* $($acc)+,], [], $($rest)*)
    };
    (@entry [$($types:ty,)*], [$($acc:tt)+], with [$($policy:expr),* $(,)?]) => {
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
        let __middleware: Vec<Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory>> =
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
            __middleware,
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
macro_rules! __obzenflow_effectful_transform_exact_contract {
    (name = $name:literal, $($rest:tt)+) => {
        $crate::__obzenflow_effectful_transform_exact_contract!(@collect name = $name, in = (), $($rest)+)
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*, effects: [$($effects:tt)*], middleware: [$($mw:expr),* $(,)?] $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_effectful_transform_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            effects = [$($effects)*],
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty, outputs: [$($member:ty),+ $(,)?] => $handler_head:ident $(:: $handler_tail:ident)*, effects: [$($effects:tt)*], middleware: [$($mw:expr),* $(,)?] $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_effectful_transform_typed!(
            input = exact($($in)+),
            output = $out,
            output_contract = [$($member),+],
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            effects = [$($effects)*],
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler_head:ident $(:: $handler_tail:ident)*, effects: [$($effects:tt)*], middleware: [$($mw:expr),* $(,)?] $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_effectful_transform_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            effects = [$($effects)*],
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => $handler:expr, effects: [$($effects:tt)*], middleware: [$($mw:expr),* $(,)?] $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "effectful_transform!",
            "let handler = MyEffectfulTransform::new(...); output = effectful_transform!(Input -> Output => handler, effects: [...], middleware: [...]);"
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty, outputs: [$($member:ty),+ $(,)?] => $handler:expr, effects: [$($effects:tt)*], middleware: [$($mw:expr),* $(,)?] $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "effectful_transform!",
            "let handler = MyEffectfulTransform::new(...); output = effectful_transform!(Input -> Output => handler, effects: [...], middleware: [...]);"
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler:expr, effects: [$($effects:tt)*], middleware: [$($mw:expr),* $(,)?] $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "effectful_transform!",
            "let handler = MyEffectfulTransform::new(...); output = effectful_transform!(Input -> Output => handler, effects: [...], middleware: [...]);"
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)*), $tok:tt $($rest:tt)+) => {
        $crate::__obzenflow_effectful_transform_exact_contract!(
            @collect
            name = $name,
            in = ($($in)* $tok),
            $($rest)+
        )
    };
    (@collect name = $name:literal, in = (), -> $($rest:tt)*) => {
        compile_error!("effectful_transform!: expected `InputType -> OutputType => handler`");
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
macro_rules! __obzenflow_sink_untyped {
    (name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*]) => {{
        use $crate::dsl::stage_descriptor::{SinkDescriptor, StageDescriptor};
        Box::new(SinkDescriptor {
            name: $name.to_string(),
            handler: $handler,
            middleware: vec![$(Box::new($mw)),*],
        }) as Box<dyn StageDescriptor>
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_sink_typed {
    // ── exact input, placeholder ──
    (input = exact($in:ty), name = $name:literal, handler = placeholder!(), middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::sink(
            $crate::dsl::typing::TypeHint::exact_payload::<$in>(),
            true,
            None,
        );
        let __descriptor = $crate::__obzenflow_sink_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderSink::<$in>::new(None),
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (input = exact($in:ty), name = $name:literal, handler = placeholder!($msg:expr), middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::sink(
            $crate::dsl::typing::TypeHint::exact_payload::<$in>(),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::__obzenflow_sink_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderSink::<$in>::new(Some($msg)),
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};

    // ── exact input, real handler (facade call anchoring) ──
    //
    // Like joins, sink facade helpers often need the contract type injected to avoid
    // turbofish/annotations at the call site (e.g., `sinks::json()` and `sinks::table(...)`).

    // ── exact input, real handler ──
    //
    // FLOWIP-114c PR D: the previous `assert_sink_input::<_, $in>` check is dropped.
    // Per the proposal's canonical-identity rationale, the declared input is a
    // topology fingerprint, not a Rust type-system constraint, matching the
    // tautological pattern already used by `BoundTransform` wrappers.
    (input = exact($in:ty), name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*]) => {{
        let __handler = $handler;
        let __metadata = $crate::dsl::typing::StageTypingMetadata::sink(
            $crate::dsl::typing::TypeHint::exact_payload::<$in>(),
            false,
            None,
        );
        let __descriptor = $crate::__obzenflow_sink_untyped!(name = $name, handler = __handler, middleware = [$($mw),*]);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
}

/// Lower the optional `delivery:` clause of `sink!` (FLOWIP-120n F16,
/// FLOWIP-120s). Routes through the sealed `DeclareDeliverySafety` trait so
/// the clause is accepted only by the closure-tier typed sinks; a typed
/// `Delivery` carries `SAFETY` on the type and fails here by trait bound.
#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_sink_delivery {
    ($handler:expr) => {
        $handler
    };
    ($handler:expr, idempotent) => {
        ::obzenflow_runtime::stages::sink::DeclareDeliverySafety::declare_idempotent($handler)
    };
    ($handler:expr, non_idempotent) => {
        ::obzenflow_runtime::stages::sink::DeclareDeliverySafety::declare_non_idempotent($handler)
    };
    ($handler:expr, $other:ident) => {
        compile_error!("sink!: `delivery:` accepts `idempotent` or `non_idempotent`")
    };
}

/// Create a sink stage descriptor.
///
/// Canonical grammar: `InputType => handler_path`, then an optional
/// `delivery: idempotent | non_idempotent` safety clause, then an optional
/// named `middleware: [ ... ]` clause. Construct closure-tier `SinkTyped`
/// adapters and sink facades in ordinary Rust inside the materialiser, then
/// pass the resulting binding by path.
#[macro_export]
macro_rules! sink {
    // ── typed (binding-derived name): exact input ──
    ($in:ty => placeholder!() $(, middleware: [$($mw:expr),* $(,)?])?) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = "__obzenflow_binding_derived_name__", handler = placeholder!(), middleware = [$($($mw),*)?])
    };
    ($in:ty => placeholder!($msg:expr) $(, middleware: [$($mw:expr),* $(,)?])?) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = "__obzenflow_binding_derived_name__", handler = placeholder!($msg), middleware = [$($($mw),*)?])
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
    ($in:ty => sinks::$factory:ident($($args:tt)*) $(, middleware: [$($mw:expr),* $(,)?])?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "sink!",
            "let output = sinks::json::<Event>(); events = sink!(Event => output);"
        )
    };
    ($in:ty => $handler_head:ident $(:: $handler_tail:ident)*
        $(, delivery: $delivery:ident)?
        $(, middleware: [$($mw:expr),* $(,)?])?
    ) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = "__obzenflow_binding_derived_name__", handler = $crate::__obzenflow_sink_delivery!($handler_head $(:: $handler_tail)* $(, $delivery)?), middleware = [$($($mw),*)?])
    };

    // ── typed (explicit name override): exact input ──
    (name: $name:literal, $in:ty => placeholder!() $(, middleware: [$($mw:expr),* $(,)?])?) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = $name, handler = placeholder!(), middleware = [$($($mw),*)?])
    };
    (name: $name:literal, $in:ty => placeholder!($msg:expr) $(, middleware: [$($mw:expr),* $(,)?])?) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = $name, handler = placeholder!($msg), middleware = [$($($mw),*)?])
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
    (name: $name:literal, $in:ty => sinks::$factory:ident($($args:tt)*) $(, middleware: [$($mw:expr),* $(,)?])?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "sink!",
            "let output = sinks::json::<Event>(); events = sink!(name: \"events\", Event => output);"
        )
    };
    (name: $name:literal, $in:ty => $handler_head:ident $(:: $handler_tail:ident)*
        $(, delivery: $delivery:ident)?
        $(, middleware: [$($mw:expr),* $(,)?])?
    ) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = $name, handler = $crate::__obzenflow_sink_delivery!($handler_head $(:: $handler_tail)* $(, $delivery)?), middleware = [$($($mw),*)?])
    };

    // ── clause-order guardrails (FLOWIP-120s): `delivery:` precedes `middleware:` ──
    ($in:ty => $handler:expr, middleware: [$($mw:expr),* $(,)?], delivery: $delivery:ident) => {
        compile_error!("sink!: clause order is `delivery:` then `middleware:`")
    };
    (name: $name:literal, $in:ty => $handler:expr, middleware: [$($mw:expr),* $(,)?], delivery: $delivery:ident) => {
        compile_error!("sink!: clause order is `delivery:` then `middleware:`")
    };
    ($in:ty => $handler:expr, delivery: idempotent $(, middleware: [$($mw:expr),* $(,)?])?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "sink!",
            "let output = SinkTyped::new(...); events = sink!(Event => output);"
        )
    };
    ($in:ty => $handler:expr, delivery: non_idempotent $(, middleware: [$($mw:expr),* $(,)?])?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "sink!",
            "let output = SinkTyped::new(...); events = sink!(Event => output);"
        )
    };
    ($in:ty => $handler:expr, delivery: $other:ident $(, middleware: [$($mw:expr),* $(,)?])?) => {
        compile_error!("sink!: `delivery:` accepts `idempotent` or `non_idempotent`")
    };
    (name: $name:literal, $in:ty => $handler:expr, delivery: idempotent $(, middleware: [$($mw:expr),* $(,)?])?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "sink!",
            "let output = SinkTyped::new(...); events = sink!(name: \"events\", Event => output);"
        )
    };
    (name: $name:literal, $in:ty => $handler:expr, delivery: non_idempotent $(, middleware: [$($mw:expr),* $(,)?])?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "sink!",
            "let output = SinkTyped::new(...); events = sink!(name: \"events\", Event => output);"
        )
    };
    (name: $name:literal, $in:ty => $handler:expr, delivery: $other:ident $(, middleware: [$($mw:expr),* $(,)?])?) => {
        compile_error!("sink!: `delivery:` accepts `idempotent` or `non_idempotent`")
    };
    ($in:ty => $handler:expr
        $(, delivery: $delivery:ident)?
        $(, middleware: [$($mw:expr),* $(,)?])?
    ) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "sink!",
            "let output = SinkTyped::new(...); events = sink!(Event => output);"
        )
    };
    (name: $name:literal, $in:ty => $handler:expr
        $(, delivery: $delivery:ident)?
        $(, middleware: [$($mw:expr),* $(,)?])?
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
macro_rules! __obzenflow_stateful_untyped {
    (name = $name:literal, handler = $handler:expr, emit = none, middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        use $crate::dsl::stage_descriptor::StatefulDescriptor;
        let mut __desc = StatefulDescriptor::new($name, $handler)
            $(.with_middleware($mw))*;
        {
            #[allow(unused_mut)]
            let mut __bp: Option<$crate::dsl::backpressure_clause::BackpressureClause> = None;
            $($( __bp = Some($bp); )?)?
            __desc.backpressure = __bp;
        }
        __desc.build()
    }};
    (name = $name:literal, handler = $handler:expr, emit = some($emit_interval:expr), middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        use $crate::dsl::stage_descriptor::StatefulDescriptor;
        let mut __desc = StatefulDescriptor::new($name, $handler)
            .with_emit_interval($emit_interval)
            $(.with_middleware($mw))*;
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
macro_rules! __obzenflow_stateful_typed {
    // -- exact input, placeholder, explicit output contract, no emit --
    (input = exact($in:ty), output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!(), emit = none, middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::exact_payload::<$in>(),
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            true,
            None,
        )
        .with_additional_output_contract($crate::__obzenflow_output_contract_members!($($member),+));
        let __descriptor = $crate::__obzenflow_stateful_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderStateful::<$in, $out>::new(None),
            emit = none,
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // -- exact input, placeholder, explicit output contract, with emit --
    (input = exact($in:ty), output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!(), emit = some($emit_interval:expr), middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::exact_payload::<$in>(),
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            true,
            None,
        )
        .with_additional_output_contract($crate::__obzenflow_output_contract_members!($($member),+));
        let __descriptor = $crate::__obzenflow_stateful_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderStateful::<$in, $out>::new(None),
            emit = some($emit_interval),
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // -- exact input, placeholder msg, explicit output contract, no emit --
    (input = exact($in:ty), output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!($msg:expr), emit = none, middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::exact_payload::<$in>(),
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            true,
            Some(($msg).to_string()),
        )
        .with_additional_output_contract($crate::__obzenflow_output_contract_members!($($member),+));
        let __descriptor = $crate::__obzenflow_stateful_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderStateful::<$in, $out>::new(Some($msg)),
            emit = none,
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // -- exact input, placeholder msg, explicit output contract, with emit --
    (input = exact($in:ty), output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = placeholder!($msg:expr), emit = some($emit_interval:expr), middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::exact_payload::<$in>(),
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            true,
            Some(($msg).to_string()),
        )
        .with_additional_output_contract($crate::__obzenflow_output_contract_members!($($member),+));
        let __descriptor = $crate::__obzenflow_stateful_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderStateful::<$in, $out>::new(Some($msg)),
            emit = some($emit_interval),
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // -- exact input, real handler, explicit output contract, no emit --
    (input = exact($in:ty), output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = $handler:expr, emit = none, middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __handler = $handler;
        // FLOWIP-114c PR D: assert_stateful_contract dropped, see sink rationale.
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::exact_payload::<$in>(),
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            false,
            None,
        )
        .with_additional_output_contract($crate::__obzenflow_output_contract_members!($($member),+));
        let __descriptor = $crate::__obzenflow_stateful_untyped!(name = $name, handler = __handler, emit = none, middleware = [$($mw),*] $(, backpressure = [$($bp)?])?);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // -- exact input, real handler, explicit output contract, with emit --
    (input = exact($in:ty), output = $out:ty, output_contract = [$($member:ty),+ $(,)?], name = $name:literal, handler = $handler:expr, emit = some($emit_interval:expr), middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __handler = $handler;
        // FLOWIP-114c PR D: assert_stateful_contract dropped, see sink rationale.
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::exact_payload::<$in>(),
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            false,
            None,
        )
        .with_additional_output_contract($crate::__obzenflow_output_contract_members!($($member),+));
        let __descriptor = $crate::__obzenflow_stateful_untyped!(name = $name, handler = __handler, emit = some($emit_interval), middleware = [$($mw),*] $(, backpressure = [$($bp)?])?);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // ── exact input, placeholder, no emit ──
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = placeholder!(), emit = none, middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::exact_payload::<$in>(),
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            true,
            None,
        );
        let __descriptor = $crate::__obzenflow_stateful_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderStateful::<$in, $out>::new(None),
            emit = none,
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // ── exact input, placeholder, with emit ──
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = placeholder!(), emit = some($emit_interval:expr), middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::exact_payload::<$in>(),
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            true,
            None,
        );
        let __descriptor = $crate::__obzenflow_stateful_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderStateful::<$in, $out>::new(None),
            emit = some($emit_interval),
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // ── exact input, placeholder msg, no emit ──
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = placeholder!($msg:expr), emit = none, middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::exact_payload::<$in>(),
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::__obzenflow_stateful_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderStateful::<$in, $out>::new(Some($msg)),
            emit = none,
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // ── exact input, placeholder msg, with emit ──
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = placeholder!($msg:expr), emit = some($emit_interval:expr), middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::exact_payload::<$in>(),
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::__obzenflow_stateful_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderStateful::<$in, $out>::new(Some($msg)),
            emit = some($emit_interval),
            middleware = [$($mw),*]
            $(, backpressure = [$($bp)?])?
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // ── exact input, real handler, no emit ──
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = $handler:expr, emit = none, middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __handler = $handler;
        // FLOWIP-114c PR D: assert_stateful_contract dropped, see sink rationale.
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::exact_payload::<$in>(),
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            false,
            None,
        );
        let __descriptor = $crate::__obzenflow_stateful_untyped!(name = $name, handler = __handler, emit = none, middleware = [$($mw),*] $(, backpressure = [$($bp)?])?);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // ── exact input, real handler, with emit ──
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = $handler:expr, emit = some($emit_interval:expr), middleware = [$($mw:expr),*] $(, backpressure = [$($bp:expr)?])?) => {{
        let __handler = $handler;
        // FLOWIP-114c PR D: assert_stateful_contract dropped, see sink rationale.
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::exact_payload::<$in>(),
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            false,
            None,
        );
        let __descriptor = $crate::__obzenflow_stateful_untyped!(name = $name, handler = __handler, emit = some($emit_interval), middleware = [$($mw),*] $(, backpressure = [$($bp)?])?);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_stateful_exact_contract {
    (name = $name:literal, $($rest:tt)+) => {
        $crate::__obzenflow_stateful_exact_contract!(@collect name = $name, in = (), $($rest)+)
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
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!(), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
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
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
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
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!(), emit_interval = $emit_interval:expr, [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
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
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!($msg:expr), emit_interval = $emit_interval:expr, [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
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
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
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
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*, emit_interval = $emit_interval:expr, [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
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
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!(), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
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
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!($msg:expr), [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
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
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!(), emit_interval = $emit_interval:expr, [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
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
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!($msg:expr), emit_interval = $emit_interval:expr, [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
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
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler_head:ident $(:: $handler_tail:ident)*, [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
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
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler_head:ident $(:: $handler_tail:ident)*, emit_interval = $emit_interval:expr, [$($mw:expr),*] $(, backpressure: $bp:expr)?) => {
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
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => $handler:expr $(, emit_interval = $emit_interval:expr)? $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "stateful!",
            "let handler = MyStateful::new(...); output = stateful!(Input -> Output => handler);"
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler:expr $(, emit_interval = $emit_interval:expr)? $(, [$($mw:expr),*])? $(, backpressure: $bp:expr)?) => {
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
        let mut __desc = EffectfulStatefulDescriptor::new($name, $handler)
            .with_effect_declarations($crate::__obzenflow_effect_declarations_vec!($($effects)*))
            $(.with_middleware($mw))*;
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
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => $handler_head:ident $(:: $handler_tail:ident)*, effects: [$($effects:tt)*], middleware: [$($mw:expr),* $(,)?] $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_effectful_stateful_typed!(
            input = exact($($in)+),
            output = $first,
            output_contract = [$first $(, $member)*],
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            effects = [$($effects)*],
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler_head:ident $(:: $handler_tail:ident)*, effects: [$($effects:tt)*], middleware: [$($mw:expr),* $(,)?] $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_effectful_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = $handler_head $(:: $handler_tail)*,
            effects = [$($effects)*],
            middleware = [$($mw),*]
            $(, backpressure = [$bp])?
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> { $first:ty $(, $member:ty)* $(,)? } => $handler:expr, effects: [$($effects:tt)*], middleware: [$($mw:expr),* $(,)?] $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "effectful_stateful!",
            "let handler = MyEffectfulStateful::new(...); output = effectful_stateful!(Input -> Output => handler, effects: [...], middleware: [...]);"
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler:expr, effects: [$($effects:tt)*], middleware: [$($mw:expr),* $(,)?] $(, backpressure: $bp:expr)? $(,)?) => {
        $crate::__obzenflow_handler_path_diagnostic!(
            "effectful_stateful!",
            "let handler = MyEffectfulStateful::new(...); output = effectful_stateful!(Input -> Output => handler, effects: [...], middleware: [...]);"
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)*), $tok:tt $($rest:tt)+) => {
        $crate::__obzenflow_effectful_stateful_exact_contract!(
            @collect
            name = $name,
            in = ($($in)* $tok),
            $($rest)+
        )
    };
    (@collect name = $name:literal, in = (), -> $($rest:tt)*) => {
        compile_error!("effectful_stateful!: expected `InputType -> OutputType => handler`");
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
macro_rules! __obzenflow_join_untyped {
    (name = $name:literal, reference_stage_var = $ref_var:ident, handler = $handler:expr, middleware = [$($mw:expr),*]) => {{
        use $crate::dsl::stage_descriptor::{JoinDescriptor, StageDescriptor};
        use obzenflow_core::id::StageId;
        Box::new(JoinDescriptor {
            name: $name.to_string(),
            reference_stage_id: StageId::new(),
            reference_stage_var: Some(stringify!($ref_var)),
            handler: $handler,
            middleware: vec![$(Box::new($mw)),*],
        }) as Box<dyn StageDescriptor>
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_join_typed {
    // -- placeholder, explicit output contract --
    (reference = $ref_hint:tt, stream = $str_hint:tt, output = $out:ty,
     output_contract = [$($member:ty),+ $(,)?],
     ref_type = ($($ref_ty:ty)?), stream_type = ($($str_ty:ty)?),
     name = $name:literal, ref_var = $ref_var:ident, handler = placeholder!(),
     middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::join(
            $crate::__obzenflow_join_hint!($ref_hint $(, $ref_ty)?),
            $crate::__obzenflow_join_hint!($str_hint $(, $str_ty)?),
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            true,
            None,
        )
        .with_additional_output_contract($crate::__obzenflow_output_contract_members!($($member),+));
        let __descriptor = $crate::__obzenflow_join_untyped!(
            name = $name,
            reference_stage_var = $ref_var,
            handler = $crate::dsl::typing::PlaceholderJoin::<
                $crate::__obzenflow_join_phantom_type!($ref_hint $(, $ref_ty)?),
                $crate::__obzenflow_join_phantom_type!($str_hint $(, $str_ty)?),
                $out
            >::new(None),
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (reference = $ref_hint:tt, stream = $str_hint:tt, output = $out:ty,
     output_contract = [$($member:ty),+ $(,)?],
     ref_type = ($($ref_ty:ty)?), stream_type = ($($str_ty:ty)?),
     name = $name:literal, ref_var = $ref_var:ident, handler = placeholder!($msg:expr),
     middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::join(
            $crate::__obzenflow_join_hint!($ref_hint $(, $ref_ty)?),
            $crate::__obzenflow_join_hint!($str_hint $(, $str_ty)?),
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            true,
            Some(($msg).to_string()),
        )
        .with_additional_output_contract($crate::__obzenflow_output_contract_members!($($member),+));
        let __descriptor = $crate::__obzenflow_join_untyped!(
            name = $name,
            reference_stage_var = $ref_var,
            handler = $crate::dsl::typing::PlaceholderJoin::<
                $crate::__obzenflow_join_phantom_type!($ref_hint $(, $ref_ty)?),
                $crate::__obzenflow_join_phantom_type!($str_hint $(, $str_ty)?),
                $out
            >::new(Some($msg)),
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};

    // -- placeholder --
    (reference = $ref_hint:tt, stream = $str_hint:tt, output = $out:ty,
     ref_type = ($($ref_ty:ty)?), stream_type = ($($str_ty:ty)?),
     name = $name:literal, ref_var = $ref_var:ident, handler = placeholder!(),
     middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::join(
            $crate::__obzenflow_join_hint!($ref_hint $(, $ref_ty)?),
            $crate::__obzenflow_join_hint!($str_hint $(, $str_ty)?),
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            true,
            None,
        );
        let __descriptor = $crate::__obzenflow_join_untyped!(
            name = $name,
            reference_stage_var = $ref_var,
            handler = $crate::dsl::typing::PlaceholderJoin::<
                $crate::__obzenflow_join_phantom_type!($ref_hint $(, $ref_ty)?),
                $crate::__obzenflow_join_phantom_type!($str_hint $(, $str_ty)?),
                $out
            >::new(None),
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (reference = $ref_hint:tt, stream = $str_hint:tt, output = $out:ty,
     ref_type = ($($ref_ty:ty)?), stream_type = ($($str_ty:ty)?),
     name = $name:literal, ref_var = $ref_var:ident, handler = placeholder!($msg:expr),
     middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::join(
            $crate::__obzenflow_join_hint!($ref_hint $(, $ref_ty)?),
            $crate::__obzenflow_join_hint!($str_hint $(, $str_ty)?),
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::__obzenflow_join_untyped!(
            name = $name,
            reference_stage_var = $ref_var,
            handler = $crate::dsl::typing::PlaceholderJoin::<
                $crate::__obzenflow_join_phantom_type!($ref_hint $(, $ref_ty)?),
                $crate::__obzenflow_join_phantom_type!($str_hint $(, $str_ty)?),
                $out
            >::new(Some($msg)),
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};

    // ── real handler: both exact ──
    //
    // FLOWIP-114c PR D: __obzenflow_anchor_join JoinTyping bound dropped.
    // The metadata declares ref/stream/output types; the handler does not need
    // to implement JoinTyping itself, matching the BoundTransform tautology.
    (reference = exact, stream = exact, output = $out:ty,
     output_contract = [$($member:ty),+ $(,)?],
     ref_type = ($ref_ty:ty), stream_type = ($str_ty:ty),
     name = $name:literal, ref_var = $ref_var:ident, handler = $handler:expr,
     middleware = [$($mw:expr),*]) => {{
        let __handler = $handler;
        let __metadata = $crate::dsl::typing::StageTypingMetadata::join(
            $crate::dsl::typing::TypeHint::exact_payload::<$ref_ty>(),
            $crate::dsl::typing::TypeHint::exact_payload::<$str_ty>(),
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            false,
            None,
        )
        .with_additional_output_contract($crate::__obzenflow_output_contract_members!($($member),+));
        let __descriptor = $crate::__obzenflow_join_untyped!(
            name = $name,
            reference_stage_var = $ref_var,
            handler = __handler,
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};

    (reference = exact, stream = exact, output = $out:ty,
     ref_type = ($ref_ty:ty), stream_type = ($str_ty:ty),
     name = $name:literal, ref_var = $ref_var:ident, handler = $handler:expr,
     middleware = [$($mw:expr),*]) => {{
        let __handler = $handler;
        let __metadata = $crate::dsl::typing::StageTypingMetadata::join(
            $crate::dsl::typing::TypeHint::exact_payload::<$ref_ty>(),
            $crate::dsl::typing::TypeHint::exact_payload::<$str_ty>(),
            $crate::dsl::typing::TypeHint::exact_payload::<$out>(),
            false,
            None,
        );
        let __descriptor = $crate::__obzenflow_join_untyped!(
            name = $name,
            reference_stage_var = $ref_var,
            handler = __handler,
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};

}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_join_hint {
    (exact, $ty:ty) => {
        $crate::dsl::typing::TypeHint::exact_payload::<$ty>()
    };
    (exact) => {
        compile_error!(
            "__obzenflow_join_hint!(exact) requires a type; this is a bug in the macro dispatch"
        )
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_join_phantom_type {
    (exact, $ty:ty) => {
        $ty
    };
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
     -> { $first:ty $(, $member:ty)* $(,)? } => placeholder!(),
     [$($mw:expr),*]) => {
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
     [$($mw:expr),*]) => {
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
     [$($mw:expr),*]) => {
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
     $(, [$($mw:expr),*])?) => {
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
     [$($mw:expr),*]) => {
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
     [$($mw:expr),*]) => {
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
     [$($mw:expr),*]) => {
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
     $(, [$($mw:expr),*])?) => {
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
        $crate::dsl::ai_effect::clone_inference_chat_contract::<_, $binding>(&$binding)
    }};
    ($surface:literal, $binding:ident) => {
        $crate::dsl::ai_effect::clone_chat_contract(&$binding)
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_ai_chat_effect_row {
    (
        surface = $surface:tt,
        row = {
            at_least_once(ChatCompletion)
                via $binding:ident
                with { retry($($retry:tt)*) $(,)? }
            $(,)?
        }
    ) => {
        compile_error!(concat!(
            $surface,
            ": ChatCompletion is NonIdempotentAtLeastOnce; retry is forbidden"
        ))
    };
    (
        surface = $surface:tt,
        row = {
            at_least_once(ChatCompletion)
                via $binding:ident
                with { $first:expr, $($rest:expr),+ $(,)? }
            $(,)?
        }
    ) => {
        compile_error!(
            "AI effect rows accept exactly one EffectResilience policy; \
             multi-policy composition belongs to FLOWIP-132b"
        )
    };
    (
        surface = $surface:tt,
        row = {
            at_least_once(ChatCompletion)
                via $binding:ident
                with { $policy:expr $(,)? }
            $(,)?
        }
    ) => {{
        let __chat_binding: ::obzenflow_core::ai::ChatBindingContract =
            $crate::__obzenflow_clone_ai_chat_contract!($surface, $binding);
        let __chat_policy: Box<dyn ::obzenflow_adapters::middleware::MiddlewareFactory> = $policy;
        (__chat_binding, __chat_policy)
    }};
    (
        surface = $surface:tt,
        row = { ChatCompletion $($rest:tt)* }
    ) => {
        compile_error!(concat!(
            $surface,
            ": paid non-idempotent ChatCompletion requires \
             `at_least_once(ChatCompletion)` acknowledgement"
        ))
    };
    (
        surface = $surface:tt,
        row = { transactional(ChatCompletion) $($rest:tt)* }
    ) => {
        compile_error!(concat!(
            $surface,
            ": ChatCompletion accepts only `at_least_once(ChatCompletion)`"
        ))
    };
    (
        surface = $surface:tt,
        row = { $($invalid:tt)* }
    ) => {
        compile_error!(concat!(
            $surface,
            ": expected `at_least_once(ChatCompletion) via <chat binding> \
             with { <EffectResilience> }`"
        ))
    };
}

/// Preserve effect-row diagnostics before reporting a non-path AI role.
///
/// Before FLOWIP-133a, an expression role reached effect-row parsing. The
/// teaching fallback must therefore validate the row first so narrowing the
/// role slot does not mask an existing, more specific diagnostic.
#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_ai_effect_row_syntax_then {
    (
        surface = $surface:tt,
        row = {
            at_least_once(ChatCompletion)
                via $binding:ident
                with { retry($($retry:tt)*) $(,)? }
            $(,)?
        },
        then = { $($then:tt)* }
    ) => {
        compile_error!(concat!(
            $surface,
            ": ChatCompletion is NonIdempotentAtLeastOnce; retry is forbidden"
        ))
    };
    (
        surface = $surface:tt,
        row = {
            at_least_once(ChatCompletion)
                via $binding:ident
                with { $first:expr, $($rest:expr),+ $(,)? }
            $(,)?
        },
        then = { $($then:tt)* }
    ) => {
        compile_error!(
            "AI effect rows accept exactly one EffectResilience policy; \
             multi-policy composition belongs to FLOWIP-132b"
        )
    };
    (
        surface = $surface:tt,
        row = {
            at_least_once(ChatCompletion)
                via $binding:ident
                with { $policy:expr $(,)? }
            $(,)?
        },
        then = { $($then:tt)* }
    ) => {
        $($then)*
    };
    (
        surface = $surface:tt,
        row = { ChatCompletion $($rest:tt)* },
        then = { $($then:tt)* }
    ) => {
        compile_error!(concat!(
            $surface,
            ": paid non-idempotent ChatCompletion requires \
             `at_least_once(ChatCompletion)` acknowledgement"
        ))
    };
    (
        surface = $surface:tt,
        row = { transactional(ChatCompletion) $($rest:tt)* },
        then = { $($then:tt)* }
    ) => {
        compile_error!(concat!(
            $surface,
            ": ChatCompletion accepts only `at_least_once(ChatCompletion)`"
        ))
    };
    (
        surface = $surface:tt,
        row = { $($invalid:tt)* },
        then = { $($then:tt)* }
    ) => {
        compile_error!(concat!(
            $surface,
            ": expected `at_least_once(ChatCompletion) via <chat binding> \
             with { <EffectResilience> }`"
        ))
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_inference_contract {
    (
        name = $name:literal,
        input = ($($input:tt)+),
        -> { $($row:tt)* } $out:ty => $role_head:ident $(:: $role_tail:ident)*
        $(,)?
    ) => {{
        let (__chat_binding, __chat_policy) =
            $crate::__obzenflow_ai_chat_effect_row!(
                surface = "inference!",
                row = { $($row)* }
            );
        $crate::dsl::inference::generated_inference::<$($input)+, $out, _>(
            $name,
            $role_head $(:: $role_tail)*,
            __chat_binding,
            __chat_policy,
        )
    }};
    (
        name = $name:literal,
        input = ($($input:tt)+),
        -> { $($row:tt)* } $out:ty => $role_head:ident $(:: $role_tail:ident)*,
        chunking: $($chunking:tt)*
    ) => {
        compile_error!(
            "inference!: `chunking` is not supported; use `ai_map!` or `ai_map_reduce!`"
        )
    };
    (
        name = $name:literal,
        input = ($($input:tt)+),
        -> { $($row:tt)* } $out:ty => $role:expr,
        chunking: $($chunking:tt)*
    ) => {
        compile_error!(
            "inference!: `chunking` is not supported; use `ai_map!` or `ai_map_reduce!`"
        )
    };
    (
        name = $name:literal,
        input = ($($input:tt)+),
        -> { $($row:tt)* } $out:ty => $role:expr
        $(,)?
    ) => {
        $crate::__obzenflow_ai_effect_row_syntax_then!(
            surface = "inference!",
            row = { $($row)* },
            then = {
                $crate::__obzenflow_handler_path_diagnostic!(
                    "inference!",
                    "let role = MyRole::new(...); answer = inference!(Input -> { ... } Output => role);"
                )
            }
        )
    };
    (
        name = $name:literal,
        input = ($($input:tt)*),
        -> $($rest:tt)+
    ) => {
        compile_error!(
            "inference!: expected `Input -> { at_least_once(ChatCompletion) \
             via <chat binding> with { <EffectResilience> } } Output => role`"
        )
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
        map_chat = ($map_chat:expr),
        finalise_chat = ($finalise_chat:expr),
        map_policy = ($map_policy:expr),
        finalise_policy = ($finalise_policy:expr)
    ) => {{
        $crate::dsl::composites::ai_map_reduce::generated_map_reduce::<
            $($seed_ty)+,
            $item_ty,
            $partial_ty,
            $out_ty,
            _,
            _,
            _,
        >(
            $name,
            ($chunker, $map_role, $finalise_role),
            ($map_chat, $finalise_chat),
            (vec![$map_policy], vec![$finalise_policy]),
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
        let (__map_chat, __map_policy) =
            $crate::__obzenflow_ai_chat_effect_row!(
                surface = "ai_map_reduce!",
                row = { $($map_row)* }
            );
        let (__finalise_chat, __finalise_policy) =
            $crate::__obzenflow_ai_chat_effect_row!(
                surface = "ai_map_reduce!",
                row = { $($finalise_row)* }
            );
        let __chunker = $crate::__obzenflow_ai_map_reduce_chunker_by_budget!(
            seed_type = ($($seed_ty)+),
            item_type = ($item_ty),
            estimator: __map_chat.estimator().estimator(),
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
            map_chat = (__map_chat),
            finalise_chat = (__finalise_chat),
            map_policy = (__map_policy),
            finalise_policy = (__finalise_policy)
        )
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_ai_map_reduce_generated_contract {
    (
        name = $name:literal,
        seed = ($($seed_ty:tt)+),
        -> $out_ty:ty => {
            map: [$item_ty:ty] -> { $($map_row:tt)* } $partial_ty:ty => $map_role_head:ident $(:: $map_role_tail:ident)*,
            reduce: ($reduce_seed_ty:ty, [$reduce_partial_ty:ty])
                -> { $($finalise_row:tt)* } $reduce_out_ty:ty => $finalise_role_head:ident $(:: $finalise_role_tail:ident)*
                $(,)?
        },
        chunking: by_budget { $($chunking:tt)+ }
        $(,)?
    ) => {
        $crate::__obzenflow_ai_map_reduce_build!(
            name = $name,
            seed_type = ($($seed_ty)+),
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
        name = $name:literal,
        seed = ($($seed_ty:tt)+),
        -> $out_ty:ty => {
            map: [$item_ty:ty] -> { $($map_row:tt)* } $partial_ty:ty => $map_role:expr,
            reduce: ($reduce_seed_ty:ty, [$reduce_partial_ty:ty])
                -> { $($finalise_row:tt)* } $reduce_out_ty:ty => $finalise_role_head:ident $(:: $finalise_role_tail:ident)*
                $(,)?
        },
        chunking: by_budget { $($chunking:tt)+ }
        $(,)?
    ) => {
        $crate::__obzenflow_ai_effect_row_syntax_then!(
            surface = "ai_map_reduce!",
            row = { $($map_row)* },
            then = {
                $crate::__obzenflow_ai_effect_row_syntax_then!(
                    surface = "ai_map_reduce!",
                    row = { $($finalise_row)* },
                    then = {
                        $crate::__obzenflow_handler_path_diagnostic!(
                            "ai_map_reduce! map role",
                            "let map_role = MyMapRole::new(...); result = ai_map_reduce!(Seed -> Out => { map: [Item] -> { ... } Partial => map_role, ... }, chunking: ...);"
                        )
                    }
                )
            }
        )
    };
    (
        name = $name:literal,
        seed = ($($seed_ty:tt)+),
        -> $out_ty:ty => {
            map: [$item_ty:ty] -> { $($map_row:tt)* } $partial_ty:ty => $map_role_head:ident $(:: $map_role_tail:ident)*,
            reduce: ($reduce_seed_ty:ty, [$reduce_partial_ty:ty])
                -> { $($finalise_row:tt)* } $reduce_out_ty:ty => $finalise_role:expr
                $(,)?
        },
        chunking: by_budget { $($chunking:tt)+ }
        $(,)?
    ) => {
        $crate::__obzenflow_ai_effect_row_syntax_then!(
            surface = "ai_map_reduce!",
            row = { $($map_row)* },
            then = {
                $crate::__obzenflow_ai_effect_row_syntax_then!(
                    surface = "ai_map_reduce!",
                    row = { $($finalise_row)* },
                    then = {
                        $crate::__obzenflow_handler_path_diagnostic!(
                            "ai_map_reduce! reduce role",
                            "let reduce_role = MyReduceRole::new(...); result = ai_map_reduce!(Seed -> Out => { ..., reduce: (Seed, [Partial]) -> { ... } Out => reduce_role }, chunking: ...);"
                        )
                    }
                )
            }
        )
    };
    (
        name = $name:literal,
        seed = ($($seed_ty:tt)+),
        -> $out_ty:ty => {
            map: [$item_ty:ty] -> { $($map_row:tt)* } $partial_ty:ty => $map_role:expr,
            reduce: ($reduce_seed_ty:ty, [$reduce_partial_ty:ty])
                -> { $($finalise_row:tt)* } $reduce_out_ty:ty => $finalise_role:expr
                $(,)?
        },
        chunking: by_budget { $($chunking:tt)+ }
        $(,)?
    ) => {
        $crate::__obzenflow_ai_effect_row_syntax_then!(
            surface = "ai_map_reduce!",
            row = { $($map_row)* },
            then = {
                $crate::__obzenflow_ai_effect_row_syntax_then!(
                    surface = "ai_map_reduce!",
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
    (
        name = $name:literal,
        seed = ($($seed_ty:tt)+),
        -> $out_ty:ty => { $($roles:tt)* },
        chunking: by_budget { $($chunking:tt)* },
        effects: { $($effects:tt)* }
        $(,)?
    ) => {
        compile_error!(
            "ai_map_reduce!: `effects: { ... }` was replaced by role-local effect rows"
        )
    };
    (
        name = $name:literal,
        seed = ($($seed:tt)*),
        -> $($rest:tt)+
    ) => {
        compile_error!(
            "ai_map_reduce!: expected role-local `-> { at_least_once(ChatCompletion) \
             via <chat binding> with { <EffectResilience> } }` rows on map and reduce"
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
        // Full user-facing chain: source! -> typed -> untyped with the clause.
        let descriptor = crate::source!(
            name: "s",
            TestFact => placeholder!(),
            [],
            backpressure: enforced(1000)
        );
        assert_eq!(descriptor.name(), "s");
        assert!(
            descriptor.backpressure_clause().is_some(),
            "the clause survives the typing wrapper"
        );
        let inner = crate::__obzenflow_source_untyped!(
            name = "s_inner",
            handler = crate::dsl::typing::PlaceholderFiniteSource::<TestFact>::new(None),
            middleware = [],
            backpressure = [enforced(1000)]
        );
        assert!(inner.backpressure_clause().is_some());
    }

    #[test]
    fn source_macro_without_clause_leaves_backpressure_none() {
        let inner = crate::__obzenflow_source_untyped!(
            name = "s_none",
            handler = crate::dsl::typing::PlaceholderFiniteSource::<TestFact>::new(None),
            middleware = []
        );
        assert!(inner.backpressure_clause().is_none());
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
