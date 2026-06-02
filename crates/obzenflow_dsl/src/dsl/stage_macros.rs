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
//! stateful!(In -> Out => handler)
//! sink!(In => handler)
//! join!(catalog CatalogStage: Catalog, Stream -> Out => handler)
//! ```
//!
//! Each form supports four decorations: bare (binding-derived name), the
//! `name: "..."` prefix to override the runtime stage name, a `[middleware,
//! ...]` suffix to attach middleware, and the named-plus-middleware
//! combination. The 8 stage families x 4 decorations matrix is exercised as a
//! permanent regression by `typed_decoration_matrix_test`.
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
//! descriptor assembly; those are not part of the public surface and are not
//! reachable by author code.

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

// ============================================================================
// source!  +  __obzenflow_source_typed!
// ============================================================================

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_source_typed {
    (output = $out:ty, name = $name:literal, handler = placeholder!(), middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact::<$out>(),
            true,
            None,
        );
        let __descriptor = $crate::__obzenflow_source_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderFiniteSource::<$out>::new(None),
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (output = $out:ty, name = $name:literal, handler = placeholder!($msg:expr), middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact::<$out>(),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::__obzenflow_source_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderFiniteSource::<$out>::new(Some($msg)),
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (output = $out:ty, name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*]) => {{
        let __handler = $handler;
        // FLOWIP-114c PR D: assert_source_output dropped, see sink rationale.
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact::<$out>(),
            false,
            None,
        );
        let __descriptor = $crate::__obzenflow_source_untyped!(
            name = $name,
            handler = __handler,
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_source_untyped {
    (name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*]) => {{
        use $crate::dsl::stage_descriptor::{FiniteSourceDescriptor, StageDescriptor};
        Box::new(FiniteSourceDescriptor {
            name: $name.to_string(),
            handler: $handler,
            middleware: vec![$(Box::new($mw)),*],
        }) as Box<dyn StageDescriptor>
    }};
}

/// Create a finite source stage descriptor.
#[macro_export]
macro_rules! source {
    // ── typed (binding-derived name) ──
    ($out:ty => placeholder!()) => {
        $crate::__obzenflow_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!(),
            middleware = []
        )
    };
    ($out:ty => placeholder!($msg:expr)) => {
        $crate::__obzenflow_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!($msg),
            middleware = []
        )
    };
    ($out:ty => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!(),
            middleware = [$($mw),*]
        )
    };
    ($out:ty => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!($msg),
            middleware = [$($mw),*]
        )
    };
    ($out:ty => $handler:expr) => {
        $crate::__obzenflow_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = $handler,
            middleware = []
        )
    };
    ($out:ty => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = $handler,
            middleware = [$($mw),*]
        )
    };

    // ── typed (explicit name override) ──
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
    (name: $name:literal, $out:ty => $handler:expr) => {
        $crate::__obzenflow_source_typed!(output = $out, name = $name, handler = $handler, middleware = [])
    };
    (name: $name:literal, $out:ty => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_source_typed!(output = $out, name = $name, handler = $handler, middleware = [$($mw),*])
    };
}

// ============================================================================
// async_source!  +  __obzenflow_async_source_typed!
// ============================================================================

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_async_source_untyped {
    (name = $name:literal, handler = ($handler:expr, $poll_timeout:expr), middleware = [$($mw:expr),*]) => {{
        $crate::dsl::stage_descriptor::AsyncFiniteSourceDescriptor::new($name, $handler)
            .with_poll_timeout($poll_timeout)
            $(.with_middleware($mw))*
            .build()
    }};
    (name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*]) => {{
        $crate::dsl::stage_descriptor::AsyncFiniteSourceDescriptor::new($name, $handler)
            $(.with_middleware($mw))*
            .build()
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_async_source_typed {
    (output = $out:ty, name = $name:literal, handler = placeholder!(), middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact::<$out>(),
            true,
            None,
        );
        let __descriptor = $crate::__obzenflow_async_source_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderAsyncSource::<$out>::new(None),
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (output = $out:ty, name = $name:literal, handler = placeholder!($msg:expr), middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact::<$out>(),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::__obzenflow_async_source_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderAsyncSource::<$out>::new(Some($msg)),
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (output = $out:ty, name = $name:literal, handler = ($handler:expr, $poll_timeout:expr), middleware = [$($mw:expr),*]) => {{
        let __handler = $handler;
        // FLOWIP-114c PR D: assert_source_output dropped, see sink rationale.
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact::<$out>(),
            false,
            None,
        );
        let __descriptor = $crate::__obzenflow_async_source_untyped!(
            name = $name,
            handler = (__handler, $poll_timeout),
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (output = $out:ty, name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*]) => {{
        let __handler = $handler;
        // FLOWIP-114c PR D: assert_source_output dropped, see sink rationale.
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact::<$out>(),
            false,
            None,
        );
        let __descriptor = $crate::__obzenflow_async_source_untyped!(
            name = $name,
            handler = __handler,
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
}

/// Create an async finite source stage descriptor.
#[macro_export]
macro_rules! async_source {
    // ── typed (binding-derived name) ──
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
    ($out:ty => ($handler:expr, $poll_timeout:expr)) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = ($handler, $poll_timeout), middleware = [])
    };
    ($out:ty => ($handler:expr, $poll_timeout:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = ($handler, $poll_timeout), middleware = [$($mw),*])
    };
    ($out:ty => $handler:expr) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = $handler, middleware = [])
    };
    ($out:ty => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = "__obzenflow_binding_derived_name__", handler = $handler, middleware = [$($mw),*])
    };

    // ── typed (explicit name override) ──
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
    (name: $name:literal, $out:ty => ($handler:expr, $poll_timeout:expr)) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = ($handler, $poll_timeout), middleware = [])
    };
    (name: $name:literal, $out:ty => ($handler:expr, $poll_timeout:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = ($handler, $poll_timeout), middleware = [$($mw),*])
    };
    (name: $name:literal, $out:ty => $handler:expr) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = $handler, middleware = [])
    };
    (name: $name:literal, $out:ty => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = $handler, middleware = [$($mw),*])
    };
}

// ============================================================================
// infinite_source!  +  __obzenflow_infinite_source_typed!
// ============================================================================

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_infinite_source_untyped {
    (name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*]) => {{
        use $crate::dsl::stage_descriptor::{InfiniteSourceDescriptor, StageDescriptor};
        Box::new(InfiniteSourceDescriptor {
            name: $name.to_string(),
            handler: $handler,
            middleware: vec![$(Box::new($mw)),*],
        }) as Box<dyn StageDescriptor>
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_infinite_source_typed {
    (output = $out:ty, name = $name:literal, handler = placeholder!(), middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact::<$out>(),
            true,
            None,
        );
        let __descriptor = $crate::__obzenflow_infinite_source_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderInfiniteSource::<$out>::new(None),
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (output = $out:ty, name = $name:literal, handler = placeholder!($msg:expr), middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact::<$out>(),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::__obzenflow_infinite_source_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderInfiniteSource::<$out>::new(Some($msg)),
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (output = $out:ty, name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*]) => {{
        let __handler = $handler;
        // FLOWIP-114c PR D: assert_source_output dropped, see sink rationale.
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact::<$out>(),
            false,
            None,
        );
        let __descriptor = $crate::__obzenflow_infinite_source_untyped!(
            name = $name,
            handler = __handler,
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
}

/// Create an infinite source stage descriptor.
#[macro_export]
macro_rules! infinite_source {
    // ── typed (binding-derived name) ──
    ($out:ty => placeholder!()) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!(),
            middleware = []
        )
    };
    ($out:ty => placeholder!($msg:expr)) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!($msg),
            middleware = []
        )
    };
    ($out:ty => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!(),
            middleware = [$($mw),*]
        )
    };
    ($out:ty => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!($msg),
            middleware = [$($mw),*]
        )
    };
    ($out:ty => $handler:expr) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = $handler,
            middleware = []
        )
    };
    ($out:ty => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = $handler,
            middleware = [$($mw),*]
        )
    };

    // ── typed (explicit name override) ──
    (name: $name:literal, $out:ty => placeholder!()) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = placeholder!(),
            middleware = []
        )
    };
    (name: $name:literal, $out:ty => placeholder!($msg:expr)) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = placeholder!($msg),
            middleware = []
        )
    };
    (name: $name:literal, $out:ty => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = placeholder!(),
            middleware = [$($mw),*]
        )
    };
    (name: $name:literal, $out:ty => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = placeholder!($msg),
            middleware = [$($mw),*]
        )
    };
    (name: $name:literal, $out:ty => $handler:expr) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = $handler,
            middleware = []
        )
    };
    (name: $name:literal, $out:ty => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = $handler,
            middleware = [$($mw),*]
        )
    };
}

// ============================================================================
// async_infinite_source!  +  __obzenflow_async_infinite_source_typed!
// ============================================================================

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_async_infinite_source_untyped {
    (name = $name:literal, handler = ($handler:expr, $poll_timeout:expr), middleware = [$($mw:expr),*]) => {{
        $crate::dsl::stage_descriptor::AsyncInfiniteSourceDescriptor::new($name, $handler)
            .with_poll_timeout($poll_timeout)
            $(.with_middleware($mw))*
            .build()
    }};
    (name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*]) => {{
        $crate::dsl::stage_descriptor::AsyncInfiniteSourceDescriptor::new($name, $handler)
            $(.with_middleware($mw))*
            .build()
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_async_infinite_source_typed {
    (output = $out:ty, name = $name:literal, handler = placeholder!(), middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact::<$out>(),
            true,
            None,
        );
        let __descriptor = $crate::__obzenflow_async_infinite_source_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderAsyncSource::<$out>::new(None),
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (output = $out:ty, name = $name:literal, handler = placeholder!($msg:expr), middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact::<$out>(),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::__obzenflow_async_infinite_source_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderAsyncSource::<$out>::new(Some($msg)),
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (output = $out:ty, name = $name:literal, handler = ($handler:expr, $poll_timeout:expr), middleware = [$($mw:expr),*]) => {{
        let __handler = $handler;
        // FLOWIP-114c PR D: assert_source_output dropped, see sink rationale.
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact::<$out>(),
            false,
            None,
        );
        let __descriptor = $crate::__obzenflow_async_infinite_source_untyped!(
            name = $name,
            handler = (__handler, $poll_timeout),
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (output = $out:ty, name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*]) => {{
        let __handler = $handler;
        // FLOWIP-114c PR D: assert_source_output dropped, see sink rationale.
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact::<$out>(),
            false,
            None,
        );
        let __descriptor = $crate::__obzenflow_async_infinite_source_untyped!(
            name = $name,
            handler = __handler,
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
}

/// Create an async infinite source stage descriptor.
#[macro_export]
macro_rules! async_infinite_source {
    // ── typed (binding-derived name) ──
    ($out:ty => placeholder!()) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!(),
            middleware = []
        )
    };
    ($out:ty => placeholder!($msg:expr)) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!($msg),
            middleware = []
        )
    };
    ($out:ty => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!(),
            middleware = [$($mw),*]
        )
    };
    ($out:ty => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!($msg),
            middleware = [$($mw),*]
        )
    };
    ($out:ty => ($handler:expr, $poll_timeout:expr)) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = ($handler, $poll_timeout),
            middleware = []
        )
    };
    ($out:ty => ($handler:expr, $poll_timeout:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = ($handler, $poll_timeout),
            middleware = [$($mw),*]
        )
    };
    ($out:ty => $handler:expr) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = $handler,
            middleware = []
        )
    };
    ($out:ty => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = $handler,
            middleware = [$($mw),*]
        )
    };

    // ── typed (explicit name override) ──
    (name: $name:literal, $out:ty => placeholder!()) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = placeholder!(),
            middleware = []
        )
    };
    (name: $name:literal, $out:ty => placeholder!($msg:expr)) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = placeholder!($msg),
            middleware = []
        )
    };
    (name: $name:literal, $out:ty => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = placeholder!(),
            middleware = [$($mw),*]
        )
    };
    (name: $name:literal, $out:ty => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = placeholder!($msg),
            middleware = [$($mw),*]
        )
    };
    (name: $name:literal, $out:ty => ($handler:expr, $poll_timeout:expr)) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = ($handler, $poll_timeout),
            middleware = []
        )
    };
    (name: $name:literal, $out:ty => ($handler:expr, $poll_timeout:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = ($handler, $poll_timeout),
            middleware = [$($mw),*]
        )
    };
    (name: $name:literal, $out:ty => $handler:expr) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = $handler,
            middleware = []
        )
    };
    (name: $name:literal, $out:ty => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_async_infinite_source_typed!(
            output = $out,
            name = $name,
            handler = $handler,
            middleware = [$($mw),*]
        )
    };
}

// ============================================================================
// transform!  +  __obzenflow_transform_typed!
// ============================================================================

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_transform_untyped {
    (name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*]) => {{
        use $crate::dsl::stage_descriptor::{StageDescriptor, TransformDescriptor};
        Box::new(TransformDescriptor {
            name: $name.to_string(),
            handler: $handler,
            middleware: vec![$(Box::new($mw)),*],
        }) as Box<dyn StageDescriptor>
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_transform_typed {
    // ── exact input, placeholder ──
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = placeholder!(), middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::exact::<$in>(),
            $crate::dsl::typing::TypeHint::exact::<$out>(),
            true,
            None,
        );
        let __descriptor = $crate::__obzenflow_transform_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderTransform::<$in, $out>::new(None),
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = placeholder!($msg:expr), middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::exact::<$in>(),
            $crate::dsl::typing::TypeHint::exact::<$out>(),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::__obzenflow_transform_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderTransform::<$in, $out>::new(Some($msg)),
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // ── exact input, real handler ──
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*]) => {{
        let __handler = $handler;
        let __handler =
            $crate::dsl::typing::BoundTransform::<$in, $out, _>::new(__handler);
        ::obzenflow_runtime::typing::assert_transform_contract::<_, $in, $out>(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::exact::<$in>(),
            $crate::dsl::typing::TypeHint::exact::<$out>(),
            false,
            None,
        );
        let __descriptor = $crate::__obzenflow_transform_untyped!(name = $name, handler = __handler, middleware = [$($mw),*]);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_transform_exact_contract {
    (name = $name:literal, $($rest:tt)+) => {
        $crate::__obzenflow_transform_exact_contract!(@collect name = $name, in = (), $($rest)+)
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!()) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!(),
            middleware = []
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!($msg:expr)) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!($msg),
            middleware = []
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!(),
            middleware = [$($mw),*]
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!($msg),
            middleware = [$($mw),*]
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler:expr) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = $handler,
            middleware = []
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_transform_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = $handler,
            middleware = [$($mw),*]
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
    (name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*]) => {{
        use $crate::dsl::stage_descriptor::{AsyncTransformDescriptor, StageDescriptor};
        Box::new(AsyncTransformDescriptor {
            name: $name.to_string(),
            handler: $handler,
            middleware: vec![$(Box::new($mw)),*],
        }) as Box<dyn StageDescriptor>
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_async_transform_typed {
    // ── exact input, placeholder ──
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = placeholder!(), middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::exact::<$in>(),
            $crate::dsl::typing::TypeHint::exact::<$out>(),
            true,
            None,
        );
        let __descriptor = $crate::__obzenflow_async_transform_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderAsyncTransform::<$in, $out>::new(None),
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = placeholder!($msg:expr), middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::exact::<$in>(),
            $crate::dsl::typing::TypeHint::exact::<$out>(),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::__obzenflow_async_transform_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderAsyncTransform::<$in, $out>::new(Some($msg)),
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // ── exact input, real handler ──
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*]) => {{
        let __handler = $handler;
        let __handler =
            $crate::dsl::typing::BoundAsyncTransform::<$in, $out, _>::new(__handler);
        ::obzenflow_runtime::typing::assert_transform_contract::<_, $in, $out>(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::exact::<$in>(),
            $crate::dsl::typing::TypeHint::exact::<$out>(),
            false,
            None,
        );
        let __descriptor = $crate::__obzenflow_async_transform_untyped!(name = $name, handler = __handler, middleware = [$($mw),*]);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_async_transform_exact_contract {
    (name = $name:literal, $($rest:tt)+) => {
        $crate::__obzenflow_async_transform_exact_contract!(@collect name = $name, in = (), $($rest)+)
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!()) => {
        $crate::__obzenflow_async_transform_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!(),
            middleware = []
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!($msg:expr)) => {
        $crate::__obzenflow_async_transform_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!($msg),
            middleware = []
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_transform_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!(),
            middleware = [$($mw),*]
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_transform_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!($msg),
            middleware = [$($mw),*]
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler:expr) => {
        $crate::__obzenflow_async_transform_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = $handler,
            middleware = []
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_async_transform_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = $handler,
            middleware = [$($mw),*]
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
// effectful_async_transform!
// ============================================================================

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_effectful_async_transform_untyped {
    (name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*]) => {{
        use $crate::dsl::stage_descriptor::{EffectfulAsyncTransformDescriptor, StageDescriptor};
        Box::new(EffectfulAsyncTransformDescriptor {
            name: $name.to_string(),
            handler: $handler,
            middleware: vec![$(Box::new($mw)),*],
        }) as Box<dyn StageDescriptor>
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_effectful_async_transform_typed {
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*]) => {{
        let __handler = $handler;
        fn __assert_effectful_contract<H>(_handler: &H)
        where
            H: ::obzenflow_runtime::stages::EffectfulAsyncTransformHandler<
                Input = $in,
                Output = $out,
            >,
        {}
        __assert_effectful_contract(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::exact::<$in>(),
            $crate::dsl::typing::TypeHint::exact::<$out>(),
            false,
            None,
        );
        let __descriptor = $crate::__obzenflow_effectful_async_transform_untyped!(
            name = $name,
            handler = __handler,
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_effectful_async_transform_exact_contract {
    (name = $name:literal, $($rest:tt)+) => {
        $crate::__obzenflow_effectful_async_transform_exact_contract!(@collect name = $name, in = (), $($rest)+)
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler:expr) => {
        $crate::__obzenflow_effectful_async_transform_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = $handler,
            middleware = []
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_effectful_async_transform_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = $handler,
            middleware = [$($mw),*]
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)*), $tok:tt $($rest:tt)+) => {
        $crate::__obzenflow_effectful_async_transform_exact_contract!(
            @collect
            name = $name,
            in = ($($in)* $tok),
            $($rest)+
        )
    };
    (@collect name = $name:literal, in = (), -> $($rest:tt)*) => {
        compile_error!("effectful_async_transform!: expected `InputType -> OutputType => handler`");
    };
    (@collect name = $name:literal, in = ($($in:tt)+), $($rest:tt)*) => {
        compile_error!("effectful_async_transform!: expected `-> OutputType => handler` after input type");
    };
}

#[macro_export]
macro_rules! effectful_async_transform {
    (name: $name:literal, $($rest:tt)+) => {
        $crate::__obzenflow_effectful_async_transform_exact_contract!(name = $name, $($rest)+)
    };
    ($($rest:tt)+) => {
        $crate::__obzenflow_effectful_async_transform_exact_contract!(
            name = "__obzenflow_binding_derived_name__",
            $($rest)+
        )
    };
}

// ============================================================================
// effectful_sink!
// ============================================================================

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_effectful_sink_untyped {
    (name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*]) => {{
        use $crate::dsl::stage_descriptor::{EffectfulSinkDescriptor, StageDescriptor};
        Box::new(EffectfulSinkDescriptor {
            name: $name.to_string(),
            handler: $handler,
            middleware: vec![$(Box::new($mw)),*],
        }) as Box<dyn StageDescriptor>
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_effectful_sink_typed {
    (input = exact($in:ty), name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*]) => {{
        let __handler = $handler;
        fn __assert_effectful_sink_contract<H>(_handler: &H)
        where
            H: ::obzenflow_runtime::stages::EffectfulAsyncSinkHandler<Input = $in>,
        {}
        __assert_effectful_sink_contract(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::sink(
            $crate::dsl::typing::TypeHint::exact::<$in>(),
            false,
            None,
        );
        let __descriptor = $crate::__obzenflow_effectful_sink_untyped!(
            name = $name,
            handler = __handler,
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_effectful_sink_exact_contract {
    (name = $name:literal, $($rest:tt)+) => {
        $crate::__obzenflow_effectful_sink_exact_contract!(@collect name = $name, in = (), $($rest)+)
    };
    (@collect name = $name:literal, in = ($($in:tt)+), => $handler:expr) => {
        $crate::__obzenflow_effectful_sink_typed!(
            input = exact($($in)+),
            name = $name,
            handler = $handler,
            middleware = []
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_effectful_sink_typed!(
            input = exact($($in)+),
            name = $name,
            handler = $handler,
            middleware = [$($mw),*]
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)*), $tok:tt $($rest:tt)+) => {
        $crate::__obzenflow_effectful_sink_exact_contract!(
            @collect
            name = $name,
            in = ($($in)* $tok),
            $($rest)+
        )
    };
    (@collect name = $name:literal, in = (), => $($rest:tt)*) => {
        compile_error!("effectful_sink!: expected `InputType => handler`");
    };
    (@collect name = $name:literal, in = ($($in:tt)+), $($rest:tt)*) => {
        compile_error!("effectful_sink!: expected `=> handler` after input type");
    };
}

#[macro_export]
macro_rules! effectful_sink {
    (name: $name:literal, $($rest:tt)+) => {
        $crate::__obzenflow_effectful_sink_exact_contract!(name = $name, $($rest)+)
    };
    ($($rest:tt)+) => {
        $crate::__obzenflow_effectful_sink_exact_contract!(
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
    (name = $name:literal, handler = |$arg:ident : $ty:ty| $body:block, middleware = [$($mw:expr),*]) => {{
        use $crate::dsl::stage_descriptor::{SinkDescriptor, StageDescriptor};
        Box::new(SinkDescriptor {
            name: $name.to_string(),
            handler: ::obzenflow_runtime::stages::sink::SinkTyped::new(move |$arg: $ty| {
                $body;
                async move {}
            }),
            middleware: vec![$(Box::new($mw)),*],
        }) as Box<dyn StageDescriptor>
    }};
    (name = $name:literal, handler = move |$arg:ident : $ty:ty| $body:block, middleware = [$($mw:expr),*]) => {{
        use $crate::dsl::stage_descriptor::{SinkDescriptor, StageDescriptor};
        Box::new(SinkDescriptor {
            name: $name.to_string(),
            handler: ::obzenflow_runtime::stages::sink::SinkTyped::new(move |$arg: $ty| {
                $body;
                async move {}
            }),
            middleware: vec![$(Box::new($mw)),*],
        }) as Box<dyn StageDescriptor>
    }};
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
            $crate::dsl::typing::TypeHint::exact::<$in>(),
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
            $crate::dsl::typing::TypeHint::exact::<$in>(),
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
    (input = exact($in:ty), name = $name:literal, handler = sinks::console($formatter:expr $(,)?), middleware = [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(
            input = exact($in),
            name = $name,
            handler = sinks::console::<$in, _>($formatter),
            middleware = [$($mw),*]
        )
    };
    (input = exact($in:ty), name = $name:literal, handler = sinks::table($columns:expr, $extractor:expr $(,)?), middleware = [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(
            input = exact($in),
            name = $name,
            handler = sinks::table::<$in, _>($columns, $extractor),
            middleware = [$($mw),*]
        )
    };
    (input = exact($in:ty), name = $name:literal, handler = sinks::json(), middleware = [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(
            input = exact($in),
            name = $name,
            handler = sinks::json::<$in>(),
            middleware = [$($mw),*]
        )
    };
    (input = exact($in:ty), name = $name:literal, handler = sinks::json_pretty(), middleware = [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(
            input = exact($in),
            name = $name,
            handler = sinks::json_pretty::<$in>(),
            middleware = [$($mw),*]
        )
    };
    (input = exact($in:ty), name = $name:literal, handler = sinks::debug(), middleware = [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(
            input = exact($in),
            name = $name,
            handler = sinks::debug::<$in>(),
            middleware = [$($mw),*]
        )
    };

    // ── exact input, real handler ──
    //
    // FLOWIP-114c PR D: the previous `assert_sink_input::<_, $in>` check is dropped.
    // Per the proposal's canonical-identity rationale, the declared input is a
    // topology fingerprint, not a Rust type-system constraint, matching the
    // tautological pattern already used by `BoundTransform` wrappers.
    (input = exact($in:ty), name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*]) => {{
        let __handler = $handler;
        let __metadata = $crate::dsl::typing::StageTypingMetadata::sink(
            $crate::dsl::typing::TypeHint::exact::<$in>(),
            false,
            None,
        );
        let __descriptor = $crate::__obzenflow_sink_untyped!(name = $name, handler = __handler, middleware = [$($mw),*]);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
}

/// Create a sink stage descriptor.
#[macro_export]
macro_rules! sink {
    // ── typed (binding-derived name): closure shorthand ──
    (|$arg:ident : $ty:ty| $body:block) => {
        $crate::__obzenflow_sink_typed!(
            input = exact($ty),
            name = "__obzenflow_binding_derived_name__",
            handler = ::obzenflow_runtime::stages::sink::SinkTyped::new(|$arg: $ty| async move $body),
            middleware = []
        )
    };
    (move |$arg:ident : $ty:ty| $body:block) => {
        $crate::__obzenflow_sink_typed!(
            input = exact($ty),
            name = "__obzenflow_binding_derived_name__",
            handler = ::obzenflow_runtime::stages::sink::SinkTyped::new(move |$arg: $ty| async move $body),
            middleware = []
        )
    };
    (|$arg:ident : $ty:ty| $body:block, [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(
            input = exact($ty),
            name = "__obzenflow_binding_derived_name__",
            handler = ::obzenflow_runtime::stages::sink::SinkTyped::new(|$arg: $ty| async move $body),
            middleware = [$($mw),*]
        )
    };
    (move |$arg:ident : $ty:ty| $body:block, [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(
            input = exact($ty),
            name = "__obzenflow_binding_derived_name__",
            handler = ::obzenflow_runtime::stages::sink::SinkTyped::new(move |$arg: $ty| async move $body),
            middleware = [$($mw),*]
        )
    };

    // ── typed (binding-derived name): exact input ──
    ($in:ty => placeholder!()) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = "__obzenflow_binding_derived_name__", handler = placeholder!(), middleware = [])
    };
    ($in:ty => placeholder!($msg:expr)) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = "__obzenflow_binding_derived_name__", handler = placeholder!($msg), middleware = [])
    };
    ($in:ty => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = "__obzenflow_binding_derived_name__", handler = placeholder!(), middleware = [$($mw),*])
    };
    ($in:ty => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = "__obzenflow_binding_derived_name__", handler = placeholder!($msg), middleware = [$($mw),*])
    };
    ($in:ty => sinks::console($formatter:expr $(,)?)) => {
        $crate::__obzenflow_sink_typed!(
            input = exact($in),
            name = "__obzenflow_binding_derived_name__",
            handler = sinks::console::<$in, _>($formatter),
            middleware = []
        )
    };
    ($in:ty => sinks::console($formatter:expr $(,)?), [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(
            input = exact($in),
            name = "__obzenflow_binding_derived_name__",
            handler = sinks::console::<$in, _>($formatter),
            middleware = [$($mw),*]
        )
    };
    ($in:ty => sinks::table($columns:expr, $extractor:expr $(,)?)) => {
        $crate::__obzenflow_sink_typed!(
            input = exact($in),
            name = "__obzenflow_binding_derived_name__",
            handler = sinks::table::<$in, _>($columns, $extractor),
            middleware = []
        )
    };
    ($in:ty => sinks::table($columns:expr, $extractor:expr $(,)?), [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(
            input = exact($in),
            name = "__obzenflow_binding_derived_name__",
            handler = sinks::table::<$in, _>($columns, $extractor),
            middleware = [$($mw),*]
        )
    };
    ($in:ty => sinks::json()) => {
        $crate::__obzenflow_sink_typed!(
            input = exact($in),
            name = "__obzenflow_binding_derived_name__",
            handler = sinks::json::<$in>(),
            middleware = []
        )
    };
    ($in:ty => sinks::json(), [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(
            input = exact($in),
            name = "__obzenflow_binding_derived_name__",
            handler = sinks::json::<$in>(),
            middleware = [$($mw),*]
        )
    };
    ($in:ty => sinks::json_pretty()) => {
        $crate::__obzenflow_sink_typed!(
            input = exact($in),
            name = "__obzenflow_binding_derived_name__",
            handler = sinks::json_pretty::<$in>(),
            middleware = []
        )
    };
    ($in:ty => sinks::json_pretty(), [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(
            input = exact($in),
            name = "__obzenflow_binding_derived_name__",
            handler = sinks::json_pretty::<$in>(),
            middleware = [$($mw),*]
        )
    };
    ($in:ty => sinks::debug()) => {
        $crate::__obzenflow_sink_typed!(
            input = exact($in),
            name = "__obzenflow_binding_derived_name__",
            handler = sinks::debug::<$in>(),
            middleware = []
        )
    };
    ($in:ty => sinks::debug(), [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(
            input = exact($in),
            name = "__obzenflow_binding_derived_name__",
            handler = sinks::debug::<$in>(),
            middleware = [$($mw),*]
        )
    };
    ($in:ty => $handler:expr) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = "__obzenflow_binding_derived_name__", handler = $handler, middleware = [])
    };
    ($in:ty => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = "__obzenflow_binding_derived_name__", handler = $handler, middleware = [$($mw),*])
    };

    // ── typed (explicit name override): closure shorthand ──
    (name: $name:literal, |$arg:ident : $ty:ty| $body:block) => {
        $crate::__obzenflow_sink_typed!(
            input = exact($ty),
            name = $name,
            handler = ::obzenflow_runtime::stages::sink::SinkTyped::new(|$arg: $ty| async move $body),
            middleware = []
        )
    };
    (name: $name:literal, move |$arg:ident : $ty:ty| $body:block) => {
        $crate::__obzenflow_sink_typed!(
            input = exact($ty),
            name = $name,
            handler = ::obzenflow_runtime::stages::sink::SinkTyped::new(move |$arg: $ty| async move $body),
            middleware = []
        )
    };
    (name: $name:literal, |$arg:ident : $ty:ty| $body:block, [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(
            input = exact($ty),
            name = $name,
            handler = ::obzenflow_runtime::stages::sink::SinkTyped::new(|$arg: $ty| async move $body),
            middleware = [$($mw),*]
        )
    };
    (name: $name:literal, move |$arg:ident : $ty:ty| $body:block, [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(
            input = exact($ty),
            name = $name,
            handler = ::obzenflow_runtime::stages::sink::SinkTyped::new(move |$arg: $ty| async move $body),
            middleware = [$($mw),*]
        )
    };

    // ── typed (explicit name override): exact input ──
    (name: $name:literal, $in:ty => placeholder!()) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = $name, handler = placeholder!(), middleware = [])
    };
    (name: $name:literal, $in:ty => placeholder!($msg:expr)) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = $name, handler = placeholder!($msg), middleware = [])
    };
    (name: $name:literal, $in:ty => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = $name, handler = placeholder!(), middleware = [$($mw),*])
    };
    (name: $name:literal, $in:ty => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = $name, handler = placeholder!($msg), middleware = [$($mw),*])
    };
    (name: $name:literal, $in:ty => sinks::console($formatter:expr $(,)?)) => {
        $crate::__obzenflow_sink_typed!(
            input = exact($in),
            name = $name,
            handler = sinks::console::<$in, _>($formatter),
            middleware = []
        )
    };
    (name: $name:literal, $in:ty => sinks::console($formatter:expr $(,)?), [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(
            input = exact($in),
            name = $name,
            handler = sinks::console::<$in, _>($formatter),
            middleware = [$($mw),*]
        )
    };
    (name: $name:literal, $in:ty => sinks::table($columns:expr, $extractor:expr $(,)?)) => {
        $crate::__obzenflow_sink_typed!(
            input = exact($in),
            name = $name,
            handler = sinks::table::<$in, _>($columns, $extractor),
            middleware = []
        )
    };
    (name: $name:literal, $in:ty => sinks::table($columns:expr, $extractor:expr $(,)?), [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(
            input = exact($in),
            name = $name,
            handler = sinks::table::<$in, _>($columns, $extractor),
            middleware = [$($mw),*]
        )
    };
    (name: $name:literal, $in:ty => sinks::json()) => {
        $crate::__obzenflow_sink_typed!(
            input = exact($in),
            name = $name,
            handler = sinks::json::<$in>(),
            middleware = []
        )
    };
    (name: $name:literal, $in:ty => sinks::json(), [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(
            input = exact($in),
            name = $name,
            handler = sinks::json::<$in>(),
            middleware = [$($mw),*]
        )
    };
    (name: $name:literal, $in:ty => sinks::json_pretty()) => {
        $crate::__obzenflow_sink_typed!(
            input = exact($in),
            name = $name,
            handler = sinks::json_pretty::<$in>(),
            middleware = []
        )
    };
    (name: $name:literal, $in:ty => sinks::json_pretty(), [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(
            input = exact($in),
            name = $name,
            handler = sinks::json_pretty::<$in>(),
            middleware = [$($mw),*]
        )
    };
    (name: $name:literal, $in:ty => sinks::debug()) => {
        $crate::__obzenflow_sink_typed!(
            input = exact($in),
            name = $name,
            handler = sinks::debug::<$in>(),
            middleware = []
        )
    };
    (name: $name:literal, $in:ty => sinks::debug(), [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(
            input = exact($in),
            name = $name,
            handler = sinks::debug::<$in>(),
            middleware = [$($mw),*]
        )
    };
    (name: $name:literal, $in:ty => $handler:expr) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = $name, handler = $handler, middleware = [])
    };
    (name: $name:literal, $in:ty => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = $name, handler = $handler, middleware = [$($mw),*])
    };
}

// ============================================================================
// stateful!  +  __obzenflow_stateful_typed!
// ============================================================================

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_stateful_untyped {
    (name = $name:literal, handler = $handler:expr, emit = none, middleware = [$($mw:expr),*]) => {{
        use $crate::dsl::stage_descriptor::StatefulDescriptor;
        StatefulDescriptor::new($name, $handler)
            $(.with_middleware($mw))*
            .build()
    }};
    (name = $name:literal, handler = $handler:expr, emit = some($emit_interval:expr), middleware = [$($mw:expr),*]) => {{
        use $crate::dsl::stage_descriptor::StatefulDescriptor;
        StatefulDescriptor::new($name, $handler)
            .with_emit_interval($emit_interval)
            $(.with_middleware($mw))*
            .build()
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_stateful_typed {
    // ── exact input, placeholder, no emit ──
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = placeholder!(), emit = none, middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::exact::<$in>(),
            $crate::dsl::typing::TypeHint::exact::<$out>(),
            true,
            None,
        );
        let __descriptor = $crate::__obzenflow_stateful_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderStateful::<$in, $out>::new(None),
            emit = none,
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // ── exact input, placeholder, with emit ──
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = placeholder!(), emit = some($emit_interval:expr), middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::exact::<$in>(),
            $crate::dsl::typing::TypeHint::exact::<$out>(),
            true,
            None,
        );
        let __descriptor = $crate::__obzenflow_stateful_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderStateful::<$in, $out>::new(None),
            emit = some($emit_interval),
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // ── exact input, placeholder msg, no emit ──
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = placeholder!($msg:expr), emit = none, middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::exact::<$in>(),
            $crate::dsl::typing::TypeHint::exact::<$out>(),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::__obzenflow_stateful_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderStateful::<$in, $out>::new(Some($msg)),
            emit = none,
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // ── exact input, placeholder msg, with emit ──
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = placeholder!($msg:expr), emit = some($emit_interval:expr), middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::exact::<$in>(),
            $crate::dsl::typing::TypeHint::exact::<$out>(),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::__obzenflow_stateful_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderStateful::<$in, $out>::new(Some($msg)),
            emit = some($emit_interval),
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // ── exact input, real handler, no emit ──
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = $handler:expr, emit = none, middleware = [$($mw:expr),*]) => {{
        let __handler = $handler;
        // FLOWIP-114c PR D: assert_stateful_contract dropped, see sink rationale.
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::exact::<$in>(),
            $crate::dsl::typing::TypeHint::exact::<$out>(),
            false,
            None,
        );
        let __descriptor = $crate::__obzenflow_stateful_untyped!(name = $name, handler = __handler, emit = none, middleware = [$($mw),*]);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // ── exact input, real handler, with emit ──
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = $handler:expr, emit = some($emit_interval:expr), middleware = [$($mw:expr),*]) => {{
        let __handler = $handler;
        // FLOWIP-114c PR D: assert_stateful_contract dropped, see sink rationale.
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::exact::<$in>(),
            $crate::dsl::typing::TypeHint::exact::<$out>(),
            false,
            None,
        );
        let __descriptor = $crate::__obzenflow_stateful_untyped!(name = $name, handler = __handler, emit = some($emit_interval), middleware = [$($mw),*]);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_stateful_exact_contract {
    (name = $name:literal, $($rest:tt)+) => {
        $crate::__obzenflow_stateful_exact_contract!(@collect name = $name, in = (), $($rest)+)
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!()) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!(),
            emit = none,
            middleware = []
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!($msg:expr)) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!($msg),
            emit = none,
            middleware = []
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!(),
            emit = none,
            middleware = [$($mw),*]
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!($msg),
            emit = none,
            middleware = [$($mw),*]
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!(), emit_interval = $emit_interval:expr) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!(),
            emit = some($emit_interval),
            middleware = []
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!($msg:expr), emit_interval = $emit_interval:expr) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!($msg),
            emit = some($emit_interval),
            middleware = []
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!(), emit_interval = $emit_interval:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!(),
            emit = some($emit_interval),
            middleware = [$($mw),*]
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => placeholder!($msg:expr), emit_interval = $emit_interval:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = placeholder!($msg),
            emit = some($emit_interval),
            middleware = [$($mw),*]
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler:expr) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = $handler,
            emit = none,
            middleware = []
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = $handler,
            emit = none,
            middleware = [$($mw),*]
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler:expr, emit_interval = $emit_interval:expr) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = $handler,
            emit = some($emit_interval),
            middleware = []
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler:expr, emit_interval = $emit_interval:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = $handler,
            emit = some($emit_interval),
            middleware = [$($mw),*]
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
    (name = $name:literal, handler = $handler:expr, emit = none, middleware = [$($mw:expr),*]) => {{
        use $crate::dsl::stage_descriptor::EffectfulStatefulDescriptor;
        EffectfulStatefulDescriptor::new($name, $handler)
            $(.with_middleware($mw))*
            .build()
    }};
    (name = $name:literal, handler = $handler:expr, emit = some($emit_interval:expr), middleware = [$($mw:expr),*]) => {{
        use $crate::dsl::stage_descriptor::EffectfulStatefulDescriptor;
        EffectfulStatefulDescriptor::new($name, $handler)
            .with_emit_interval($emit_interval)
            $(.with_middleware($mw))*
            .build()
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_effectful_stateful_typed {
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = $handler:expr, emit = none, middleware = [$($mw:expr),*]) => {{
        let __handler = $handler;
        fn __assert_effectful_stateful_contract<H>(_handler: &H)
        where
            H: ::obzenflow_runtime::stages::EffectfulStatefulHandler<Input = $in, Output = $out>,
        {}
        __assert_effectful_stateful_contract(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::exact::<$in>(),
            $crate::dsl::typing::TypeHint::exact::<$out>(),
            false,
            None,
        );
        let __descriptor = $crate::__obzenflow_effectful_stateful_untyped!(
            name = $name,
            handler = __handler,
            emit = none,
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = $handler:expr, emit = some($emit_interval:expr), middleware = [$($mw:expr),*]) => {{
        let __handler = $handler;
        fn __assert_effectful_stateful_contract<H>(_handler: &H)
        where
            H: ::obzenflow_runtime::stages::EffectfulStatefulHandler<Input = $in, Output = $out>,
        {}
        __assert_effectful_stateful_contract(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::exact::<$in>(),
            $crate::dsl::typing::TypeHint::exact::<$out>(),
            false,
            None,
        );
        let __descriptor = $crate::__obzenflow_effectful_stateful_untyped!(
            name = $name,
            handler = __handler,
            emit = some($emit_interval),
            middleware = [$($mw),*]
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
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler:expr) => {
        $crate::__obzenflow_effectful_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = $handler,
            emit = none,
            middleware = []
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_effectful_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = $handler,
            emit = none,
            middleware = [$($mw),*]
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler:expr, emit_interval = $emit_interval:expr) => {
        $crate::__obzenflow_effectful_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = $handler,
            emit = some($emit_interval),
            middleware = []
        )
    };
    (@collect name = $name:literal, in = ($($in:tt)+), -> $out:ty => $handler:expr, emit_interval = $emit_interval:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_effectful_stateful_typed!(
            input = exact($($in)+),
            output = $out,
            name = $name,
            handler = $handler,
            emit = some($emit_interval),
            middleware = [$($mw),*]
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
    // ── placeholder ──
    (reference = $ref_hint:tt, stream = $str_hint:tt, output = $out:ty,
     ref_type = ($($ref_ty:ty)?), stream_type = ($($str_ty:ty)?),
     name = $name:literal, ref_var = $ref_var:ident, handler = placeholder!(),
     middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::join(
            $crate::__obzenflow_join_hint!($ref_hint $(, $ref_ty)?),
            $crate::__obzenflow_join_hint!($str_hint $(, $str_ty)?),
            $crate::dsl::typing::TypeHint::exact::<$out>(),
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
            $crate::dsl::typing::TypeHint::exact::<$out>(),
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

    // ── real handler: both exact (facade call anchoring) ──
    //
    // Rust cannot always infer the generic parameters of helper *functions* like
    // `joins::inner(...)` from trait bounds on an opaque `impl JoinTyping` return.
    // Anchor those helpers by injecting the stage-contract types in the expansion.
    (reference = exact, stream = exact, output = $out:ty,
     ref_type = ($ref_ty:ty), stream_type = ($str_ty:ty),
     name = $name:literal, ref_var = $ref_var:ident, handler = joins::inner($catalog_key:expr, $stream_key:expr, $join_fn:expr $(,)?),
     middleware = [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = exact,
            stream = exact,
            output = $out,
            ref_type = ($ref_ty),
            stream_type = ($str_ty),
            name = $name,
            ref_var = $ref_var,
            handler = joins::inner::<$ref_ty, $str_ty, $out, _, _, _, _>($catalog_key, $stream_key, $join_fn),
            middleware = [$($mw),*]
        )
    };
    (reference = exact, stream = exact, output = $out:ty,
     ref_type = ($ref_ty:ty), stream_type = ($str_ty:ty),
     name = $name:literal, ref_var = $ref_var:ident, handler = joins::inner_live($catalog_key:expr, $stream_key:expr, $join_fn:expr $(,)?),
     middleware = [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = exact,
            stream = exact,
            output = $out,
            ref_type = ($ref_ty),
            stream_type = ($str_ty),
            name = $name,
            ref_var = $ref_var,
            handler = joins::inner_live::<$ref_ty, $str_ty, $out, _, _, _, _>($catalog_key, $stream_key, $join_fn),
            middleware = [$($mw),*]
        )
    };
    (reference = exact, stream = exact, output = $out:ty,
     ref_type = ($ref_ty:ty), stream_type = ($str_ty:ty),
     name = $name:literal, ref_var = $ref_var:ident, handler = joins::left($catalog_key:expr, $stream_key:expr, $join_fn:expr $(,)?),
     middleware = [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = exact,
            stream = exact,
            output = $out,
            ref_type = ($ref_ty),
            stream_type = ($str_ty),
            name = $name,
            ref_var = $ref_var,
            handler = joins::left::<$ref_ty, $str_ty, $out, _, _, _, _>($catalog_key, $stream_key, $join_fn),
            middleware = [$($mw),*]
        )
    };
    (reference = exact, stream = exact, output = $out:ty,
     ref_type = ($ref_ty:ty), stream_type = ($str_ty:ty),
     name = $name:literal, ref_var = $ref_var:ident, handler = joins::left_live($catalog_key:expr, $stream_key:expr, $join_fn:expr $(,)?),
     middleware = [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = exact,
            stream = exact,
            output = $out,
            ref_type = ($ref_ty),
            stream_type = ($str_ty),
            name = $name,
            ref_var = $ref_var,
            handler = joins::left_live::<$ref_ty, $str_ty, $out, _, _, _, _>($catalog_key, $stream_key, $join_fn),
            middleware = [$($mw),*]
        )
    };
    (reference = exact, stream = exact, output = $out:ty,
     ref_type = ($ref_ty:ty), stream_type = ($str_ty:ty),
     name = $name:literal, ref_var = $ref_var:ident, handler = joins::strict($catalog_key:expr, $stream_key:expr, $join_fn:expr $(,)?),
     middleware = [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = exact,
            stream = exact,
            output = $out,
            ref_type = ($ref_ty),
            stream_type = ($str_ty),
            name = $name,
            ref_var = $ref_var,
            handler = joins::strict::<$ref_ty, $str_ty, $out, _, _, _, _>($catalog_key, $stream_key, $join_fn),
            middleware = [$($mw),*]
        )
    };
    (reference = exact, stream = exact, output = $out:ty,
     ref_type = ($ref_ty:ty), stream_type = ($str_ty:ty),
     name = $name:literal, ref_var = $ref_var:ident, handler = joins::strict_live($catalog_key:expr, $stream_key:expr, $join_fn:expr $(,)?),
     middleware = [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = exact,
            stream = exact,
            output = $out,
            ref_type = ($ref_ty),
            stream_type = ($str_ty),
            name = $name,
            ref_var = $ref_var,
            handler = joins::strict_live::<$ref_ty, $str_ty, $out, _, _, _, _>($catalog_key, $stream_key, $join_fn),
            middleware = [$($mw),*]
        )
    };

    // ── real handler: both exact ──
    //
    // FLOWIP-114c PR D: __obzenflow_anchor_join JoinTyping bound dropped.
    // The metadata declares ref/stream/output types; the handler does not need
    // to implement JoinTyping itself, matching the BoundTransform tautology.
    (reference = exact, stream = exact, output = $out:ty,
     ref_type = ($ref_ty:ty), stream_type = ($str_ty:ty),
     name = $name:literal, ref_var = $ref_var:ident, handler = $handler:expr,
     middleware = [$($mw:expr),*]) => {{
        let __handler = $handler;
        let __metadata = $crate::dsl::typing::StageTypingMetadata::join(
            $crate::dsl::typing::TypeHint::exact::<$ref_ty>(),
            $crate::dsl::typing::TypeHint::exact::<$str_ty>(),
            $crate::dsl::typing::TypeHint::exact::<$out>(),
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
        $crate::dsl::typing::TypeHint::exact::<$ty>()
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
     reference = exact,
     ref_type = ($reference:ty),
     stream = ($($stream:tt)+),
     -> $out:ty => joins::inner($catalog_key:expr, $stream_key:expr, $join_fn:expr $(,)?)) => {
        $crate::__obzenflow_join_typed!(
            reference = exact,
            stream = exact,
            output = $out,
            ref_type = ($reference),
            stream_type = ($($stream)+),
            name = $name,
            ref_var = $ref_var,
            handler = joins::inner::<$reference, $($stream)+, $out, _, _, _, _>(
                $catalog_key,
                $stream_key,
                $join_fn
            ),
            middleware = []
        )
    };
    (@collect
     name = $name:literal,
     ref_var = $ref_var:ident,
     reference = exact,
     ref_type = ($reference:ty),
     stream = ($($stream:tt)+),
     -> $out:ty => joins::inner_live($catalog_key:expr, $stream_key:expr, $join_fn:expr $(,)?)) => {
        $crate::__obzenflow_join_typed!(
            reference = exact,
            stream = exact,
            output = $out,
            ref_type = ($reference),
            stream_type = ($($stream)+),
            name = $name,
            ref_var = $ref_var,
            handler = joins::inner_live::<$reference, $($stream)+, $out, _, _, _, _>(
                $catalog_key,
                $stream_key,
                $join_fn
            ),
            middleware = []
        )
    };
    (@collect
     name = $name:literal,
     ref_var = $ref_var:ident,
     reference = exact,
     ref_type = ($reference:ty),
     stream = ($($stream:tt)+),
     -> $out:ty => joins::left($catalog_key:expr, $stream_key:expr, $join_fn:expr $(,)?)) => {
        $crate::__obzenflow_join_typed!(
            reference = exact,
            stream = exact,
            output = $out,
            ref_type = ($reference),
            stream_type = ($($stream)+),
            name = $name,
            ref_var = $ref_var,
            handler = joins::left::<$reference, $($stream)+, $out, _, _, _, _>(
                $catalog_key,
                $stream_key,
                $join_fn
            ),
            middleware = []
        )
    };
    (@collect
     name = $name:literal,
     ref_var = $ref_var:ident,
     reference = exact,
     ref_type = ($reference:ty),
     stream = ($($stream:tt)+),
     -> $out:ty => joins::left_live($catalog_key:expr, $stream_key:expr, $join_fn:expr $(,)?)) => {
        $crate::__obzenflow_join_typed!(
            reference = exact,
            stream = exact,
            output = $out,
            ref_type = ($reference),
            stream_type = ($($stream)+),
            name = $name,
            ref_var = $ref_var,
            handler = joins::left_live::<$reference, $($stream)+, $out, _, _, _, _>(
                $catalog_key,
                $stream_key,
                $join_fn
            ),
            middleware = []
        )
    };
    (@collect
     name = $name:literal,
     ref_var = $ref_var:ident,
     reference = exact,
     ref_type = ($reference:ty),
     stream = ($($stream:tt)+),
     -> $out:ty => joins::strict($catalog_key:expr, $stream_key:expr, $join_fn:expr $(,)?)) => {
        $crate::__obzenflow_join_typed!(
            reference = exact,
            stream = exact,
            output = $out,
            ref_type = ($reference),
            stream_type = ($($stream)+),
            name = $name,
            ref_var = $ref_var,
            handler = joins::strict::<$reference, $($stream)+, $out, _, _, _, _>(
                $catalog_key,
                $stream_key,
                $join_fn
            ),
            middleware = []
        )
    };
    (@collect
     name = $name:literal,
     ref_var = $ref_var:ident,
     reference = exact,
     ref_type = ($reference:ty),
     stream = ($($stream:tt)+),
     -> $out:ty => joins::strict_live($catalog_key:expr, $stream_key:expr, $join_fn:expr $(,)?)) => {
        $crate::__obzenflow_join_typed!(
            reference = exact,
            stream = exact,
            output = $out,
            ref_type = ($reference),
            stream_type = ($($stream)+),
            name = $name,
            ref_var = $ref_var,
            handler = joins::strict_live::<$reference, $($stream)+, $out, _, _, _, _>(
                $catalog_key,
                $stream_key,
                $join_fn
            ),
            middleware = []
        )
    };
    (@collect
     name = $name:literal,
     ref_var = $ref_var:ident,
     reference = exact,
     ref_type = ($reference:ty),
     stream = ($($stream:tt)+),
     -> $out:ty => joins::inner($catalog_key:expr, $stream_key:expr, $join_fn:expr $(,)?),
     [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = exact,
            stream = exact,
            output = $out,
            ref_type = ($reference),
            stream_type = ($($stream)+),
            name = $name,
            ref_var = $ref_var,
            handler = joins::inner::<$reference, $($stream)+, $out, _, _, _, _>(
                $catalog_key,
                $stream_key,
                $join_fn
            ),
            middleware = [$($mw),*]
        )
    };
    (@collect
     name = $name:literal,
     ref_var = $ref_var:ident,
     reference = exact,
     ref_type = ($reference:ty),
     stream = ($($stream:tt)+),
     -> $out:ty => joins::inner_live($catalog_key:expr, $stream_key:expr, $join_fn:expr $(,)?),
     [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = exact,
            stream = exact,
            output = $out,
            ref_type = ($reference),
            stream_type = ($($stream)+),
            name = $name,
            ref_var = $ref_var,
            handler = joins::inner_live::<$reference, $($stream)+, $out, _, _, _, _>(
                $catalog_key,
                $stream_key,
                $join_fn
            ),
            middleware = [$($mw),*]
        )
    };
    (@collect
     name = $name:literal,
     ref_var = $ref_var:ident,
     reference = exact,
     ref_type = ($reference:ty),
     stream = ($($stream:tt)+),
     -> $out:ty => joins::left($catalog_key:expr, $stream_key:expr, $join_fn:expr $(,)?),
     [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = exact,
            stream = exact,
            output = $out,
            ref_type = ($reference),
            stream_type = ($($stream)+),
            name = $name,
            ref_var = $ref_var,
            handler = joins::left::<$reference, $($stream)+, $out, _, _, _, _>(
                $catalog_key,
                $stream_key,
                $join_fn
            ),
            middleware = [$($mw),*]
        )
    };
    (@collect
     name = $name:literal,
     ref_var = $ref_var:ident,
     reference = exact,
     ref_type = ($reference:ty),
     stream = ($($stream:tt)+),
     -> $out:ty => joins::left_live($catalog_key:expr, $stream_key:expr, $join_fn:expr $(,)?),
     [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = exact,
            stream = exact,
            output = $out,
            ref_type = ($reference),
            stream_type = ($($stream)+),
            name = $name,
            ref_var = $ref_var,
            handler = joins::left_live::<$reference, $($stream)+, $out, _, _, _, _>(
                $catalog_key,
                $stream_key,
                $join_fn
            ),
            middleware = [$($mw),*]
        )
    };
    (@collect
     name = $name:literal,
     ref_var = $ref_var:ident,
     reference = exact,
     ref_type = ($reference:ty),
     stream = ($($stream:tt)+),
     -> $out:ty => joins::strict($catalog_key:expr, $stream_key:expr, $join_fn:expr $(,)?),
     [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = exact,
            stream = exact,
            output = $out,
            ref_type = ($reference),
            stream_type = ($($stream)+),
            name = $name,
            ref_var = $ref_var,
            handler = joins::strict::<$reference, $($stream)+, $out, _, _, _, _>(
                $catalog_key,
                $stream_key,
                $join_fn
            ),
            middleware = [$($mw),*]
        )
    };
    (@collect
     name = $name:literal,
     ref_var = $ref_var:ident,
     reference = exact,
     ref_type = ($reference:ty),
     stream = ($($stream:tt)+),
     -> $out:ty => joins::strict_live($catalog_key:expr, $stream_key:expr, $join_fn:expr $(,)?),
     [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = exact,
            stream = exact,
            output = $out,
            ref_type = ($reference),
            stream_type = ($($stream)+),
            name = $name,
            ref_var = $ref_var,
            handler = joins::strict_live::<$reference, $($stream)+, $out, _, _, _, _>(
                $catalog_key,
                $stream_key,
                $join_fn
            ),
            middleware = [$($mw),*]
        )
    };
    (@collect
     name = $name:literal,
     ref_var = $ref_var:ident,
     reference = $ref_hint:tt,
     ref_type = $ref_type:tt,
     stream = ($($stream:tt)+),
     -> $out:ty => $handler:expr) => {
        $crate::__obzenflow_join_typed!(
            reference = $ref_hint,
            stream = exact,
            output = $out,
            ref_type = $ref_type,
            stream_type = ($($stream)+),
            name = $name,
            ref_var = $ref_var,
            handler = $handler,
            middleware = []
        )
    };
    (@collect
     name = $name:literal,
     ref_var = $ref_var:ident,
     reference = $ref_hint:tt,
     ref_type = $ref_type:tt,
     stream = ($($stream:tt)+),
     -> $out:ty => $handler:expr,
     [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = $ref_hint,
            stream = exact,
            output = $out,
            ref_type = $ref_type,
            stream_type = ($($stream)+),
            name = $name,
            ref_var = $ref_var,
            handler = $handler,
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
// ai_map_reduce!  +  __obzenflow_ai_map_reduce_typed!
// ============================================================================

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_ai_map_reduce_apply_middleware {
    ($builder:expr, []) => {
        $builder
    };

    ($builder:expr, [chunk: $mw:expr, $($rest:tt)+]) => {
        $crate::__obzenflow_ai_map_reduce_apply_middleware!($builder.chunk_middleware([$mw]), [$($rest)+])
    };
    ($builder:expr, [chunk: $mw:expr $(,)?]) => {
        $builder.chunk_middleware([$mw])
    };

    ($builder:expr, [map: $mw:expr, $($rest:tt)+]) => {
        $crate::__obzenflow_ai_map_reduce_apply_middleware!($builder.map_middleware([$mw]), [$($rest)+])
    };
    ($builder:expr, [map: $mw:expr $(,)?]) => {
        $builder.map_middleware([$mw])
    };

    ($builder:expr, [collect: $mw:expr, $($rest:tt)+]) => {
        $crate::__obzenflow_ai_map_reduce_apply_middleware!($builder.collect_middleware([$mw]), [$($rest)+])
    };
    ($builder:expr, [collect: $mw:expr $(,)?]) => {
        $builder.collect_middleware([$mw])
    };

    ($builder:expr, [reduce: $mw:expr, $($rest:tt)+]) => {
        $crate::__obzenflow_ai_map_reduce_apply_middleware!($builder.finalize_middleware([$mw]), [$($rest)+])
    };
    ($builder:expr, [reduce: $mw:expr $(,)?]) => {
        $builder.finalize_middleware([$mw])
    };

    ($builder:expr, [$role:ident : $mw:expr, $($rest:tt)+]) => {
        compile_error!("ai_map_reduce!: unsupported middleware role; expected one of: chunk, map, collect, reduce");
    };
    ($builder:expr, [$role:ident : $mw:expr $(,)?]) => {
        compile_error!("ai_map_reduce!: unsupported middleware role; expected one of: chunk, map, collect, reduce");
    };
}

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
macro_rules! __obzenflow_ai_map_reduce_cadillac_typed_default {
    (
        name = $name:literal,
        seed_type = ($($seed_ty:tt)+),
        item_type = ($item_ty:ty),
        partial_type = ($partial_ty:ty),
        out_type = ($out_ty:ty),
        chunker = ($chunker:expr),
        map_handler = ($map_handler:expr),
        reduce_handler = ($reduce_handler:expr),
        middleware = [ $($mw_role:ident : $mw_expr:expr),* $(,)? ]
    ) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::exact::<$($seed_ty)+>(),
            $crate::dsl::typing::TypeHint::exact::<$out_ty>(),
            false,
            None,
        );

        let __chunk_handler = $chunker;

        let __map_handler = $map_handler;

        let __collect_handler: ::obzenflow_runtime::stages::stateful::CollectByInput<
            $partial_ty,
            ::obzenflow_core::ai::AiMapReduceReduceInput<
                $($seed_ty)+,
                ::obzenflow_core::ai::Many<$partial_ty>,
            >,
        > = ::obzenflow_runtime::stages::stateful::CollectByInput::new(
            ::obzenflow_core::ai::Many::<$partial_ty>::default(),
            |acc, partial: &$partial_ty| {
                acc.items.push(partial.clone());
            },
        )
        .with_planning_summary(|acc, planning| {
            acc.planning = planning.clone();
        })
        .with_seed::<$($seed_ty)+>();

        let __reduce_handler = $reduce_handler;
        let __reduce_handler =
            $crate::dsl::composites::ai_map_reduce::AiMapReduceReduceInputManyAdapter::<
                $($seed_ty)+,
                $partial_ty,
                _,
            >::new(__reduce_handler);

        let __builder = $crate::dsl::composites::ai_map_reduce::map_reduce::<
            $($seed_ty)+,
            ::obzenflow_core::ai::ChunkEnvelope<$item_ty>,
            $partial_ty,
            ::obzenflow_core::ai::AiMapReduceReduceInput<
                $($seed_ty)+,
                ::obzenflow_core::ai::Many<$partial_ty>,
            >,
            $out_ty,
        >()
        .chunker(__chunk_handler)
        .map(__map_handler)
        .collect(__collect_handler)
        .finalize(__reduce_handler);

        let __builder = $crate::__obzenflow_ai_map_reduce_apply_middleware!(
            __builder,
            [ $($mw_role : $mw_expr),* ]
        );

        let mut __descriptor = __builder.build();
        __descriptor.set_name($name.to_string());
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_ai_map_reduce_cadillac_typed_collect {
    (
        name = $name:literal,
        seed_type = ($($seed_ty:tt)+),
        item_type = ($item_ty:ty),
        partial_type = ($partial_ty:ty),
        collected_type = ($collected_ty:ty),
        out_type = ($out_ty:ty),
        chunker = ($chunker:expr),
        map_handler = ($map_handler:expr),
        collect_handler = ($collect_handler:expr),
        reduce_handler = ($reduce_handler:expr),
        middleware = [ $($mw_role:ident : $mw_expr:expr),* $(,)? ]
    ) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::exact::<$($seed_ty)+>(),
            $crate::dsl::typing::TypeHint::exact::<$out_ty>(),
            false,
            None,
        );

        let __chunk_handler = $chunker;

        let __map_handler = $map_handler;

        let __collect_handler = $collect_handler;
        let __collect_handler = __collect_handler.with_seed::<$($seed_ty)+>();

        let __reduce_handler = $reduce_handler;
        let __reduce_handler =
            $crate::dsl::composites::ai_map_reduce::AiMapReduceReduceInputCollectedAdapter::<
                $($seed_ty)+,
                $collected_ty,
                _,
            >::new(__reduce_handler);

        let __builder = $crate::dsl::composites::ai_map_reduce::map_reduce::<
            $($seed_ty)+,
            ::obzenflow_core::ai::ChunkEnvelope<$item_ty>,
            $partial_ty,
            ::obzenflow_core::ai::AiMapReduceReduceInput<$($seed_ty)+, $collected_ty>,
            $out_ty,
        >()
        .chunker(__chunk_handler)
        .map(__map_handler)
        .collect(__collect_handler)
        .finalize(__reduce_handler);

        let __builder = $crate::__obzenflow_ai_map_reduce_apply_middleware!(
            __builder,
            [ $($mw_role : $mw_expr),* ]
        );

        let mut __descriptor = __builder.build();
        __descriptor.set_name($name.to_string());
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_ai_map_reduce_cadillac_contract {
    (
        name = $name:literal,
        $($rest:tt)+
    ) => {
        $crate::__obzenflow_ai_map_reduce_cadillac_contract!(@seed name = $name, seed = (), $($rest)+)
    };

    // ── seed type parsed; map/reduce roles (default collector) ──
    (@seed
        name = $name:literal,
        seed = ($($seed_ty:tt)+),
        -> $out_ty:ty => {
            map: [$item_ty:ty] -> $partial_ty:ty => $map_handler:expr,
            reduce: ($reduce_seed_ty:ty, [$reduce_partial_ty:ty]) -> $reduce_out_ty:ty => $reduce_handler:expr $(,)?
        },
        chunking: by_budget { $($chunking:tt)+ }
        $(, middleware: { $($mw_role:ident : $mw_expr:expr),* $(,)? } )?
        $(,)?
    ) => {{
        let _: ::core::marker::PhantomData<$($seed_ty)+> =
            ::core::marker::PhantomData::<$reduce_seed_ty>;
        let _: ::core::marker::PhantomData<$partial_ty> =
            ::core::marker::PhantomData::<$reduce_partial_ty>;
        let _: ::core::marker::PhantomData<$out_ty> =
            ::core::marker::PhantomData::<$reduce_out_ty>;

        let __chunker = $crate::__obzenflow_ai_map_reduce_chunker_by_budget!(
            seed_type = ($($seed_ty)+),
            item_type = ($item_ty),
            $($chunking)+
        );

        $crate::__obzenflow_ai_map_reduce_cadillac_typed_default!(
            name = $name,
            seed_type = ($($seed_ty)+),
            item_type = ($item_ty),
            partial_type = ($partial_ty),
            out_type = ($out_ty),
            chunker = (__chunker),
            map_handler = ($map_handler),
            reduce_handler = ($reduce_handler),
            middleware = [ $($($mw_role : $mw_expr),*)? ]
        )
    }};

    // ── seed type parsed; map/collect/reduce roles (custom collector) ──
    (@seed
        name = $name:literal,
        seed = ($($seed_ty:tt)+),
        -> $out_ty:ty => {
            map: [$item_ty:ty] -> $partial_ty:ty => $map_handler:expr,
            collect: $($collect_partial_ty:tt)+ -> $collected_ty:ty => $collect_handler:expr,
            reduce: ($reduce_seed_ty:ty, $reduce_collected_ty:ty) -> $reduce_out_ty:ty => $reduce_handler:expr $(,)?
        },
        chunking: by_budget { $($chunking:tt)+ }
        $(, middleware: { $($mw_role:ident : $mw_expr:expr),* $(,)? } )?
        $(,)?
    ) => {{
        let _: ::core::marker::PhantomData<$($seed_ty)+> =
            ::core::marker::PhantomData::<$reduce_seed_ty>;
        let _: ::core::marker::PhantomData<$out_ty> =
            ::core::marker::PhantomData::<$reduce_out_ty>;
        let _: ::core::marker::PhantomData<$partial_ty> =
            ::core::marker::PhantomData::<$($collect_partial_ty)+>;
        let _: ::core::marker::PhantomData<$collected_ty> =
            ::core::marker::PhantomData::<$reduce_collected_ty>;

        let __chunker = $crate::__obzenflow_ai_map_reduce_chunker_by_budget!(
            seed_type = ($($seed_ty)+),
            item_type = ($item_ty),
            $($chunking)+
        );

        $crate::__obzenflow_ai_map_reduce_cadillac_typed_collect!(
            name = $name,
            seed_type = ($($seed_ty)+),
            item_type = ($item_ty),
            partial_type = ($partial_ty),
            collected_type = ($collected_ty),
            out_type = ($out_ty),
            chunker = (__chunker),
            map_handler = ($map_handler),
            collect_handler = ($collect_handler),
            reduce_handler = ($reduce_handler),
            middleware = [ $($($mw_role : $mw_expr),*)? ]
        )
    }};

    // ── seed type accumulator ──
    (@seed name = $name:literal, seed = ($($seed:tt)+), -> $($rest:tt)+) => {
        compile_error!(
            "ai_map_reduce!: expected `Out => { ... }, chunking: by_budget { ... }` after `Seed ->`"
        );
    };
    (@seed name = $name:literal, seed = ($($seed:tt)*), -> $($rest:tt)+) => {
        $crate::__obzenflow_ai_map_reduce_cadillac_contract!(
            @seed
            name = $name,
            seed = ($($seed)+),
            -> $($rest)+
        )
    };
    (@seed name = $name:literal, seed = ($($seed:tt)*), $tok:tt $($rest:tt)+) => {
        $crate::__obzenflow_ai_map_reduce_cadillac_contract!(
            @seed
            name = $name,
            seed = ($($seed)* $tok),
            $($rest)+
        )
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_ai_map_reduce_typed {
    (
        name = $name:literal,
        in_type = ($($in:tt)+),
        chunk_type = ($chunk_ty:ty),
        partial_type = ($partial_ty:ty),
        collected_type = ($($collected_ty:tt)+),
        out_type = ($out:ty),
        chunk_handler = ($chunk_handler:expr),
        map_handler = ($map_handler:expr),
        collect_handler = ($collect_handler:expr),
        reduce_handler = ($reduce_handler:expr),
        middleware = [ $($mw_role:ident : $mw_expr:expr),* $(,)? ]
    ) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::exact::<$($in)+>(),
            $crate::dsl::typing::TypeHint::exact::<$out>(),
            false,
            None,
        );

        let __chunk_handler = $chunk_handler;
        let __chunk_handler =
            $crate::dsl::typing::BoundTransform::<$($in)+, $chunk_ty, _>::new(__chunk_handler);

        let __map_handler = $map_handler;
        let __map_handler =
            $crate::dsl::typing::BoundAsyncTransform::<$chunk_ty, $partial_ty, _>::new(
                __map_handler,
            );

        let __collect_handler = $collect_handler;

        let __reduce_handler = $reduce_handler;
        let __reduce_handler =
            $crate::dsl::typing::BoundAsyncTransform::<$($collected_ty)+, $out, _>::new(
                __reduce_handler,
            );

        let __builder = $crate::dsl::composites::ai_map_reduce::map_reduce::<
            $($in)+,
            $chunk_ty,
            $partial_ty,
            $($collected_ty)+,
            $out,
        >()
        .chunker(__chunk_handler)
        .map(__map_handler)
        .collect(__collect_handler)
        .finalize(__reduce_handler);

        let __builder = $crate::__obzenflow_ai_map_reduce_apply_middleware!(
            __builder,
            [ $($mw_role : $mw_expr),* ]
        );

        let mut __descriptor = __builder.build();
        __descriptor.set_name($name.to_string());
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_ai_map_reduce_contract {
    (name = $name:literal, chunk: $($rest:tt)+) => {
        $crate::__obzenflow_ai_map_reduce_contract!(@chunk name = $name, in = (), $($rest)+)
    };
    (name = $name:literal, $($rest:tt)+) => {
        compile_error!("ai_map_reduce!: expected `chunk: In -> Chunk => handler` as the first role line");
    };

    // ── chunk: In -> Chunk => handler, ... ──
    (@chunk name = $name:literal, in = ($($in:tt)*), -> $chunk_ty:ty => $chunk_handler:expr, map: $($rest:tt)+) => {
        $crate::__obzenflow_ai_map_reduce_contract!(
            @map
            name = $name,
            in_type = ($($in)*),
            chunk_type = ($chunk_ty),
            chunk_handler = ($chunk_handler),
            map_in = (),
            $($rest)+
        )
    };
    (@chunk name = $name:literal, in = ($($in:tt)*), $tok:tt $($rest:tt)+) => {
        $crate::__obzenflow_ai_map_reduce_contract!(
            @chunk
            name = $name,
            in = ($($in)* $tok),
            $($rest)+
        )
    };

    // ── map: Chunk -> Partial => handler, (reduce|collect): ... ──
    (@map
        name = $name:literal,
        in_type = ($($in:tt)+),
        chunk_type = ($chunk_ty:ty),
        chunk_handler = ($chunk_handler:expr),
        map_in = ($($chunk_in:tt)*),
        -> $partial_ty:ty => $map_handler:expr,
        reduce: $($rest:tt)+
    ) => {
        $crate::__obzenflow_ai_map_reduce_contract!(
            @reduce_default
            name = $name,
            in_type = ($($in)+),
            chunk_type = ($chunk_ty),
            partial_type = ($partial_ty),
            collected_type = (),
            chunk_handler = ($chunk_handler),
            map_handler = ($map_handler),
            map_in = ($($chunk_in)*),
            $($rest)+
        )
    };
    (@map
        name = $name:literal,
        in_type = ($($in:tt)+),
        chunk_type = ($chunk_ty:ty),
        chunk_handler = ($chunk_handler:expr),
        map_in = ($($chunk_in:tt)*),
        -> $partial_ty:ty => $map_handler:expr,
        collect: $($rest:tt)+
    ) => {
        $crate::__obzenflow_ai_map_reduce_contract!(
            @collect
            name = $name,
            in_type = ($($in)+),
            chunk_type = ($chunk_ty),
            partial_type = ($partial_ty),
            chunk_handler = ($chunk_handler),
            map_handler = ($map_handler),
            map_in = ($($chunk_in)*),
            partial_in = (),
            $($rest)+
        )
    };
    (@map
        name = $name:literal,
        in_type = ($($in:tt)+),
        chunk_type = ($chunk_ty:ty),
        chunk_handler = ($chunk_handler:expr),
        map_in = ($($chunk_in:tt)*),
        $tok:tt $($rest:tt)+
    ) => {
        $crate::__obzenflow_ai_map_reduce_contract!(
            @map
            name = $name,
            in_type = ($($in)+),
            chunk_type = ($chunk_ty),
            chunk_handler = ($chunk_handler),
            map_in = ($($chunk_in)* $tok),
            $($rest)+
        )
    };

    // ── collect: Partial -> Collected => handler, reduce: ... ──
    (@collect
        name = $name:literal,
        in_type = ($($in:tt)+),
        chunk_type = ($chunk_ty:ty),
        partial_type = ($partial_ty:ty),
        chunk_handler = ($chunk_handler:expr),
        map_handler = ($map_handler:expr),
        map_in = ($($chunk_in:tt)+),
        partial_in = ($($partial_in:tt)*),
        -> $collected_ty:ty => $collect_handler:expr,
        reduce: $($rest:tt)+
    ) => {
        $crate::__obzenflow_ai_map_reduce_contract!(
            @reduce_override
            name = $name,
            in_type = ($($in)+),
            chunk_type = ($chunk_ty),
            partial_type = ($partial_ty),
            collected_type = ($collected_ty),
            chunk_handler = ($chunk_handler),
            map_handler = ($map_handler),
            collect_handler = ($collect_handler),
            map_in = ($($chunk_in)+),
            partial_in = ($($partial_in)*),
            reduce_in = (),
            $($rest)+
        )
    };
    (@collect
        name = $name:literal,
        in_type = ($($in:tt)+),
        chunk_type = ($chunk_ty:ty),
        partial_type = ($partial_ty:ty),
        chunk_handler = ($chunk_handler:expr),
        map_handler = ($map_handler:expr),
        map_in = ($($chunk_in:tt)+),
        partial_in = ($($partial_in:tt)*),
        $tok:tt $($rest:tt)+
    ) => {
        $crate::__obzenflow_ai_map_reduce_contract!(
            @collect
            name = $name,
            in_type = ($($in)+),
            chunk_type = ($chunk_ty),
            partial_type = ($partial_ty),
            chunk_handler = ($chunk_handler),
            map_handler = ($map_handler),
            map_in = ($($chunk_in)+),
            partial_in = ($($partial_in)* $tok),
            $($rest)+
        )
    };

    // ── reduce: Many<Partial> -> Out => handler (+ optional middleware) ──
    (@reduce_default
        name = $name:literal,
        in_type = ($($in:tt)+),
        chunk_type = ($chunk_ty:ty),
        partial_type = ($partial_ty:ty),
        collected_type = ($($collected_ty:tt)*),
        chunk_handler = ($chunk_handler:expr),
        map_handler = ($map_handler:expr),
        map_in = ($($chunk_in:tt)+),
        -> $out:ty => $reduce_handler:expr $(,)?
    ) => {{
        let _: ::core::marker::PhantomData<$($chunk_in)+> =
            ::core::marker::PhantomData::<$chunk_ty>;

        let __default_collect_handler: ::obzenflow_runtime::stages::stateful::CollectByInput<
            $partial_ty,
            $($collected_ty)+,
        > = ::obzenflow_runtime::stages::stateful::CollectByInput::new(
            ::obzenflow_core::ai::Many::<$partial_ty>::default(),
            |acc, partial: &$partial_ty| {
                acc.items.push(partial.clone());
            },
        )
        .with_planning_summary(|acc, planning| {
            acc.planning = planning.clone();
        });

        $crate::__obzenflow_ai_map_reduce_typed!(
            name = $name,
            in_type = ($($in)+),
            chunk_type = ($chunk_ty),
            partial_type = ($partial_ty),
            collected_type = ($($collected_ty)+),
            out_type = ($out),
            chunk_handler = ($chunk_handler),
            map_handler = ($map_handler),
            collect_handler = (__default_collect_handler),
            reduce_handler = ($reduce_handler),
            middleware = []
        )
    }};
    (@reduce_default
        name = $name:literal,
        in_type = ($($in:tt)+),
        chunk_type = ($chunk_ty:ty),
        partial_type = ($partial_ty:ty),
        collected_type = ($($collected_ty:tt)*),
        chunk_handler = ($chunk_handler:expr),
        map_handler = ($map_handler:expr),
        map_in = ($($chunk_in:tt)+),
        -> $out:ty => $reduce_handler:expr,
        [ $($mw_role:ident : $mw_expr:expr),* $(,)? ] $(,)?
    ) => {{
        let _: ::core::marker::PhantomData<$($chunk_in)+> =
            ::core::marker::PhantomData::<$chunk_ty>;

        let __default_collect_handler: ::obzenflow_runtime::stages::stateful::CollectByInput<
            $partial_ty,
            $($collected_ty)+,
        > = ::obzenflow_runtime::stages::stateful::CollectByInput::new(
            ::obzenflow_core::ai::Many::<$partial_ty>::default(),
            |acc, partial: &$partial_ty| {
                acc.items.push(partial.clone());
            },
        )
        .with_planning_summary(|acc, planning| {
            acc.planning = planning.clone();
        });

        $crate::__obzenflow_ai_map_reduce_typed!(
            name = $name,
            in_type = ($($in)+),
            chunk_type = ($chunk_ty),
            partial_type = ($partial_ty),
            collected_type = ($($collected_ty)+),
            out_type = ($out),
            chunk_handler = ($chunk_handler),
            map_handler = ($map_handler),
            collect_handler = (__default_collect_handler),
            reduce_handler = ($reduce_handler),
            middleware = [ $($mw_role : $mw_expr),* ]
        )
    }};
    (@reduce_default
        name = $name:literal,
        in_type = ($($in:tt)+),
        chunk_type = ($chunk_ty:ty),
        partial_type = ($partial_ty:ty),
        collected_type = ($($collected_ty:tt)*),
        chunk_handler = ($chunk_handler:expr),
        map_handler = ($map_handler:expr),
        map_in = ($($chunk_in:tt)+),
        $tok:tt $($rest:tt)+
    ) => {
        $crate::__obzenflow_ai_map_reduce_contract!(
            @reduce_default
            name = $name,
            in_type = ($($in)+),
            chunk_type = ($chunk_ty),
            partial_type = ($partial_ty),
            collected_type = ($($collected_ty)* $tok),
            chunk_handler = ($chunk_handler),
            map_handler = ($map_handler),
            map_in = ($($chunk_in)+),
            $($rest)+
        )
    };

    // ── reduce: Collected -> Out => handler (+ optional middleware) ──
    (@reduce_override
        name = $name:literal,
        in_type = ($($in:tt)+),
        chunk_type = ($chunk_ty:ty),
        partial_type = ($partial_ty:ty),
        collected_type = ($collected_ty:ty),
        chunk_handler = ($chunk_handler:expr),
        map_handler = ($map_handler:expr),
        collect_handler = ($collect_handler:expr),
        map_in = ($($chunk_in:tt)+),
        partial_in = ($($partial_in:tt)+),
        reduce_in = ($($collected_in:tt)*),
        -> $out:ty => $reduce_handler:expr $(,)?
    ) => {{
        let _: ::core::marker::PhantomData<$($chunk_in)+> =
            ::core::marker::PhantomData::<$chunk_ty>;
        let _: ::core::marker::PhantomData<$($partial_in)+> =
            ::core::marker::PhantomData::<$partial_ty>;
        let _: ::core::marker::PhantomData<$($collected_in)+> =
            ::core::marker::PhantomData::<$collected_ty>;

        $crate::__obzenflow_ai_map_reduce_typed!(
            name = $name,
            in_type = ($($in)+),
            chunk_type = ($chunk_ty),
            partial_type = ($partial_ty),
            collected_type = ($collected_ty),
            out_type = ($out),
            chunk_handler = ($chunk_handler),
            map_handler = ($map_handler),
            collect_handler = ($collect_handler),
            reduce_handler = ($reduce_handler),
            middleware = []
        )
    }};
    (@reduce_override
        name = $name:literal,
        in_type = ($($in:tt)+),
        chunk_type = ($chunk_ty:ty),
        partial_type = ($partial_ty:ty),
        collected_type = ($collected_ty:ty),
        chunk_handler = ($chunk_handler:expr),
        map_handler = ($map_handler:expr),
        collect_handler = ($collect_handler:expr),
        map_in = ($($chunk_in:tt)+),
        partial_in = ($($partial_in:tt)+),
        reduce_in = ($($collected_in:tt)*),
        -> $out:ty => $reduce_handler:expr,
        [ $($mw_role:ident : $mw_expr:expr),* $(,)? ] $(,)?
    ) => {{
        let _: ::core::marker::PhantomData<$($chunk_in)+> =
            ::core::marker::PhantomData::<$chunk_ty>;
        let _: ::core::marker::PhantomData<$($partial_in)+> =
            ::core::marker::PhantomData::<$partial_ty>;
        let _: ::core::marker::PhantomData<$($collected_in)+> =
            ::core::marker::PhantomData::<$collected_ty>;

        $crate::__obzenflow_ai_map_reduce_typed!(
            name = $name,
            in_type = ($($in)+),
            chunk_type = ($chunk_ty),
            partial_type = ($partial_ty),
            collected_type = ($collected_ty),
            out_type = ($out),
            chunk_handler = ($chunk_handler),
            map_handler = ($map_handler),
            collect_handler = ($collect_handler),
            reduce_handler = ($reduce_handler),
            middleware = [ $($mw_role : $mw_expr),* ]
        )
    }};
    (@reduce_override
        name = $name:literal,
        in_type = ($($in:tt)+),
        chunk_type = ($chunk_ty:ty),
        partial_type = ($partial_ty:ty),
        collected_type = ($collected_ty:ty),
        chunk_handler = ($chunk_handler:expr),
        map_handler = ($map_handler:expr),
        collect_handler = ($collect_handler:expr),
        map_in = ($($chunk_in:tt)+),
        partial_in = ($($partial_in:tt)+),
        reduce_in = ($($collected_in:tt)*),
        $tok:tt $($rest:tt)+
    ) => {
        $crate::__obzenflow_ai_map_reduce_contract!(
            @reduce_override
            name = $name,
            in_type = ($($in)+),
            chunk_type = ($chunk_ty),
            partial_type = ($partial_ty),
            collected_type = ($collected_ty),
            chunk_handler = ($chunk_handler),
            map_handler = ($map_handler),
            collect_handler = ($collect_handler),
            map_in = ($($chunk_in)+),
            partial_in = ($($partial_in)+),
            reduce_in = ($($collected_in)* $tok),
            $($rest)+
        )
    };
}

/// Create an AI map-reduce composite stage descriptor.
#[macro_export]
macro_rules! ai_map_reduce {
    // ── Legacy surface (FLOWIP-086z-part-2) ──
    (name: $name:literal, chunk: $($rest:tt)+) => {
        $crate::__obzenflow_ai_map_reduce_contract!(name = $name, chunk: $($rest)+)
    };
    (chunk: $($rest:tt)+) => {
        $crate::__obzenflow_ai_map_reduce_contract!(
            name = "__obzenflow_binding_derived_name__",
            chunk: $($rest)+
        )
    };

    // ── Cadillac surface (FLOWIP-086z-part-3) ──
    (name: $name:literal, $($rest:tt)+) => {
        $crate::__obzenflow_ai_map_reduce_cadillac_contract!(name = $name, $($rest)+)
    };
    ($($rest:tt)+) => {
        $crate::__obzenflow_ai_map_reduce_cadillac_contract!(
            name = "__obzenflow_binding_derived_name__",
            $($rest)+
        )
    };
}
