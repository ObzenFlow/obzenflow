// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Stage macros for building ObzenFlow pipeline descriptors.
//!
//! Public macros use canonical directional contracts for typed stages:
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
//! Untyped macros follow the same naming and join-role conventions:
//!
//! ```ignore
//! source!(handler)
//! async_source!(handler)
//! infinite_source!(handler)
//! async_infinite_source!(handler)
//! transform!(handler)
//! async_transform!(handler)
//! stateful!(handler)
//! sink!(handler)
//! join!(catalog CatalogStage => handler)
//! ```
//!
//! By default, runtime stage names are derived from the left-hand binding in
//! the enclosing `flow!` block. Use `name: "..."` to override the runtime
//! name explicitly.
//!
//! Typed arms dispatch into `#[doc(hidden)]` helper macros that handle
//! normalisation, metadata construction, assertions, and descriptor wrapping.
//! Untyped arms produce descriptors directly.

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
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
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
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
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
        ::obzenflow_runtime::typing::assert_source_output::<_, $out>(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
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
    // ── untyped (binding-derived name) ──
    ($handler:expr) => {
        $crate::__obzenflow_source_untyped!(
            name = "__obzenflow_binding_derived_name__",
            handler = $handler,
            middleware = []
        )
    };
    ($handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_source_untyped!(
            name = "__obzenflow_binding_derived_name__",
            handler = $handler,
            middleware = [$($mw),*]
        )
    };

    // ── untyped (explicit name override) ──
    (name: $name:literal, $handler:expr) => {
        $crate::__obzenflow_source_untyped!(name = $name, handler = $handler, middleware = [])
    };
    (name: $name:literal, $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_source_untyped!(name = $name, handler = $handler, middleware = [$($mw),*])
    };

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
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
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
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
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
        ::obzenflow_runtime::typing::assert_source_output::<_, $out>(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
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
        ::obzenflow_runtime::typing::assert_source_output::<_, $out>(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
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
    // ── untyped (binding-derived name) ──
    (($handler:expr, $poll_timeout:expr)) => {
        $crate::__obzenflow_async_source_untyped!(
            name = "__obzenflow_binding_derived_name__",
            handler = ($handler, $poll_timeout),
            middleware = []
        )
    };
    (($handler:expr, $poll_timeout:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_source_untyped!(
            name = "__obzenflow_binding_derived_name__",
            handler = ($handler, $poll_timeout),
            middleware = [$($mw),*]
        )
    };
    ($handler:expr) => {
        $crate::__obzenflow_async_source_untyped!(name = "__obzenflow_binding_derived_name__", handler = $handler, middleware = [])
    };
    ($handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_async_source_untyped!(name = "__obzenflow_binding_derived_name__", handler = $handler, middleware = [$($mw),*])
    };

    // ── untyped (explicit name override) ──
    (name: $name:literal, ($handler:expr, $poll_timeout:expr)) => {
        $crate::__obzenflow_async_source_untyped!(
            name = $name,
            handler = ($handler, $poll_timeout),
            middleware = []
        )
    };
    (name: $name:literal, ($handler:expr, $poll_timeout:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_source_untyped!(
            name = $name,
            handler = ($handler, $poll_timeout),
            middleware = [$($mw),*]
        )
    };
    (name: $name:literal, $handler:expr) => {
        $crate::__obzenflow_async_source_untyped!(name = $name, handler = $handler, middleware = [])
    };
    (name: $name:literal, $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_async_source_untyped!(name = $name, handler = $handler, middleware = [$($mw),*])
    };

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
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
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
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
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
        ::obzenflow_runtime::typing::assert_source_output::<_, $out>(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
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
    // ── untyped (binding-derived name) ──
    ($handler:expr) => {
        $crate::__obzenflow_infinite_source_untyped!(
            name = "__obzenflow_binding_derived_name__",
            handler = $handler,
            middleware = []
        )
    };
    ($handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_infinite_source_untyped!(
            name = "__obzenflow_binding_derived_name__",
            handler = $handler,
            middleware = [$($mw),*]
        )
    };

    // ── untyped (explicit name override) ──
    (name: $name:literal, $handler:expr) => {
        $crate::__obzenflow_infinite_source_untyped!(name = $name, handler = $handler, middleware = [])
    };
    (name: $name:literal, $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_infinite_source_untyped!(name = $name, handler = $handler, middleware = [$($mw),*])
    };

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
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
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
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
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
        ::obzenflow_runtime::typing::assert_source_output::<_, $out>(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
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
        ::obzenflow_runtime::typing::assert_source_output::<_, $out>(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
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
    // ── untyped (binding-derived name) ──
    (($handler:expr, $poll_timeout:expr)) => {
        $crate::__obzenflow_async_infinite_source_untyped!(
            name = "__obzenflow_binding_derived_name__",
            handler = ($handler, $poll_timeout),
            middleware = []
        )
    };
    (($handler:expr, $poll_timeout:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_infinite_source_untyped!(
            name = "__obzenflow_binding_derived_name__",
            handler = ($handler, $poll_timeout),
            middleware = [$($mw),*]
        )
    };
    ($handler:expr) => {
        $crate::__obzenflow_async_infinite_source_untyped!(
            name = "__obzenflow_binding_derived_name__",
            handler = $handler,
            middleware = []
        )
    };
    ($handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_async_infinite_source_untyped!(
            name = "__obzenflow_binding_derived_name__",
            handler = $handler,
            middleware = [$($mw),*]
        )
    };

    // ── untyped (explicit name override) ──
    (name: $name:literal, ($handler:expr, $poll_timeout:expr)) => {
        $crate::__obzenflow_async_infinite_source_untyped!(
            name = $name,
            handler = ($handler, $poll_timeout),
            middleware = []
        )
    };
    (name: $name:literal, ($handler:expr, $poll_timeout:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_infinite_source_untyped!(
            name = $name,
            handler = ($handler, $poll_timeout),
            middleware = [$($mw),*]
        )
    };
    (name: $name:literal, $handler:expr) => {
        $crate::__obzenflow_async_infinite_source_untyped!(name = $name, handler = $handler, middleware = [])
    };
    (name: $name:literal, $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_async_infinite_source_untyped!(name = $name, handler = $handler, middleware = [$($mw),*])
    };

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
    // ── mixed input, placeholder ──
    (input = mixed, output = $out:ty, name = $name:literal, handler = placeholder!(), middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            None,
        );
        let __descriptor = $crate::__obzenflow_transform_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderTransform::<::obzenflow_runtime::typing::MixedInput, $out>::new(None),
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (input = mixed, output = $out:ty, name = $name:literal, handler = placeholder!($msg:expr), middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::__obzenflow_transform_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderTransform::<::obzenflow_runtime::typing::MixedInput, $out>::new(Some($msg)),
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // ── mixed input, real handler ──
    (input = mixed, output = $out:ty, name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*]) => {{
        let __handler = $handler;
        let __handler =
            $crate::dsl::typing::BoundTransform::<::obzenflow_runtime::typing::MixedInput, $out, _>::new(__handler);
        ::obzenflow_runtime::typing::assert_transform_output::<_, $out>(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            false,
            None,
        );
        let __descriptor = $crate::__obzenflow_transform_untyped!(name = $name, handler = __handler, middleware = [$($mw),*]);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // ── exact input, placeholder ──
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = placeholder!(), middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::exact(stringify!($in)),
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
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
            $crate::dsl::typing::TypeHint::exact(stringify!($in)),
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
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
            $crate::dsl::typing::TypeHint::exact(stringify!($in)),
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
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
    // ── typed (binding-derived name): mixed input ──
    (mixed -> $out:ty => placeholder!()) => {
        $crate::__obzenflow_transform_typed!(
            input = mixed,
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!(),
            middleware = []
        )
    };
    (mixed -> $out:ty => placeholder!($msg:expr)) => {
        $crate::__obzenflow_transform_typed!(
            input = mixed,
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!($msg),
            middleware = []
        )
    };
    (mixed -> $out:ty => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_transform_typed!(
            input = mixed,
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!(),
            middleware = [$($mw),*]
        )
    };
    (mixed -> $out:ty => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_transform_typed!(
            input = mixed,
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!($msg),
            middleware = [$($mw),*]
        )
    };
    (mixed -> $out:ty => $handler:expr) => {
        $crate::__obzenflow_transform_typed!(
            input = mixed,
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = $handler,
            middleware = []
        )
    };
    (mixed -> $out:ty => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_transform_typed!(
            input = mixed,
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = $handler,
            middleware = [$($mw),*]
        )
    };

    // ── typed (explicit name override): mixed input ──
    (name: $name:literal, mixed -> $out:ty => placeholder!()) => {
        $crate::__obzenflow_transform_typed!(input = mixed, output = $out, name = $name, handler = placeholder!(), middleware = [])
    };
    (name: $name:literal, mixed -> $out:ty => placeholder!($msg:expr)) => {
        $crate::__obzenflow_transform_typed!(input = mixed, output = $out, name = $name, handler = placeholder!($msg), middleware = [])
    };
    (name: $name:literal, mixed -> $out:ty => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_transform_typed!(input = mixed, output = $out, name = $name, handler = placeholder!(), middleware = [$($mw),*])
    };
    (name: $name:literal, mixed -> $out:ty => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_transform_typed!(input = mixed, output = $out, name = $name, handler = placeholder!($msg), middleware = [$($mw),*])
    };
    (name: $name:literal, mixed -> $out:ty => $handler:expr) => {
        $crate::__obzenflow_transform_typed!(input = mixed, output = $out, name = $name, handler = $handler, middleware = [])
    };
    (name: $name:literal, mixed -> $out:ty => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_transform_typed!(input = mixed, output = $out, name = $name, handler = $handler, middleware = [$($mw),*])
    };

    // ── untyped (binding-derived name) ──
    ($handler:expr) => {
        $crate::__obzenflow_transform_untyped!(
            name = "__obzenflow_binding_derived_name__",
            handler = $handler,
            middleware = []
        )
    };
    ($handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_transform_untyped!(
            name = "__obzenflow_binding_derived_name__",
            handler = $handler,
            middleware = [$($mw),*]
        )
    };

    // ── untyped (explicit name override) ──
    (name: $name:literal, $handler:expr) => {
        $crate::__obzenflow_transform_untyped!(name = $name, handler = $handler, middleware = [])
    };
    (name: $name:literal, $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_transform_untyped!(name = $name, handler = $handler, middleware = [$($mw),*])
    };

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
    // ── mixed input, placeholder ──
    (input = mixed, output = $out:ty, name = $name:literal, handler = placeholder!(), middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            None,
        );
        let __descriptor = $crate::__obzenflow_async_transform_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderAsyncTransform::<::obzenflow_runtime::typing::MixedInput, $out>::new(None),
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (input = mixed, output = $out:ty, name = $name:literal, handler = placeholder!($msg:expr), middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::__obzenflow_async_transform_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderAsyncTransform::<::obzenflow_runtime::typing::MixedInput, $out>::new(Some($msg)),
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // ── mixed input, real handler ──
    (input = mixed, output = $out:ty, name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*]) => {{
        let __handler = $handler;
        let __handler =
            $crate::dsl::typing::BoundAsyncTransform::<::obzenflow_runtime::typing::MixedInput, $out, _>::new(__handler);
        ::obzenflow_runtime::typing::assert_transform_output::<_, $out>(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            false,
            None,
        );
        let __descriptor = $crate::__obzenflow_async_transform_untyped!(name = $name, handler = __handler, middleware = [$($mw),*]);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // ── exact input, placeholder ──
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = placeholder!(), middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::exact(stringify!($in)),
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
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
            $crate::dsl::typing::TypeHint::exact(stringify!($in)),
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
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
            $crate::dsl::typing::TypeHint::exact(stringify!($in)),
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
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
    // ── typed (binding-derived name): mixed input ──
    (mixed -> $out:ty => placeholder!()) => {
        $crate::__obzenflow_async_transform_typed!(
            input = mixed,
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!(),
            middleware = []
        )
    };
    (mixed -> $out:ty => placeholder!($msg:expr)) => {
        $crate::__obzenflow_async_transform_typed!(
            input = mixed,
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!($msg),
            middleware = []
        )
    };
    (mixed -> $out:ty => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_transform_typed!(
            input = mixed,
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!(),
            middleware = [$($mw),*]
        )
    };
    (mixed -> $out:ty => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_transform_typed!(
            input = mixed,
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = placeholder!($msg),
            middleware = [$($mw),*]
        )
    };
    (mixed -> $out:ty => $handler:expr) => {
        $crate::__obzenflow_async_transform_typed!(
            input = mixed,
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = $handler,
            middleware = []
        )
    };
    (mixed -> $out:ty => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_async_transform_typed!(
            input = mixed,
            output = $out,
            name = "__obzenflow_binding_derived_name__",
            handler = $handler,
            middleware = [$($mw),*]
        )
    };

    // ── typed (explicit name override): mixed input ──
    (name: $name:literal, mixed -> $out:ty => placeholder!()) => {
        $crate::__obzenflow_async_transform_typed!(input = mixed, output = $out, name = $name, handler = placeholder!(), middleware = [])
    };
    (name: $name:literal, mixed -> $out:ty => placeholder!($msg:expr)) => {
        $crate::__obzenflow_async_transform_typed!(input = mixed, output = $out, name = $name, handler = placeholder!($msg), middleware = [])
    };
    (name: $name:literal, mixed -> $out:ty => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_transform_typed!(input = mixed, output = $out, name = $name, handler = placeholder!(), middleware = [$($mw),*])
    };
    (name: $name:literal, mixed -> $out:ty => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_transform_typed!(input = mixed, output = $out, name = $name, handler = placeholder!($msg), middleware = [$($mw),*])
    };
    (name: $name:literal, mixed -> $out:ty => $handler:expr) => {
        $crate::__obzenflow_async_transform_typed!(input = mixed, output = $out, name = $name, handler = $handler, middleware = [])
    };
    (name: $name:literal, mixed -> $out:ty => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_async_transform_typed!(input = mixed, output = $out, name = $name, handler = $handler, middleware = [$($mw),*])
    };

    // ── untyped (binding-derived name) ──
    ($handler:expr) => {
        $crate::__obzenflow_async_transform_untyped!(
            name = "__obzenflow_binding_derived_name__",
            handler = $handler,
            middleware = []
        )
    };
    ($handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_async_transform_untyped!(
            name = "__obzenflow_binding_derived_name__",
            handler = $handler,
            middleware = [$($mw),*]
        )
    };

    // ── untyped (explicit name override) ──
    (name: $name:literal, $handler:expr) => {
        $crate::__obzenflow_async_transform_untyped!(name = $name, handler = $handler, middleware = [])
    };
    (name: $name:literal, $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_async_transform_untyped!(name = $name, handler = $handler, middleware = [$($mw),*])
    };

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
    // ── mixed input, placeholder ──
    (input = mixed, name = $name:literal, handler = placeholder!(), middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::sink(
            $crate::dsl::typing::TypeHint::Mixed,
            true,
            None,
        );
        let __descriptor = $crate::__obzenflow_sink_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderSink::<::obzenflow_runtime::typing::MixedInput>::new(None),
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (input = mixed, name = $name:literal, handler = placeholder!($msg:expr), middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::sink(
            $crate::dsl::typing::TypeHint::Mixed,
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::__obzenflow_sink_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderSink::<::obzenflow_runtime::typing::MixedInput>::new(Some($msg)),
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // ── mixed input, real handler (no SinkTyping required) ──
    (input = mixed, name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::sink(
            $crate::dsl::typing::TypeHint::Mixed,
            false,
            None,
        );
        let __descriptor = $crate::__obzenflow_sink_untyped!(name = $name, handler = $handler, middleware = [$($mw),*]);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // ── exact input, placeholder ──
    (input = exact($in:ty), name = $name:literal, handler = placeholder!(), middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::sink(
            $crate::dsl::typing::TypeHint::exact(stringify!($in)),
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
            $crate::dsl::typing::TypeHint::exact(stringify!($in)),
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
    (input = exact($in:ty), name = $name:literal, handler = $handler:expr, middleware = [$($mw:expr),*]) => {{
        let __handler = $handler;
        ::obzenflow_runtime::typing::assert_sink_input::<_, $in>(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::sink(
            $crate::dsl::typing::TypeHint::exact(stringify!($in)),
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
    // ── typed (binding-derived name): mixed input ──
    (mixed => placeholder!()) => {
        $crate::__obzenflow_sink_typed!(input = mixed, name = "__obzenflow_binding_derived_name__", handler = placeholder!(), middleware = [])
    };
    (mixed => placeholder!($msg:expr)) => {
        $crate::__obzenflow_sink_typed!(input = mixed, name = "__obzenflow_binding_derived_name__", handler = placeholder!($msg), middleware = [])
    };
    (mixed => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(input = mixed, name = "__obzenflow_binding_derived_name__", handler = placeholder!(), middleware = [$($mw),*])
    };
    (mixed => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(input = mixed, name = "__obzenflow_binding_derived_name__", handler = placeholder!($msg), middleware = [$($mw),*])
    };
    (mixed => $handler:expr) => {
        $crate::__obzenflow_sink_typed!(input = mixed, name = "__obzenflow_binding_derived_name__", handler = $handler, middleware = [])
    };
    (mixed => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(input = mixed, name = "__obzenflow_binding_derived_name__", handler = $handler, middleware = [$($mw),*])
    };

    // ── typed (explicit name override): mixed input ──
    (name: $name:literal, mixed => placeholder!()) => {
        $crate::__obzenflow_sink_typed!(input = mixed, name = $name, handler = placeholder!(), middleware = [])
    };
    (name: $name:literal, mixed => placeholder!($msg:expr)) => {
        $crate::__obzenflow_sink_typed!(input = mixed, name = $name, handler = placeholder!($msg), middleware = [])
    };
    (name: $name:literal, mixed => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(input = mixed, name = $name, handler = placeholder!(), middleware = [$($mw),*])
    };
    (name: $name:literal, mixed => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(input = mixed, name = $name, handler = placeholder!($msg), middleware = [$($mw),*])
    };
    (name: $name:literal, mixed => $handler:expr) => {
        $crate::__obzenflow_sink_typed!(input = mixed, name = $name, handler = $handler, middleware = [])
    };
    (name: $name:literal, mixed => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(input = mixed, name = $name, handler = $handler, middleware = [$($mw),*])
    };

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

    // ── untyped (binding-derived name): handler ──
    ($handler:expr) => {
        $crate::__obzenflow_sink_untyped!(name = "__obzenflow_binding_derived_name__", handler = $handler, middleware = [])
    };
    ($handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_untyped!(name = "__obzenflow_binding_derived_name__", handler = $handler, middleware = [$($mw),*])
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

    // ── untyped (explicit name override): handler ──
    (name: $name:literal, $handler:expr) => {
        $crate::__obzenflow_sink_untyped!(name = $name, handler = $handler, middleware = [])
    };
    (name: $name:literal, $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_untyped!(name = $name, handler = $handler, middleware = [$($mw),*])
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
    // ── mixed input, placeholder, no emit ──
    (input = mixed, output = $out:ty, name = $name:literal, handler = placeholder!(), emit = none, middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            None,
        );
        let __descriptor = $crate::__obzenflow_stateful_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderStateful::<::obzenflow_runtime::typing::MixedInput, $out>::new(None),
            emit = none,
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // ── mixed input, placeholder, with emit ──
    (input = mixed, output = $out:ty, name = $name:literal, handler = placeholder!(), emit = some($emit_interval:expr), middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            None,
        );
        let __descriptor = $crate::__obzenflow_stateful_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderStateful::<::obzenflow_runtime::typing::MixedInput, $out>::new(None),
            emit = some($emit_interval),
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // ── mixed input, placeholder msg, no emit ──
    (input = mixed, output = $out:ty, name = $name:literal, handler = placeholder!($msg:expr), emit = none, middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::__obzenflow_stateful_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderStateful::<::obzenflow_runtime::typing::MixedInput, $out>::new(Some($msg)),
            emit = none,
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // ── mixed input, placeholder msg, with emit ──
    (input = mixed, output = $out:ty, name = $name:literal, handler = placeholder!($msg:expr), emit = some($emit_interval:expr), middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::__obzenflow_stateful_untyped!(
            name = $name,
            handler = $crate::dsl::typing::PlaceholderStateful::<::obzenflow_runtime::typing::MixedInput, $out>::new(Some($msg)),
            emit = some($emit_interval),
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // ── mixed input, real handler, no emit ──
    (input = mixed, output = $out:ty, name = $name:literal, handler = $handler:expr, emit = none, middleware = [$($mw:expr),*]) => {{
        let __handler = $handler;
        ::obzenflow_runtime::typing::assert_stateful_output::<_, $out>(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            false,
            None,
        );
        let __descriptor = $crate::__obzenflow_stateful_untyped!(name = $name, handler = __handler, emit = none, middleware = [$($mw),*]);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // ── mixed input, real handler, with emit ──
    (input = mixed, output = $out:ty, name = $name:literal, handler = $handler:expr, emit = some($emit_interval:expr), middleware = [$($mw:expr),*]) => {{
        let __handler = $handler;
        ::obzenflow_runtime::typing::assert_stateful_output::<_, $out>(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            false,
            None,
        );
        let __descriptor = $crate::__obzenflow_stateful_untyped!(name = $name, handler = __handler, emit = some($emit_interval), middleware = [$($mw),*]);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // ── exact input, placeholder, no emit ──
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = placeholder!(), emit = none, middleware = [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::exact(stringify!($in)),
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
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
            $crate::dsl::typing::TypeHint::exact(stringify!($in)),
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
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
            $crate::dsl::typing::TypeHint::exact(stringify!($in)),
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
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
            $crate::dsl::typing::TypeHint::exact(stringify!($in)),
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
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
        ::obzenflow_runtime::typing::assert_stateful_contract::<_, $in, $out>(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::exact(stringify!($in)),
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            false,
            None,
        );
        let __descriptor = $crate::__obzenflow_stateful_untyped!(name = $name, handler = __handler, emit = none, middleware = [$($mw),*]);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // ── exact input, real handler, with emit ──
    (input = exact($in:ty), output = $out:ty, name = $name:literal, handler = $handler:expr, emit = some($emit_interval:expr), middleware = [$($mw:expr),*]) => {{
        let __handler = $handler;
        ::obzenflow_runtime::typing::assert_stateful_contract::<_, $in, $out>(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::exact(stringify!($in)),
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
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
    // ── typed (binding-derived name): mixed input ──
    (mixed -> $out:ty => placeholder!()) => {
        $crate::__obzenflow_stateful_typed!(input = mixed, output = $out, name = "__obzenflow_binding_derived_name__", handler = placeholder!(), emit = none, middleware = [])
    };
    (mixed -> $out:ty => placeholder!($msg:expr)) => {
        $crate::__obzenflow_stateful_typed!(input = mixed, output = $out, name = "__obzenflow_binding_derived_name__", handler = placeholder!($msg), emit = none, middleware = [])
    };
    (mixed -> $out:ty => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_stateful_typed!(input = mixed, output = $out, name = "__obzenflow_binding_derived_name__", handler = placeholder!(), emit = none, middleware = [$($mw),*])
    };
    (mixed -> $out:ty => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_stateful_typed!(input = mixed, output = $out, name = "__obzenflow_binding_derived_name__", handler = placeholder!($msg), emit = none, middleware = [$($mw),*])
    };
    (mixed -> $out:ty => $handler:expr) => {
        $crate::__obzenflow_stateful_typed!(input = mixed, output = $out, name = "__obzenflow_binding_derived_name__", handler = $handler, emit = none, middleware = [])
    };
    (mixed -> $out:ty => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_stateful_typed!(input = mixed, output = $out, name = "__obzenflow_binding_derived_name__", handler = $handler, emit = none, middleware = [$($mw),*])
    };
    (mixed -> $out:ty => $handler:expr, emit_interval = $emit_interval:expr) => {
        $crate::__obzenflow_stateful_typed!(input = mixed, output = $out, name = "__obzenflow_binding_derived_name__", handler = $handler, emit = some($emit_interval), middleware = [])
    };
    (mixed -> $out:ty => $handler:expr, emit_interval = $emit_interval:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_stateful_typed!(input = mixed, output = $out, name = "__obzenflow_binding_derived_name__", handler = $handler, emit = some($emit_interval), middleware = [$($mw),*])
    };

    // ── typed (explicit name override): mixed input ──
    (name: $name:literal, mixed -> $out:ty => placeholder!()) => {
        $crate::__obzenflow_stateful_typed!(input = mixed, output = $out, name = $name, handler = placeholder!(), emit = none, middleware = [])
    };
    (name: $name:literal, mixed -> $out:ty => placeholder!($msg:expr)) => {
        $crate::__obzenflow_stateful_typed!(input = mixed, output = $out, name = $name, handler = placeholder!($msg), emit = none, middleware = [])
    };
    (name: $name:literal, mixed -> $out:ty => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_stateful_typed!(input = mixed, output = $out, name = $name, handler = placeholder!(), emit = none, middleware = [$($mw),*])
    };
    (name: $name:literal, mixed -> $out:ty => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_stateful_typed!(input = mixed, output = $out, name = $name, handler = placeholder!($msg), emit = none, middleware = [$($mw),*])
    };
    (name: $name:literal, mixed -> $out:ty => $handler:expr) => {
        $crate::__obzenflow_stateful_typed!(input = mixed, output = $out, name = $name, handler = $handler, emit = none, middleware = [])
    };
    (name: $name:literal, mixed -> $out:ty => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_stateful_typed!(input = mixed, output = $out, name = $name, handler = $handler, emit = none, middleware = [$($mw),*])
    };
    (name: $name:literal, mixed -> $out:ty => $handler:expr, emit_interval = $emit_interval:expr) => {
        $crate::__obzenflow_stateful_typed!(input = mixed, output = $out, name = $name, handler = $handler, emit = some($emit_interval), middleware = [])
    };
    (name: $name:literal, mixed -> $out:ty => $handler:expr, emit_interval = $emit_interval:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_stateful_typed!(input = mixed, output = $out, name = $name, handler = $handler, emit = some($emit_interval), middleware = [$($mw),*])
    };

    // ── untyped (binding-derived name) ──
    ($handler:expr) => {
        $crate::__obzenflow_stateful_untyped!(name = "__obzenflow_binding_derived_name__", handler = $handler, emit = none, middleware = [])
    };
    ($handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_stateful_untyped!(name = "__obzenflow_binding_derived_name__", handler = $handler, emit = none, middleware = [$($mw),*])
    };
    ($handler:expr, emit_interval = $emit_interval:expr) => {
        $crate::__obzenflow_stateful_untyped!(name = "__obzenflow_binding_derived_name__", handler = $handler, emit = some($emit_interval), middleware = [])
    };
    ($handler:expr, emit_interval = $emit_interval:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_stateful_untyped!(name = "__obzenflow_binding_derived_name__", handler = $handler, emit = some($emit_interval), middleware = [$($mw),*])
    };

    // ── untyped (explicit name override) ──
    (name: $name:literal, $handler:expr) => {
        $crate::__obzenflow_stateful_untyped!(name = $name, handler = $handler, emit = none, middleware = [])
    };
    (name: $name:literal, $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_stateful_untyped!(name = $name, handler = $handler, emit = none, middleware = [$($mw),*])
    };
    (name: $name:literal, $handler:expr, emit_interval = $emit_interval:expr) => {
        $crate::__obzenflow_stateful_untyped!(name = $name, handler = $handler, emit = some($emit_interval), middleware = [])
    };
    (name: $name:literal, $handler:expr, emit_interval = $emit_interval:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_stateful_untyped!(name = $name, handler = $handler, emit = some($emit_interval), middleware = [$($mw),*])
    };

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
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
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
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
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

    // ── real handler: both exact ──
    (reference = exact, stream = exact, output = $out:ty,
     ref_type = ($ref_ty:ty), stream_type = ($str_ty:ty),
     name = $name:literal, ref_var = $ref_var:ident, handler = $handler:expr,
     middleware = [$($mw:expr),*]) => {{
        let __handler = {
            fn __obzenflow_anchor_join<H>(handler: H) -> H
            where
                H: ::obzenflow_runtime::typing::JoinTyping<
                    Reference = $ref_ty,
                    Stream = $str_ty,
                    Output = $out,
                >,
            {
                handler
            }
            __obzenflow_anchor_join($handler)
        };
        ::obzenflow_runtime::typing::assert_join_contract::<_, $ref_ty, $str_ty, $out>(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::join(
            $crate::dsl::typing::TypeHint::exact(stringify!($ref_ty)),
            $crate::dsl::typing::TypeHint::exact(stringify!($str_ty)),
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
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

    // ── real handler: mixed reference, exact stream ──
    (reference = mixed, stream = exact, output = $out:ty,
     ref_type = (), stream_type = ($str_ty:ty),
     name = $name:literal, ref_var = $ref_var:ident, handler = $handler:expr,
     middleware = [$($mw:expr),*]) => {{
        let __handler = {
            fn __obzenflow_anchor_join<H>(handler: H) -> H
            where
                H: ::obzenflow_runtime::typing::JoinTyping<Stream = $str_ty, Output = $out>,
            {
                handler
            }
            __obzenflow_anchor_join($handler)
        };
        ::obzenflow_runtime::typing::assert_join_stream_output::<_, $str_ty, $out>(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::join(
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($str_ty)),
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
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

    // ── real handler: exact reference, mixed stream ──
    (reference = exact, stream = mixed, output = $out:ty,
     ref_type = ($ref_ty:ty), stream_type = (),
     name = $name:literal, ref_var = $ref_var:ident, handler = $handler:expr,
     middleware = [$($mw:expr),*]) => {{
        let __handler = {
            fn __obzenflow_anchor_join<H>(handler: H) -> H
            where
                H: ::obzenflow_runtime::typing::JoinTyping<Reference = $ref_ty, Output = $out>,
            {
                handler
            }
            __obzenflow_anchor_join($handler)
        };
        ::obzenflow_runtime::typing::assert_join_reference_output::<_, $ref_ty, $out>(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::join(
            $crate::dsl::typing::TypeHint::exact(stringify!($ref_ty)),
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
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

    // ── real handler: both mixed ──
    (reference = mixed, stream = mixed, output = $out:ty,
     ref_type = (), stream_type = (),
     name = $name:literal, ref_var = $ref_var:ident, handler = $handler:expr,
     middleware = [$($mw:expr),*]) => {{
        let __handler = {
            fn __obzenflow_anchor_join<H>(handler: H) -> H
            where
                H: ::obzenflow_runtime::typing::JoinTyping<Output = $out>,
            {
                handler
            }
            __obzenflow_anchor_join($handler)
        };
        ::obzenflow_runtime::typing::assert_join_output::<_, $out>(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::join(
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
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
    (mixed) => {
        $crate::dsl::typing::TypeHint::Mixed
    };
    (exact, $ty:ty) => {
        $crate::dsl::typing::TypeHint::exact(stringify!($ty))
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
    (mixed) => {
        ::obzenflow_runtime::typing::MixedInput
    };
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
    (name = $name:literal, ref_var = $ref_var:ident, reference = mixed, $($rest:tt)+) => {
        $crate::__obzenflow_join_exact_stream_contract!(
            @collect
            name = $name,
            ref_var = $ref_var,
            reference = mixed,
            ref_type = (),
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
    // ── typed (binding-derived name) ──
    (catalog $ref_var:ident : $reference:ty, mixed -> $out:ty => placeholder!()) => {
        $crate::__obzenflow_join_typed!(
            reference = exact,
            stream = mixed,
            output = $out,
            ref_type = ($reference),
            stream_type = (),
            name = "__obzenflow_binding_derived_name__",
            ref_var = $ref_var,
            handler = placeholder!(),
            middleware = []
        )
    };
    (catalog $ref_var:ident : $reference:ty, mixed -> $out:ty => placeholder!($msg:expr)) => {
        $crate::__obzenflow_join_typed!(
            reference = exact,
            stream = mixed,
            output = $out,
            ref_type = ($reference),
            stream_type = (),
            name = "__obzenflow_binding_derived_name__",
            ref_var = $ref_var,
            handler = placeholder!($msg),
            middleware = []
        )
    };
    (catalog $ref_var:ident : $reference:ty, mixed -> $out:ty => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = exact,
            stream = mixed,
            output = $out,
            ref_type = ($reference),
            stream_type = (),
            name = "__obzenflow_binding_derived_name__",
            ref_var = $ref_var,
            handler = placeholder!(),
            middleware = [$($mw),*]
        )
    };
    (catalog $ref_var:ident : $reference:ty, mixed -> $out:ty => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = exact,
            stream = mixed,
            output = $out,
            ref_type = ($reference),
            stream_type = (),
            name = "__obzenflow_binding_derived_name__",
            ref_var = $ref_var,
            handler = placeholder!($msg),
            middleware = [$($mw),*]
        )
    };
    (catalog $ref_var:ident : $reference:ty, mixed -> $out:ty => $handler:expr) => {
        $crate::__obzenflow_join_typed!(
            reference = exact,
            stream = mixed,
            output = $out,
            ref_type = ($reference),
            stream_type = (),
            name = "__obzenflow_binding_derived_name__",
            ref_var = $ref_var,
            handler = $handler,
            middleware = []
        )
    };
    (catalog $ref_var:ident : $reference:ty, mixed -> $out:ty => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = exact,
            stream = mixed,
            output = $out,
            ref_type = ($reference),
            stream_type = (),
            name = "__obzenflow_binding_derived_name__",
            ref_var = $ref_var,
            handler = $handler,
            middleware = [$($mw),*]
        )
    };
    (catalog $ref_var:ident : mixed, mixed -> $out:ty => placeholder!()) => {
        $crate::__obzenflow_join_typed!(
            reference = mixed,
            stream = mixed,
            output = $out,
            ref_type = (),
            stream_type = (),
            name = "__obzenflow_binding_derived_name__",
            ref_var = $ref_var,
            handler = placeholder!(),
            middleware = []
        )
    };
    (catalog $ref_var:ident : mixed, mixed -> $out:ty => placeholder!($msg:expr)) => {
        $crate::__obzenflow_join_typed!(
            reference = mixed,
            stream = mixed,
            output = $out,
            ref_type = (),
            stream_type = (),
            name = "__obzenflow_binding_derived_name__",
            ref_var = $ref_var,
            handler = placeholder!($msg),
            middleware = []
        )
    };
    (catalog $ref_var:ident : mixed, mixed -> $out:ty => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = mixed,
            stream = mixed,
            output = $out,
            ref_type = (),
            stream_type = (),
            name = "__obzenflow_binding_derived_name__",
            ref_var = $ref_var,
            handler = placeholder!(),
            middleware = [$($mw),*]
        )
    };
    (catalog $ref_var:ident : mixed, mixed -> $out:ty => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = mixed,
            stream = mixed,
            output = $out,
            ref_type = (),
            stream_type = (),
            name = "__obzenflow_binding_derived_name__",
            ref_var = $ref_var,
            handler = placeholder!($msg),
            middleware = [$($mw),*]
        )
    };
    (catalog $ref_var:ident : mixed, mixed -> $out:ty => $handler:expr) => {
        $crate::__obzenflow_join_typed!(
            reference = mixed,
            stream = mixed,
            output = $out,
            ref_type = (),
            stream_type = (),
            name = "__obzenflow_binding_derived_name__",
            ref_var = $ref_var,
            handler = $handler,
            middleware = []
        )
    };
    (catalog $ref_var:ident : mixed, mixed -> $out:ty => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = mixed,
            stream = mixed,
            output = $out,
            ref_type = (),
            stream_type = (),
            name = "__obzenflow_binding_derived_name__",
            ref_var = $ref_var,
            handler = $handler,
            middleware = [$($mw),*]
        )
    };

    // ── typed (binding-derived name): exact stream ──
    (catalog $ref_var:ident : $reference:ty, $($rest:tt)+) => {
        $crate::__obzenflow_join_exact_stream_contract!(
            name = "__obzenflow_binding_derived_name__",
            ref_var = $ref_var,
            reference = exact($reference),
            $($rest)+
        )
    };
    (catalog $ref_var:ident : mixed, $($rest:tt)+) => {
        $crate::__obzenflow_join_exact_stream_contract!(
            name = "__obzenflow_binding_derived_name__",
            ref_var = $ref_var,
            reference = mixed,
            $($rest)+
        )
    };

    // ── typed (explicit name override) ──
    (name: $name:literal, catalog $ref_var:ident : $reference:ty, mixed -> $out:ty => placeholder!()) => {
        $crate::__obzenflow_join_typed!(
            reference = exact,
            stream = mixed,
            output = $out,
            ref_type = ($reference),
            stream_type = (),
            name = $name,
            ref_var = $ref_var,
            handler = placeholder!(),
            middleware = []
        )
    };
    (name: $name:literal, catalog $ref_var:ident : $reference:ty, mixed -> $out:ty => placeholder!($msg:expr)) => {
        $crate::__obzenflow_join_typed!(
            reference = exact,
            stream = mixed,
            output = $out,
            ref_type = ($reference),
            stream_type = (),
            name = $name,
            ref_var = $ref_var,
            handler = placeholder!($msg),
            middleware = []
        )
    };
    (name: $name:literal, catalog $ref_var:ident : $reference:ty, mixed -> $out:ty => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = exact,
            stream = mixed,
            output = $out,
            ref_type = ($reference),
            stream_type = (),
            name = $name,
            ref_var = $ref_var,
            handler = placeholder!(),
            middleware = [$($mw),*]
        )
    };
    (name: $name:literal, catalog $ref_var:ident : $reference:ty, mixed -> $out:ty => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = exact,
            stream = mixed,
            output = $out,
            ref_type = ($reference),
            stream_type = (),
            name = $name,
            ref_var = $ref_var,
            handler = placeholder!($msg),
            middleware = [$($mw),*]
        )
    };
    (name: $name:literal, catalog $ref_var:ident : $reference:ty, mixed -> $out:ty => $handler:expr) => {
        $crate::__obzenflow_join_typed!(
            reference = exact,
            stream = mixed,
            output = $out,
            ref_type = ($reference),
            stream_type = (),
            name = $name,
            ref_var = $ref_var,
            handler = $handler,
            middleware = []
        )
    };
    (name: $name:literal, catalog $ref_var:ident : $reference:ty, mixed -> $out:ty => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = exact,
            stream = mixed,
            output = $out,
            ref_type = ($reference),
            stream_type = (),
            name = $name,
            ref_var = $ref_var,
            handler = $handler,
            middleware = [$($mw),*]
        )
    };
    (name: $name:literal, catalog $ref_var:ident : mixed, mixed -> $out:ty => placeholder!()) => {
        $crate::__obzenflow_join_typed!(
            reference = mixed,
            stream = mixed,
            output = $out,
            ref_type = (),
            stream_type = (),
            name = $name,
            ref_var = $ref_var,
            handler = placeholder!(),
            middleware = []
        )
    };
    (name: $name:literal, catalog $ref_var:ident : mixed, mixed -> $out:ty => placeholder!($msg:expr)) => {
        $crate::__obzenflow_join_typed!(
            reference = mixed,
            stream = mixed,
            output = $out,
            ref_type = (),
            stream_type = (),
            name = $name,
            ref_var = $ref_var,
            handler = placeholder!($msg),
            middleware = []
        )
    };
    (name: $name:literal, catalog $ref_var:ident : mixed, mixed -> $out:ty => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = mixed,
            stream = mixed,
            output = $out,
            ref_type = (),
            stream_type = (),
            name = $name,
            ref_var = $ref_var,
            handler = placeholder!(),
            middleware = [$($mw),*]
        )
    };
    (name: $name:literal, catalog $ref_var:ident : mixed, mixed -> $out:ty => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = mixed,
            stream = mixed,
            output = $out,
            ref_type = (),
            stream_type = (),
            name = $name,
            ref_var = $ref_var,
            handler = placeholder!($msg),
            middleware = [$($mw),*]
        )
    };
    (name: $name:literal, catalog $ref_var:ident : mixed, mixed -> $out:ty => $handler:expr) => {
        $crate::__obzenflow_join_typed!(
            reference = mixed,
            stream = mixed,
            output = $out,
            ref_type = (),
            stream_type = (),
            name = $name,
            ref_var = $ref_var,
            handler = $handler,
            middleware = []
        )
    };
    (name: $name:literal, catalog $ref_var:ident : mixed, mixed -> $out:ty => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = mixed,
            stream = mixed,
            output = $out,
            ref_type = (),
            stream_type = (),
            name = $name,
            ref_var = $ref_var,
            handler = $handler,
            middleware = [$($mw),*]
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
    (name: $name:literal, catalog $ref_var:ident : mixed, $($rest:tt)+) => {
        $crate::__obzenflow_join_exact_stream_contract!(
            name = $name,
            ref_var = $ref_var,
            reference = mixed,
            $($rest)+
        )
    };

    // ── untyped (binding-derived name) ──
    (catalog $ref_var:ident => $handler:expr) => {
        $crate::__obzenflow_join_untyped!(
            name = "__obzenflow_binding_derived_name__",
            reference_stage_var = $ref_var,
            handler = $handler,
            middleware = []
        )
    };
    (catalog $ref_var:ident => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_join_untyped!(
            name = "__obzenflow_binding_derived_name__",
            reference_stage_var = $ref_var,
            handler = $handler,
            middleware = [$($mw),*]
        )
    };

    // ── untyped (explicit name override) ──
    (name: $name:literal, catalog $ref_var:ident => $handler:expr) => {
        $crate::__obzenflow_join_untyped!(name = $name, reference_stage_var = $ref_var, handler = $handler, middleware = [])
    };
    (name: $name:literal, catalog $ref_var:ident => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_join_untyped!(name = $name, reference_stage_var = $ref_var, handler = $handler, middleware = [$($mw),*])
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
            $crate::dsl::typing::TypeHint::exact(stringify!($($in)+)),
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
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
    (name: $name:literal, $($rest:tt)+) => {
        $crate::__obzenflow_ai_map_reduce_contract!(name = $name, $($rest)+)
    };
    ($($rest:tt)+) => {
        $crate::__obzenflow_ai_map_reduce_contract!(
            name = "__obzenflow_binding_derived_name__",
            $($rest)+
        )
    };
}
