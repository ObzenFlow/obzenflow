// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Stage macros for building ObzenFlow pipeline descriptors.
//!
//! Public macros use labeled syntax:
//!
//! ```ignore
//! source!(out: T; "name" => handler)
//! transform!(input: I, out: O; "name" => handler)
//! stateful!(input: I, out: O; "name" => handler)
//! sink!(input: I; "name" => handler)
//! join!(reference: R, stream: S, out: O; "name" => handler)
//! ```
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
    // ── typed ──
    (out: $out:ty; $name:literal => placeholder!()) => {
        $crate::__obzenflow_source_typed!(output = $out, name = $name, handler = placeholder!(), middleware = [])
    };
    (out: $out:ty; $name:literal => placeholder!($msg:expr)) => {
        $crate::__obzenflow_source_typed!(output = $out, name = $name, handler = placeholder!($msg), middleware = [])
    };
    (out: $out:ty; $name:literal => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_source_typed!(output = $out, name = $name, handler = placeholder!(), middleware = [$($mw),*])
    };
    (out: $out:ty; $name:literal => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_source_typed!(output = $out, name = $name, handler = placeholder!($msg), middleware = [$($mw),*])
    };
    (out: $out:ty; $name:literal => $handler:expr) => {
        $crate::__obzenflow_source_typed!(output = $out, name = $name, handler = $handler, middleware = [])
    };
    (out: $out:ty; $name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_source_typed!(output = $out, name = $name, handler = $handler, middleware = [$($mw),*])
    };
    // ── typed (comma form) ──
    (out: $out:ty, $name:literal => placeholder!()) => {
        $crate::__obzenflow_source_typed!(output = $out, name = $name, handler = placeholder!(), middleware = [])
    };
    (out: $out:ty, $name:literal => placeholder!($msg:expr)) => {
        $crate::__obzenflow_source_typed!(output = $out, name = $name, handler = placeholder!($msg), middleware = [])
    };
    (out: $out:ty, $name:literal => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_source_typed!(output = $out, name = $name, handler = placeholder!(), middleware = [$($mw),*])
    };
    (out: $out:ty, $name:literal => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_source_typed!(output = $out, name = $name, handler = placeholder!($msg), middleware = [$($mw),*])
    };
    (out: $out:ty, $name:literal => $handler:expr) => {
        $crate::__obzenflow_source_typed!(output = $out, name = $name, handler = $handler, middleware = [])
    };
    (out: $out:ty, $name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_source_typed!(output = $out, name = $name, handler = $handler, middleware = [$($mw),*])
    };
    // ── untyped ──
    ($name:literal => $handler:expr) => {
        $crate::__obzenflow_source_untyped!(name = $name, handler = $handler, middleware = [])
    };
    ($name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_source_untyped!(name = $name, handler = $handler, middleware = [$($mw),*])
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
    // ── typed ──
    (out: $out:ty; $name:literal => placeholder!()) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = placeholder!(), middleware = [])
    };
    (out: $out:ty; $name:literal => placeholder!($msg:expr)) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = placeholder!($msg), middleware = [])
    };
    (out: $out:ty; $name:literal => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = placeholder!(), middleware = [$($mw),*])
    };
    (out: $out:ty; $name:literal => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = placeholder!($msg), middleware = [$($mw),*])
    };
    (out: $out:ty; $name:literal => ($handler:expr, $poll_timeout:expr)) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = ($handler, $poll_timeout), middleware = [])
    };
    (out: $out:ty; $name:literal => ($handler:expr, $poll_timeout:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = ($handler, $poll_timeout), middleware = [$($mw),*])
    };
    (out: $out:ty; $name:literal => $handler:expr) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = $handler, middleware = [])
    };
    (out: $out:ty; $name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = $handler, middleware = [$($mw),*])
    };
    // ── typed (comma form) ──
    (out: $out:ty, $name:literal => placeholder!()) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = placeholder!(), middleware = [])
    };
    (out: $out:ty, $name:literal => placeholder!($msg:expr)) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = placeholder!($msg), middleware = [])
    };
    (out: $out:ty, $name:literal => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = placeholder!(), middleware = [$($mw),*])
    };
    (out: $out:ty, $name:literal => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = placeholder!($msg), middleware = [$($mw),*])
    };
    (out: $out:ty, $name:literal => ($handler:expr, $poll_timeout:expr)) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = ($handler, $poll_timeout), middleware = [])
    };
    (out: $out:ty, $name:literal => ($handler:expr, $poll_timeout:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = ($handler, $poll_timeout), middleware = [$($mw),*])
    };
    (out: $out:ty, $name:literal => $handler:expr) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = $handler, middleware = [])
    };
    (out: $out:ty, $name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_async_source_typed!(output = $out, name = $name, handler = $handler, middleware = [$($mw),*])
    };
    // ── untyped ──
    ($name:literal => ($handler:expr, $poll_timeout:expr)) => {
        $crate::__obzenflow_async_source_untyped!(name = $name, handler = ($handler, $poll_timeout), middleware = [])
    };
    ($name:literal => ($handler:expr, $poll_timeout:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_source_untyped!(name = $name, handler = ($handler, $poll_timeout), middleware = [$($mw),*])
    };
    ($name:literal => $handler:expr) => {
        $crate::__obzenflow_async_source_untyped!(name = $name, handler = $handler, middleware = [])
    };
    ($name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_async_source_untyped!(name = $name, handler = $handler, middleware = [$($mw),*])
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
    // ── typed ──
    (out: $out:ty; $name:literal => placeholder!()) => {
        $crate::__obzenflow_infinite_source_typed!(output = $out, name = $name, handler = placeholder!(), middleware = [])
    };
    (out: $out:ty; $name:literal => placeholder!($msg:expr)) => {
        $crate::__obzenflow_infinite_source_typed!(output = $out, name = $name, handler = placeholder!($msg), middleware = [])
    };
    (out: $out:ty; $name:literal => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_infinite_source_typed!(output = $out, name = $name, handler = placeholder!(), middleware = [$($mw),*])
    };
    (out: $out:ty; $name:literal => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_infinite_source_typed!(output = $out, name = $name, handler = placeholder!($msg), middleware = [$($mw),*])
    };
    (out: $out:ty; $name:literal => $handler:expr) => {
        $crate::__obzenflow_infinite_source_typed!(output = $out, name = $name, handler = $handler, middleware = [])
    };
    (out: $out:ty; $name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_infinite_source_typed!(output = $out, name = $name, handler = $handler, middleware = [$($mw),*])
    };
    // ── typed (comma form) ──
    (out: $out:ty, $name:literal => placeholder!()) => {
        $crate::__obzenflow_infinite_source_typed!(output = $out, name = $name, handler = placeholder!(), middleware = [])
    };
    (out: $out:ty, $name:literal => placeholder!($msg:expr)) => {
        $crate::__obzenflow_infinite_source_typed!(output = $out, name = $name, handler = placeholder!($msg), middleware = [])
    };
    (out: $out:ty, $name:literal => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_infinite_source_typed!(output = $out, name = $name, handler = placeholder!(), middleware = [$($mw),*])
    };
    (out: $out:ty, $name:literal => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_infinite_source_typed!(output = $out, name = $name, handler = placeholder!($msg), middleware = [$($mw),*])
    };
    (out: $out:ty, $name:literal => $handler:expr) => {
        $crate::__obzenflow_infinite_source_typed!(output = $out, name = $name, handler = $handler, middleware = [])
    };
    (out: $out:ty, $name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_infinite_source_typed!(output = $out, name = $name, handler = $handler, middleware = [$($mw),*])
    };
    // ── untyped ──
    ($name:literal => $handler:expr) => {
        $crate::__obzenflow_infinite_source_untyped!(name = $name, handler = $handler, middleware = [])
    };
    ($name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_infinite_source_untyped!(name = $name, handler = $handler, middleware = [$($mw),*])
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
    // ── typed ──
    (out: $out:ty; $name:literal => placeholder!()) => {
        $crate::__obzenflow_async_infinite_source_typed!(output = $out, name = $name, handler = placeholder!(), middleware = [])
    };
    (out: $out:ty; $name:literal => placeholder!($msg:expr)) => {
        $crate::__obzenflow_async_infinite_source_typed!(output = $out, name = $name, handler = placeholder!($msg), middleware = [])
    };
    (out: $out:ty; $name:literal => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_infinite_source_typed!(output = $out, name = $name, handler = placeholder!(), middleware = [$($mw),*])
    };
    (out: $out:ty; $name:literal => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_infinite_source_typed!(output = $out, name = $name, handler = placeholder!($msg), middleware = [$($mw),*])
    };
    (out: $out:ty; $name:literal => ($handler:expr, $poll_timeout:expr)) => {
        $crate::__obzenflow_async_infinite_source_typed!(output = $out, name = $name, handler = ($handler, $poll_timeout), middleware = [])
    };
    (out: $out:ty; $name:literal => ($handler:expr, $poll_timeout:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_infinite_source_typed!(output = $out, name = $name, handler = ($handler, $poll_timeout), middleware = [$($mw),*])
    };
    (out: $out:ty; $name:literal => $handler:expr) => {
        $crate::__obzenflow_async_infinite_source_typed!(output = $out, name = $name, handler = $handler, middleware = [])
    };
    (out: $out:ty; $name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_async_infinite_source_typed!(output = $out, name = $name, handler = $handler, middleware = [$($mw),*])
    };
    // ── typed (comma form) ──
    (out: $out:ty, $name:literal => placeholder!()) => {
        $crate::__obzenflow_async_infinite_source_typed!(output = $out, name = $name, handler = placeholder!(), middleware = [])
    };
    (out: $out:ty, $name:literal => placeholder!($msg:expr)) => {
        $crate::__obzenflow_async_infinite_source_typed!(output = $out, name = $name, handler = placeholder!($msg), middleware = [])
    };
    (out: $out:ty, $name:literal => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_infinite_source_typed!(output = $out, name = $name, handler = placeholder!(), middleware = [$($mw),*])
    };
    (out: $out:ty, $name:literal => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_infinite_source_typed!(output = $out, name = $name, handler = placeholder!($msg), middleware = [$($mw),*])
    };
    (out: $out:ty, $name:literal => ($handler:expr, $poll_timeout:expr)) => {
        $crate::__obzenflow_async_infinite_source_typed!(output = $out, name = $name, handler = ($handler, $poll_timeout), middleware = [])
    };
    (out: $out:ty, $name:literal => ($handler:expr, $poll_timeout:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_infinite_source_typed!(output = $out, name = $name, handler = ($handler, $poll_timeout), middleware = [$($mw),*])
    };
    (out: $out:ty, $name:literal => $handler:expr) => {
        $crate::__obzenflow_async_infinite_source_typed!(output = $out, name = $name, handler = $handler, middleware = [])
    };
    (out: $out:ty, $name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_async_infinite_source_typed!(output = $out, name = $name, handler = $handler, middleware = [$($mw),*])
    };
    // ── untyped ──
    ($name:literal => ($handler:expr, $poll_timeout:expr)) => {
        $crate::__obzenflow_async_infinite_source_untyped!(name = $name, handler = ($handler, $poll_timeout), middleware = [])
    };
    ($name:literal => ($handler:expr, $poll_timeout:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_async_infinite_source_untyped!(name = $name, handler = ($handler, $poll_timeout), middleware = [$($mw),*])
    };
    ($name:literal => $handler:expr) => {
        $crate::__obzenflow_async_infinite_source_untyped!(name = $name, handler = $handler, middleware = [])
    };
    ($name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_async_infinite_source_untyped!(name = $name, handler = $handler, middleware = [$($mw),*])
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

/// Create a transform stage descriptor.
#[macro_export]
macro_rules! transform {
    // ── typed: mixed input ──
    (input: mixed, out: $out:ty; $name:literal => placeholder!()) => {
        $crate::__obzenflow_transform_typed!(input = mixed, output = $out, name = $name, handler = placeholder!(), middleware = [])
    };
    (input: mixed, out: $out:ty; $name:literal => placeholder!($msg:expr)) => {
        $crate::__obzenflow_transform_typed!(input = mixed, output = $out, name = $name, handler = placeholder!($msg), middleware = [])
    };
    (input: mixed, out: $out:ty; $name:literal => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_transform_typed!(input = mixed, output = $out, name = $name, handler = placeholder!(), middleware = [$($mw),*])
    };
    (input: mixed, out: $out:ty; $name:literal => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_transform_typed!(input = mixed, output = $out, name = $name, handler = placeholder!($msg), middleware = [$($mw),*])
    };
    (input: mixed, out: $out:ty; $name:literal => $handler:expr) => {
        $crate::__obzenflow_transform_typed!(input = mixed, output = $out, name = $name, handler = $handler, middleware = [])
    };
    (input: mixed, out: $out:ty; $name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_transform_typed!(input = mixed, output = $out, name = $name, handler = $handler, middleware = [$($mw),*])
    };
    // ── typed: exact input ──
    (input: $in:ty, out: $out:ty; $name:literal => placeholder!()) => {
        $crate::__obzenflow_transform_typed!(input = exact($in), output = $out, name = $name, handler = placeholder!(), middleware = [])
    };
    (input: $in:ty, out: $out:ty; $name:literal => placeholder!($msg:expr)) => {
        $crate::__obzenflow_transform_typed!(input = exact($in), output = $out, name = $name, handler = placeholder!($msg), middleware = [])
    };
    (input: $in:ty, out: $out:ty; $name:literal => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_transform_typed!(input = exact($in), output = $out, name = $name, handler = placeholder!(), middleware = [$($mw),*])
    };
    (input: $in:ty, out: $out:ty; $name:literal => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_transform_typed!(input = exact($in), output = $out, name = $name, handler = placeholder!($msg), middleware = [$($mw),*])
    };
    (input: $in:ty, out: $out:ty; $name:literal => $handler:expr) => {
        $crate::__obzenflow_transform_typed!(input = exact($in), output = $out, name = $name, handler = $handler, middleware = [])
    };
    (input: $in:ty, out: $out:ty; $name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_transform_typed!(input = exact($in), output = $out, name = $name, handler = $handler, middleware = [$($mw),*])
    };
    // ── typed (comma form): mixed input ──
    (input: mixed, out: $out:ty, $name:literal => placeholder!()) => {
        $crate::__obzenflow_transform_typed!(input = mixed, output = $out, name = $name, handler = placeholder!(), middleware = [])
    };
    (input: mixed, out: $out:ty, $name:literal => placeholder!($msg:expr)) => {
        $crate::__obzenflow_transform_typed!(input = mixed, output = $out, name = $name, handler = placeholder!($msg), middleware = [])
    };
    (input: mixed, out: $out:ty, $name:literal => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_transform_typed!(input = mixed, output = $out, name = $name, handler = placeholder!(), middleware = [$($mw),*])
    };
    (input: mixed, out: $out:ty, $name:literal => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_transform_typed!(input = mixed, output = $out, name = $name, handler = placeholder!($msg), middleware = [$($mw),*])
    };
    (input: mixed, out: $out:ty, $name:literal => $handler:expr) => {
        $crate::__obzenflow_transform_typed!(input = mixed, output = $out, name = $name, handler = $handler, middleware = [])
    };
    (input: mixed, out: $out:ty, $name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_transform_typed!(input = mixed, output = $out, name = $name, handler = $handler, middleware = [$($mw),*])
    };
    // ── typed (comma form): exact input ──
    (input: $in:ty, out: $out:ty, $name:literal => placeholder!()) => {
        $crate::__obzenflow_transform_typed!(input = exact($in), output = $out, name = $name, handler = placeholder!(), middleware = [])
    };
    (input: $in:ty, out: $out:ty, $name:literal => placeholder!($msg:expr)) => {
        $crate::__obzenflow_transform_typed!(input = exact($in), output = $out, name = $name, handler = placeholder!($msg), middleware = [])
    };
    (input: $in:ty, out: $out:ty, $name:literal => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_transform_typed!(input = exact($in), output = $out, name = $name, handler = placeholder!(), middleware = [$($mw),*])
    };
    (input: $in:ty, out: $out:ty, $name:literal => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_transform_typed!(input = exact($in), output = $out, name = $name, handler = placeholder!($msg), middleware = [$($mw),*])
    };
    (input: $in:ty, out: $out:ty, $name:literal => $handler:expr) => {
        $crate::__obzenflow_transform_typed!(input = exact($in), output = $out, name = $name, handler = $handler, middleware = [])
    };
    (input: $in:ty, out: $out:ty, $name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_transform_typed!(input = exact($in), output = $out, name = $name, handler = $handler, middleware = [$($mw),*])
    };
    // ── untyped ──
    ($name:literal => $handler:expr) => {
        $crate::__obzenflow_transform_untyped!(name = $name, handler = $handler, middleware = [])
    };
    ($name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_transform_untyped!(name = $name, handler = $handler, middleware = [$($mw),*])
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

/// Create an async transform stage descriptor.
#[macro_export]
macro_rules! async_transform {
    // ── typed: mixed input ──
    (input: mixed, out: $out:ty; $name:literal => placeholder!()) => {
        $crate::__obzenflow_async_transform_typed!(input = mixed, output = $out, name = $name, handler = placeholder!(), middleware = [])
    };
    (input: mixed, out: $out:ty; $name:literal => placeholder!($msg:expr)) => {
        $crate::__obzenflow_async_transform_typed!(input = mixed, output = $out, name = $name, handler = placeholder!($msg), middleware = [])
    };
    (input: mixed, out: $out:ty; $name:literal => $handler:expr) => {
        $crate::__obzenflow_async_transform_typed!(input = mixed, output = $out, name = $name, handler = $handler, middleware = [])
    };
    (input: mixed, out: $out:ty; $name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_async_transform_typed!(input = mixed, output = $out, name = $name, handler = $handler, middleware = [$($mw),*])
    };
    // ── typed: exact input ──
    (input: $in:ty, out: $out:ty; $name:literal => placeholder!()) => {
        $crate::__obzenflow_async_transform_typed!(input = exact($in), output = $out, name = $name, handler = placeholder!(), middleware = [])
    };
    (input: $in:ty, out: $out:ty; $name:literal => placeholder!($msg:expr)) => {
        $crate::__obzenflow_async_transform_typed!(input = exact($in), output = $out, name = $name, handler = placeholder!($msg), middleware = [])
    };
    (input: $in:ty, out: $out:ty; $name:literal => $handler:expr) => {
        $crate::__obzenflow_async_transform_typed!(input = exact($in), output = $out, name = $name, handler = $handler, middleware = [])
    };
    (input: $in:ty, out: $out:ty; $name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_async_transform_typed!(input = exact($in), output = $out, name = $name, handler = $handler, middleware = [$($mw),*])
    };
    // ── typed (comma form): mixed input ──
    (input: mixed, out: $out:ty, $name:literal => placeholder!()) => {
        $crate::__obzenflow_async_transform_typed!(input = mixed, output = $out, name = $name, handler = placeholder!(), middleware = [])
    };
    (input: mixed, out: $out:ty, $name:literal => placeholder!($msg:expr)) => {
        $crate::__obzenflow_async_transform_typed!(input = mixed, output = $out, name = $name, handler = placeholder!($msg), middleware = [])
    };
    (input: mixed, out: $out:ty, $name:literal => $handler:expr) => {
        $crate::__obzenflow_async_transform_typed!(input = mixed, output = $out, name = $name, handler = $handler, middleware = [])
    };
    (input: mixed, out: $out:ty, $name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_async_transform_typed!(input = mixed, output = $out, name = $name, handler = $handler, middleware = [$($mw),*])
    };
    // ── typed (comma form): exact input ──
    (input: $in:ty, out: $out:ty, $name:literal => placeholder!()) => {
        $crate::__obzenflow_async_transform_typed!(input = exact($in), output = $out, name = $name, handler = placeholder!(), middleware = [])
    };
    (input: $in:ty, out: $out:ty, $name:literal => placeholder!($msg:expr)) => {
        $crate::__obzenflow_async_transform_typed!(input = exact($in), output = $out, name = $name, handler = placeholder!($msg), middleware = [])
    };
    (input: $in:ty, out: $out:ty, $name:literal => $handler:expr) => {
        $crate::__obzenflow_async_transform_typed!(input = exact($in), output = $out, name = $name, handler = $handler, middleware = [])
    };
    (input: $in:ty, out: $out:ty, $name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_async_transform_typed!(input = exact($in), output = $out, name = $name, handler = $handler, middleware = [$($mw),*])
    };
    // ── untyped ──
    ($name:literal => $handler:expr) => {
        $crate::__obzenflow_async_transform_untyped!(name = $name, handler = $handler, middleware = [])
    };
    ($name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_async_transform_untyped!(name = $name, handler = $handler, middleware = [$($mw),*])
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
    // ── typed: mixed input ──
    (input: mixed; $name:literal => placeholder!()) => {
        $crate::__obzenflow_sink_typed!(input = mixed, name = $name, handler = placeholder!(), middleware = [])
    };
    (input: mixed; $name:literal => placeholder!($msg:expr)) => {
        $crate::__obzenflow_sink_typed!(input = mixed, name = $name, handler = placeholder!($msg), middleware = [])
    };
    (input: mixed; $name:literal => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(input = mixed, name = $name, handler = placeholder!(), middleware = [$($mw),*])
    };
    (input: mixed; $name:literal => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(input = mixed, name = $name, handler = placeholder!($msg), middleware = [$($mw),*])
    };
    (input: mixed; $name:literal => $handler:expr) => {
        $crate::__obzenflow_sink_typed!(input = mixed, name = $name, handler = $handler, middleware = [])
    };
    (input: mixed; $name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(input = mixed, name = $name, handler = $handler, middleware = [$($mw),*])
    };
    // ── typed: exact input ──
    (input: $in:ty; $name:literal => placeholder!()) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = $name, handler = placeholder!(), middleware = [])
    };
    (input: $in:ty; $name:literal => placeholder!($msg:expr)) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = $name, handler = placeholder!($msg), middleware = [])
    };
    (input: $in:ty; $name:literal => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = $name, handler = placeholder!(), middleware = [$($mw),*])
    };
    (input: $in:ty; $name:literal => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = $name, handler = placeholder!($msg), middleware = [$($mw),*])
    };
    (input: $in:ty; $name:literal => $handler:expr) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = $name, handler = $handler, middleware = [])
    };
    (input: $in:ty; $name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = $name, handler = $handler, middleware = [$($mw),*])
    };
    // ── typed (comma form): mixed input ──
    (input: mixed, $name:literal => placeholder!()) => {
        $crate::__obzenflow_sink_typed!(input = mixed, name = $name, handler = placeholder!(), middleware = [])
    };
    (input: mixed, $name:literal => placeholder!($msg:expr)) => {
        $crate::__obzenflow_sink_typed!(input = mixed, name = $name, handler = placeholder!($msg), middleware = [])
    };
    (input: mixed, $name:literal => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(input = mixed, name = $name, handler = placeholder!(), middleware = [$($mw),*])
    };
    (input: mixed, $name:literal => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(input = mixed, name = $name, handler = placeholder!($msg), middleware = [$($mw),*])
    };
    (input: mixed, $name:literal => $handler:expr) => {
        $crate::__obzenflow_sink_typed!(input = mixed, name = $name, handler = $handler, middleware = [])
    };
    (input: mixed, $name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(input = mixed, name = $name, handler = $handler, middleware = [$($mw),*])
    };
    // ── typed (comma form): exact input ──
    (input: $in:ty, $name:literal => placeholder!()) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = $name, handler = placeholder!(), middleware = [])
    };
    (input: $in:ty, $name:literal => placeholder!($msg:expr)) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = $name, handler = placeholder!($msg), middleware = [])
    };
    (input: $in:ty, $name:literal => placeholder!(), [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = $name, handler = placeholder!(), middleware = [$($mw),*])
    };
    (input: $in:ty, $name:literal => placeholder!($msg:expr), [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = $name, handler = placeholder!($msg), middleware = [$($mw),*])
    };
    (input: $in:ty, $name:literal => $handler:expr) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = $name, handler = $handler, middleware = [])
    };
    (input: $in:ty, $name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_typed!(input = exact($in), name = $name, handler = $handler, middleware = [$($mw),*])
    };
    // ── untyped (closure shorthand) ──
    ($name:literal => |$arg:ident : $ty:ty| $body:block) => {
        $crate::__obzenflow_sink_untyped!(name = $name, handler = |$arg: $ty| $body, middleware = [])
    };
    ($name:literal => move |$arg:ident : $ty:ty| $body:block) => {
        $crate::__obzenflow_sink_untyped!(name = $name, handler = move |$arg: $ty| $body, middleware = [])
    };
    ($name:literal => |$arg:ident : $ty:ty| $body:block, [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_untyped!(name = $name, handler = |$arg: $ty| $body, middleware = [$($mw),*])
    };
    ($name:literal => move |$arg:ident : $ty:ty| $body:block, [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_untyped!(name = $name, handler = move |$arg: $ty| $body, middleware = [$($mw),*])
    };
    // ── untyped (handler) ──
    ($name:literal => $handler:expr) => {
        $crate::__obzenflow_sink_untyped!(name = $name, handler = $handler, middleware = [])
    };
    ($name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_sink_untyped!(name = $name, handler = $handler, middleware = [$($mw),*])
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

/// Create a stateful stage descriptor.
#[macro_export]
macro_rules! stateful {
    // ── typed: mixed input ──
    (input: mixed, out: $out:ty; $name:literal => placeholder!()) => {
        $crate::__obzenflow_stateful_typed!(input = mixed, output = $out, name = $name, handler = placeholder!(), emit = none, middleware = [])
    };
    (input: mixed, out: $out:ty; $name:literal => placeholder!($msg:expr)) => {
        $crate::__obzenflow_stateful_typed!(input = mixed, output = $out, name = $name, handler = placeholder!($msg), emit = none, middleware = [])
    };
    (input: mixed, out: $out:ty; $name:literal => $handler:expr) => {
        $crate::__obzenflow_stateful_typed!(input = mixed, output = $out, name = $name, handler = $handler, emit = none, middleware = [])
    };
    (input: mixed, out: $out:ty; $name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_stateful_typed!(input = mixed, output = $out, name = $name, handler = $handler, emit = none, middleware = [$($mw),*])
    };
    (input: mixed, out: $out:ty; $name:literal => $handler:expr, emit_interval = $emit_interval:expr) => {
        $crate::__obzenflow_stateful_typed!(input = mixed, output = $out, name = $name, handler = $handler, emit = some($emit_interval), middleware = [])
    };
    (input: mixed, out: $out:ty; $name:literal => $handler:expr, emit_interval = $emit_interval:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_stateful_typed!(input = mixed, output = $out, name = $name, handler = $handler, emit = some($emit_interval), middleware = [$($mw),*])
    };
    // ── typed: exact input ──
    (input: $in:ty, out: $out:ty; $name:literal => placeholder!()) => {
        $crate::__obzenflow_stateful_typed!(input = exact($in), output = $out, name = $name, handler = placeholder!(), emit = none, middleware = [])
    };
    (input: $in:ty, out: $out:ty; $name:literal => placeholder!($msg:expr)) => {
        $crate::__obzenflow_stateful_typed!(input = exact($in), output = $out, name = $name, handler = placeholder!($msg), emit = none, middleware = [])
    };
    (input: $in:ty, out: $out:ty; $name:literal => $handler:expr) => {
        $crate::__obzenflow_stateful_typed!(input = exact($in), output = $out, name = $name, handler = $handler, emit = none, middleware = [])
    };
    (input: $in:ty, out: $out:ty; $name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_stateful_typed!(input = exact($in), output = $out, name = $name, handler = $handler, emit = none, middleware = [$($mw),*])
    };
    (input: $in:ty, out: $out:ty; $name:literal => $handler:expr, emit_interval = $emit_interval:expr) => {
        $crate::__obzenflow_stateful_typed!(input = exact($in), output = $out, name = $name, handler = $handler, emit = some($emit_interval), middleware = [])
    };
    (input: $in:ty, out: $out:ty; $name:literal => $handler:expr, emit_interval = $emit_interval:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_stateful_typed!(input = exact($in), output = $out, name = $name, handler = $handler, emit = some($emit_interval), middleware = [$($mw),*])
    };
    // ── typed (comma form): mixed input ──
    (input: mixed, out: $out:ty, $name:literal => placeholder!()) => {
        $crate::__obzenflow_stateful_typed!(input = mixed, output = $out, name = $name, handler = placeholder!(), emit = none, middleware = [])
    };
    (input: mixed, out: $out:ty, $name:literal => placeholder!($msg:expr)) => {
        $crate::__obzenflow_stateful_typed!(input = mixed, output = $out, name = $name, handler = placeholder!($msg), emit = none, middleware = [])
    };
    (input: mixed, out: $out:ty, $name:literal => $handler:expr) => {
        $crate::__obzenflow_stateful_typed!(input = mixed, output = $out, name = $name, handler = $handler, emit = none, middleware = [])
    };
    (input: mixed, out: $out:ty, $name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_stateful_typed!(input = mixed, output = $out, name = $name, handler = $handler, emit = none, middleware = [$($mw),*])
    };
    (input: mixed, out: $out:ty, $name:literal => $handler:expr, emit_interval = $emit_interval:expr) => {
        $crate::__obzenflow_stateful_typed!(input = mixed, output = $out, name = $name, handler = $handler, emit = some($emit_interval), middleware = [])
    };
    (input: mixed, out: $out:ty, $name:literal => $handler:expr, emit_interval = $emit_interval:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_stateful_typed!(input = mixed, output = $out, name = $name, handler = $handler, emit = some($emit_interval), middleware = [$($mw),*])
    };
    // ── typed (comma form): exact input ──
    (input: $in:ty, out: $out:ty, $name:literal => placeholder!()) => {
        $crate::__obzenflow_stateful_typed!(input = exact($in), output = $out, name = $name, handler = placeholder!(), emit = none, middleware = [])
    };
    (input: $in:ty, out: $out:ty, $name:literal => placeholder!($msg:expr)) => {
        $crate::__obzenflow_stateful_typed!(input = exact($in), output = $out, name = $name, handler = placeholder!($msg), emit = none, middleware = [])
    };
    (input: $in:ty, out: $out:ty, $name:literal => $handler:expr) => {
        $crate::__obzenflow_stateful_typed!(input = exact($in), output = $out, name = $name, handler = $handler, emit = none, middleware = [])
    };
    (input: $in:ty, out: $out:ty, $name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_stateful_typed!(input = exact($in), output = $out, name = $name, handler = $handler, emit = none, middleware = [$($mw),*])
    };
    (input: $in:ty, out: $out:ty, $name:literal => $handler:expr, emit_interval = $emit_interval:expr) => {
        $crate::__obzenflow_stateful_typed!(input = exact($in), output = $out, name = $name, handler = $handler, emit = some($emit_interval), middleware = [])
    };
    (input: $in:ty, out: $out:ty, $name:literal => $handler:expr, emit_interval = $emit_interval:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_stateful_typed!(input = exact($in), output = $out, name = $name, handler = $handler, emit = some($emit_interval), middleware = [$($mw),*])
    };
    // ── untyped ──
    ($name:literal => $handler:expr, emit_interval = $emit_interval:expr) => {
        $crate::__obzenflow_stateful_untyped!(name = $name, handler = $handler, emit = some($emit_interval), middleware = [])
    };
    ($name:literal => $handler:expr, emit_interval = $emit_interval:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_stateful_untyped!(name = $name, handler = $handler, emit = some($emit_interval), middleware = [$($mw),*])
    };
    ($name:literal => $handler:expr) => {
        $crate::__obzenflow_stateful_untyped!(name = $name, handler = $handler, emit = none, middleware = [])
    };
    ($name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_stateful_untyped!(name = $name, handler = $handler, emit = none, middleware = [$($mw),*])
    };
}

// ============================================================================
// join!  +  __obzenflow_join_typed!  +  with_ref!
// ============================================================================

/// Helper struct to pass reference stage variable and handler to join! macro.
pub struct JoinWithRef<H> {
    pub reference_stage_var: &'static str,
    pub handler: H,
}

/// Create a JoinWithRef struct for use with join! macro.
#[macro_export]
macro_rules! with_ref {
    ($ref_var:ident, $handler:expr) => {
        $crate::dsl::JoinWithRef {
            reference_stage_var: stringify!($ref_var),
            handler: $handler,
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_join_untyped {
    (name = $name:literal, join_with_ref = $join_with_ref:expr, middleware = [$($mw:expr),*]) => {{
        use $crate::dsl::stage_descriptor::{JoinDescriptor, StageDescriptor};
        use obzenflow_core::id::StageId;
        let jwr = $join_with_ref;
        Box::new(JoinDescriptor {
            name: $name.to_string(),
            reference_stage_id: StageId::new(),
            reference_stage_var: Some(jwr.reference_stage_var),
            handler: jwr.handler,
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
            join_with_ref = $crate::with_ref!(
                $ref_var,
                $crate::dsl::typing::PlaceholderJoin::<
                    $crate::__obzenflow_join_phantom_type!($ref_hint $(, $ref_ty)?),
                    $crate::__obzenflow_join_phantom_type!($str_hint $(, $str_ty)?),
                    $out
                >::new(None)
            ),
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
            join_with_ref = $crate::with_ref!(
                $ref_var,
                $crate::dsl::typing::PlaceholderJoin::<
                    $crate::__obzenflow_join_phantom_type!($ref_hint $(, $ref_ty)?),
                    $crate::__obzenflow_join_phantom_type!($str_hint $(, $str_ty)?),
                    $out
                >::new(Some($msg))
            ),
            middleware = [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // ── real handler: both exact ──
    (reference = exact, stream = exact, output = $out:ty,
     ref_type = ($ref_ty:ty), stream_type = ($str_ty:ty),
     name = $name:literal, join_with_ref = $join_with_ref:expr,
     middleware = [$($mw:expr),*]) => {{
        let __join_with_ref = $join_with_ref;
        ::obzenflow_runtime::typing::assert_join_contract::<_, $ref_ty, $str_ty, $out>(&__join_with_ref.handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::join(
            $crate::dsl::typing::TypeHint::exact(stringify!($ref_ty)),
            $crate::dsl::typing::TypeHint::exact(stringify!($str_ty)),
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            false,
            None,
        );
        let __descriptor = $crate::__obzenflow_join_untyped!(name = $name, join_with_ref = __join_with_ref, middleware = [$($mw),*]);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // ── real handler: mixed reference, exact stream ──
    (reference = mixed, stream = exact, output = $out:ty,
     ref_type = (), stream_type = ($str_ty:ty),
     name = $name:literal, join_with_ref = $join_with_ref:expr,
     middleware = [$($mw:expr),*]) => {{
        let __join_with_ref = $join_with_ref;
        ::obzenflow_runtime::typing::assert_join_stream_output::<_, $str_ty, $out>(&__join_with_ref.handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::join(
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($str_ty)),
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            false,
            None,
        );
        let __descriptor = $crate::__obzenflow_join_untyped!(name = $name, join_with_ref = __join_with_ref, middleware = [$($mw),*]);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // ── real handler: exact reference, mixed stream ──
    (reference = exact, stream = mixed, output = $out:ty,
     ref_type = ($ref_ty:ty), stream_type = (),
     name = $name:literal, join_with_ref = $join_with_ref:expr,
     middleware = [$($mw:expr),*]) => {{
        let __join_with_ref = $join_with_ref;
        ::obzenflow_runtime::typing::assert_join_reference_output::<_, $ref_ty, $out>(&__join_with_ref.handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::join(
            $crate::dsl::typing::TypeHint::exact(stringify!($ref_ty)),
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            false,
            None,
        );
        let __descriptor = $crate::__obzenflow_join_untyped!(name = $name, join_with_ref = __join_with_ref, middleware = [$($mw),*]);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    // ── real handler: both mixed ──
    (reference = mixed, stream = mixed, output = $out:ty,
     ref_type = (), stream_type = (),
     name = $name:literal, join_with_ref = $join_with_ref:expr,
     middleware = [$($mw:expr),*]) => {{
        let __join_with_ref = $join_with_ref;
        ::obzenflow_runtime::typing::assert_join_output::<_, $out>(&__join_with_ref.handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::join(
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            false,
            None,
        );
        let __descriptor = $crate::__obzenflow_join_untyped!(name = $name, join_with_ref = __join_with_ref, middleware = [$($mw),*]);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_join_hint {
    (mixed) => { $crate::dsl::typing::TypeHint::Mixed };
    (exact, $ty:ty) => { $crate::dsl::typing::TypeHint::exact(stringify!($ty)) };
    (exact) => { compile_error!("__obzenflow_join_hint!(exact) requires a type; this is a bug in the macro dispatch") };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_join_phantom_type {
    (mixed) => { ::obzenflow_runtime::typing::MixedInput };
    (exact, $ty:ty) => { $ty };
}

/// Create a join stage descriptor.
#[macro_export]
macro_rules! join {
    // ── typed: both exact, placeholder ──
    (reference: $reference:ty, stream: $stream:ty, out: $out:ty; $name:literal => with_ref!($ref_var:ident, placeholder!())) => {
        $crate::__obzenflow_join_typed!(
            reference = exact, stream = exact, output = $out,
            ref_type = ($reference), stream_type = ($stream),
            name = $name, ref_var = $ref_var, handler = placeholder!(),
            middleware = []
        )
    };
    (reference: $reference:ty, stream: $stream:ty, out: $out:ty; $name:literal => with_ref!($ref_var:ident, placeholder!($msg:expr))) => {
        $crate::__obzenflow_join_typed!(
            reference = exact, stream = exact, output = $out,
            ref_type = ($reference), stream_type = ($stream),
            name = $name, ref_var = $ref_var, handler = placeholder!($msg),
            middleware = []
        )
    };
    (reference: $reference:ty, stream: $stream:ty, out: $out:ty; $name:literal => with_ref!($ref_var:ident, placeholder!()), [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = exact, stream = exact, output = $out,
            ref_type = ($reference), stream_type = ($stream),
            name = $name, ref_var = $ref_var, handler = placeholder!(),
            middleware = [$($mw),*]
        )
    };
    (reference: $reference:ty, stream: $stream:ty, out: $out:ty; $name:literal => with_ref!($ref_var:ident, placeholder!($msg:expr)), [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = exact, stream = exact, output = $out,
            ref_type = ($reference), stream_type = ($stream),
            name = $name, ref_var = $ref_var, handler = placeholder!($msg),
            middleware = [$($mw),*]
        )
    };
    // ── typed: both exact, real handler ──
    (reference: $reference:ty, stream: $stream:ty, out: $out:ty; $name:literal => $join_with_ref:expr) => {
        $crate::__obzenflow_join_typed!(
            reference = exact, stream = exact, output = $out,
            ref_type = ($reference), stream_type = ($stream),
            name = $name, join_with_ref = $join_with_ref,
            middleware = []
        )
    };
    (reference: $reference:ty, stream: $stream:ty, out: $out:ty; $name:literal => $join_with_ref:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = exact, stream = exact, output = $out,
            ref_type = ($reference), stream_type = ($stream),
            name = $name, join_with_ref = $join_with_ref,
            middleware = [$($mw),*]
        )
    };
    // ── typed: mixed reference, exact stream ──
    (reference: mixed, stream: $stream:ty, out: $out:ty; $name:literal => with_ref!($ref_var:ident, placeholder!())) => {
        $crate::__obzenflow_join_typed!(
            reference = mixed, stream = exact, output = $out,
            ref_type = (), stream_type = ($stream),
            name = $name, ref_var = $ref_var, handler = placeholder!(),
            middleware = []
        )
    };
    (reference: mixed, stream: $stream:ty, out: $out:ty; $name:literal => with_ref!($ref_var:ident, placeholder!($msg:expr))) => {
        $crate::__obzenflow_join_typed!(
            reference = mixed, stream = exact, output = $out,
            ref_type = (), stream_type = ($stream),
            name = $name, ref_var = $ref_var, handler = placeholder!($msg),
            middleware = []
        )
    };
    (reference: mixed, stream: $stream:ty, out: $out:ty; $name:literal => $join_with_ref:expr) => {
        $crate::__obzenflow_join_typed!(
            reference = mixed, stream = exact, output = $out,
            ref_type = (), stream_type = ($stream),
            name = $name, join_with_ref = $join_with_ref,
            middleware = []
        )
    };
    (reference: mixed, stream: $stream:ty, out: $out:ty; $name:literal => $join_with_ref:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = mixed, stream = exact, output = $out,
            ref_type = (), stream_type = ($stream),
            name = $name, join_with_ref = $join_with_ref,
            middleware = [$($mw),*]
        )
    };
    // ── typed: exact reference, mixed stream ──
    (reference: $reference:ty, stream: mixed, out: $out:ty; $name:literal => with_ref!($ref_var:ident, placeholder!())) => {
        $crate::__obzenflow_join_typed!(
            reference = exact, stream = mixed, output = $out,
            ref_type = ($reference), stream_type = (),
            name = $name, ref_var = $ref_var, handler = placeholder!(),
            middleware = []
        )
    };
    (reference: $reference:ty, stream: mixed, out: $out:ty; $name:literal => with_ref!($ref_var:ident, placeholder!($msg:expr))) => {
        $crate::__obzenflow_join_typed!(
            reference = exact, stream = mixed, output = $out,
            ref_type = ($reference), stream_type = (),
            name = $name, ref_var = $ref_var, handler = placeholder!($msg),
            middleware = []
        )
    };
    (reference: $reference:ty, stream: mixed, out: $out:ty; $name:literal => $join_with_ref:expr) => {
        $crate::__obzenflow_join_typed!(
            reference = exact, stream = mixed, output = $out,
            ref_type = ($reference), stream_type = (),
            name = $name, join_with_ref = $join_with_ref,
            middleware = []
        )
    };
    (reference: $reference:ty, stream: mixed, out: $out:ty; $name:literal => $join_with_ref:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = exact, stream = mixed, output = $out,
            ref_type = ($reference), stream_type = (),
            name = $name, join_with_ref = $join_with_ref,
            middleware = [$($mw),*]
        )
    };
    // ── typed: both mixed ──
    (reference: mixed, stream: mixed, out: $out:ty; $name:literal => with_ref!($ref_var:ident, placeholder!())) => {
        $crate::__obzenflow_join_typed!(
            reference = mixed, stream = mixed, output = $out,
            ref_type = (), stream_type = (),
            name = $name, ref_var = $ref_var, handler = placeholder!(),
            middleware = []
        )
    };
    (reference: mixed, stream: mixed, out: $out:ty; $name:literal => with_ref!($ref_var:ident, placeholder!($msg:expr))) => {
        $crate::__obzenflow_join_typed!(
            reference = mixed, stream = mixed, output = $out,
            ref_type = (), stream_type = (),
            name = $name, ref_var = $ref_var, handler = placeholder!($msg),
            middleware = []
        )
    };
    (reference: mixed, stream: mixed, out: $out:ty; $name:literal => $join_with_ref:expr) => {
        $crate::__obzenflow_join_typed!(
            reference = mixed, stream = mixed, output = $out,
            ref_type = (), stream_type = (),
            name = $name, join_with_ref = $join_with_ref,
            middleware = []
        )
    };
    (reference: mixed, stream: mixed, out: $out:ty; $name:literal => $join_with_ref:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = mixed, stream = mixed, output = $out,
            ref_type = (), stream_type = (),
            name = $name, join_with_ref = $join_with_ref,
            middleware = [$($mw),*]
        )
    };
    // ── typed (comma form) ──
    // both exact, placeholder
    (reference: $reference:ty, stream: $stream:ty, out: $out:ty, $name:literal => with_ref!($ref_var:ident, placeholder!())) => {
        $crate::__obzenflow_join_typed!(
            reference = exact, stream = exact, output = $out,
            ref_type = ($reference), stream_type = ($stream),
            name = $name, ref_var = $ref_var, handler = placeholder!(),
            middleware = []
        )
    };
    (reference: $reference:ty, stream: $stream:ty, out: $out:ty, $name:literal => with_ref!($ref_var:ident, placeholder!($msg:expr))) => {
        $crate::__obzenflow_join_typed!(
            reference = exact, stream = exact, output = $out,
            ref_type = ($reference), stream_type = ($stream),
            name = $name, ref_var = $ref_var, handler = placeholder!($msg),
            middleware = []
        )
    };
    (reference: $reference:ty, stream: $stream:ty, out: $out:ty, $name:literal => with_ref!($ref_var:ident, placeholder!()), [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = exact, stream = exact, output = $out,
            ref_type = ($reference), stream_type = ($stream),
            name = $name, ref_var = $ref_var, handler = placeholder!(),
            middleware = [$($mw),*]
        )
    };
    (reference: $reference:ty, stream: $stream:ty, out: $out:ty, $name:literal => with_ref!($ref_var:ident, placeholder!($msg:expr)), [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = exact, stream = exact, output = $out,
            ref_type = ($reference), stream_type = ($stream),
            name = $name, ref_var = $ref_var, handler = placeholder!($msg),
            middleware = [$($mw),*]
        )
    };
    // both exact, real handler
    (reference: $reference:ty, stream: $stream:ty, out: $out:ty, $name:literal => $join_with_ref:expr) => {
        $crate::__obzenflow_join_typed!(
            reference = exact, stream = exact, output = $out,
            ref_type = ($reference), stream_type = ($stream),
            name = $name, join_with_ref = $join_with_ref,
            middleware = []
        )
    };
    (reference: $reference:ty, stream: $stream:ty, out: $out:ty, $name:literal => $join_with_ref:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = exact, stream = exact, output = $out,
            ref_type = ($reference), stream_type = ($stream),
            name = $name, join_with_ref = $join_with_ref,
            middleware = [$($mw),*]
        )
    };
    // mixed reference, exact stream
    (reference: mixed, stream: $stream:ty, out: $out:ty, $name:literal => with_ref!($ref_var:ident, placeholder!())) => {
        $crate::__obzenflow_join_typed!(
            reference = mixed, stream = exact, output = $out,
            ref_type = (), stream_type = ($stream),
            name = $name, ref_var = $ref_var, handler = placeholder!(),
            middleware = []
        )
    };
    (reference: mixed, stream: $stream:ty, out: $out:ty, $name:literal => with_ref!($ref_var:ident, placeholder!($msg:expr))) => {
        $crate::__obzenflow_join_typed!(
            reference = mixed, stream = exact, output = $out,
            ref_type = (), stream_type = ($stream),
            name = $name, ref_var = $ref_var, handler = placeholder!($msg),
            middleware = []
        )
    };
    (reference: mixed, stream: $stream:ty, out: $out:ty, $name:literal => $join_with_ref:expr) => {
        $crate::__obzenflow_join_typed!(
            reference = mixed, stream = exact, output = $out,
            ref_type = (), stream_type = ($stream),
            name = $name, join_with_ref = $join_with_ref,
            middleware = []
        )
    };
    (reference: mixed, stream: $stream:ty, out: $out:ty, $name:literal => $join_with_ref:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = mixed, stream = exact, output = $out,
            ref_type = (), stream_type = ($stream),
            name = $name, join_with_ref = $join_with_ref,
            middleware = [$($mw),*]
        )
    };
    // exact reference, mixed stream
    (reference: $reference:ty, stream: mixed, out: $out:ty, $name:literal => with_ref!($ref_var:ident, placeholder!())) => {
        $crate::__obzenflow_join_typed!(
            reference = exact, stream = mixed, output = $out,
            ref_type = ($reference), stream_type = (),
            name = $name, ref_var = $ref_var, handler = placeholder!(),
            middleware = []
        )
    };
    (reference: $reference:ty, stream: mixed, out: $out:ty, $name:literal => with_ref!($ref_var:ident, placeholder!($msg:expr))) => {
        $crate::__obzenflow_join_typed!(
            reference = exact, stream = mixed, output = $out,
            ref_type = ($reference), stream_type = (),
            name = $name, ref_var = $ref_var, handler = placeholder!($msg),
            middleware = []
        )
    };
    (reference: $reference:ty, stream: mixed, out: $out:ty, $name:literal => $join_with_ref:expr) => {
        $crate::__obzenflow_join_typed!(
            reference = exact, stream = mixed, output = $out,
            ref_type = ($reference), stream_type = (),
            name = $name, join_with_ref = $join_with_ref,
            middleware = []
        )
    };
    (reference: $reference:ty, stream: mixed, out: $out:ty, $name:literal => $join_with_ref:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = exact, stream = mixed, output = $out,
            ref_type = ($reference), stream_type = (),
            name = $name, join_with_ref = $join_with_ref,
            middleware = [$($mw),*]
        )
    };
    // both mixed
    (reference: mixed, stream: mixed, out: $out:ty, $name:literal => with_ref!($ref_var:ident, placeholder!())) => {
        $crate::__obzenflow_join_typed!(
            reference = mixed, stream = mixed, output = $out,
            ref_type = (), stream_type = (),
            name = $name, ref_var = $ref_var, handler = placeholder!(),
            middleware = []
        )
    };
    (reference: mixed, stream: mixed, out: $out:ty, $name:literal => with_ref!($ref_var:ident, placeholder!($msg:expr))) => {
        $crate::__obzenflow_join_typed!(
            reference = mixed, stream = mixed, output = $out,
            ref_type = (), stream_type = (),
            name = $name, ref_var = $ref_var, handler = placeholder!($msg),
            middleware = []
        )
    };
    (reference: mixed, stream: mixed, out: $out:ty, $name:literal => $join_with_ref:expr) => {
        $crate::__obzenflow_join_typed!(
            reference = mixed, stream = mixed, output = $out,
            ref_type = (), stream_type = (),
            name = $name, join_with_ref = $join_with_ref,
            middleware = []
        )
    };
    (reference: mixed, stream: mixed, out: $out:ty, $name:literal => $join_with_ref:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_join_typed!(
            reference = mixed, stream = mixed, output = $out,
            ref_type = (), stream_type = (),
            name = $name, join_with_ref = $join_with_ref,
            middleware = [$($mw),*]
        )
    };
    // ── untyped ──
    ($name:literal => $join_with_ref:expr) => {
        $crate::__obzenflow_join_untyped!(name = $name, join_with_ref = $join_with_ref, middleware = [])
    };
    ($name:literal => $join_with_ref:expr, [$($mw:expr),*]) => {
        $crate::__obzenflow_join_untyped!(name = $name, join_with_ref = $join_with_ref, middleware = [$($mw),*])
    };
}
