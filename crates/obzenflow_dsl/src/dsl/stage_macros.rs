// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Type-explicit macros for creating stage descriptors.

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

/// Create a finite source stage descriptor.
#[macro_export]
macro_rules! source {
    (@untyped $name:literal => $handler:expr, [$($mw:expr),*]) => {{
        use $crate::dsl::stage_descriptor::{FiniteSourceDescriptor, StageDescriptor};
        Box::new(FiniteSourceDescriptor {
            name: $name.to_string(),
            handler: $handler,
            middleware: vec![$(Box::new($mw)),*],
        }) as Box<dyn StageDescriptor>
    }};

    (<mixed> $name:literal => $handler:expr $(, [$($mw:expr),*])?) => {
        compile_error!("`mixed` is only valid in input positions; source outputs must be exact")
    };
    (<$out:ty> $name:literal => placeholder!()) => {
        $crate::source!(<$out> $name => placeholder!(), [])
    };
    (<$out:ty> $name:literal => placeholder!(), [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            None,
        );
        let __descriptor = $crate::source!(
            @untyped
            $name => $crate::dsl::typing::PlaceholderFiniteSource::<$out>::new(None),
            [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<$out:ty> $name:literal => placeholder!($msg:expr)) => {
        $crate::source!(<$out> $name => placeholder!($msg), [])
    };
    (<$out:ty> $name:literal => placeholder!($msg:expr), [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::source!(
            @untyped
            $name => $crate::dsl::typing::PlaceholderFiniteSource::<$out>::new(Some($msg)),
            [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<$out:ty> $name:literal => $handler:expr) => {
        $crate::source!(<$out> $name => $handler, [])
    };
    (<$out:ty> $name:literal => $handler:expr, [$($mw:expr),*]) => {{
        let __handler = $handler;
        ::obzenflow_runtime::typing::assert_source_output::<_, $out>(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            false,
            None,
        );
        let __descriptor = $crate::source!(@untyped $name => __handler, [$($mw),*]);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};

    ($name:literal => $handler:expr) => {
        $crate::source!(@untyped $name => $handler, [])
    };
    ($name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::source!(@untyped $name => $handler, [$($mw),*])
    };
}

/// Create an async finite source stage descriptor.
#[macro_export]
macro_rules! async_source {
    (@untyped $name:literal => ($handler:expr, $poll_timeout:expr), [$($mw:expr),*]) => {{
        $crate::dsl::stage_descriptor::AsyncFiniteSourceDescriptor::new($name, $handler)
            .with_poll_timeout($poll_timeout)
            $(.with_middleware($mw))*
            .build()
    }};
    (@untyped $name:literal => $handler:expr, [$($mw:expr),*]) => {{
        $crate::dsl::stage_descriptor::AsyncFiniteSourceDescriptor::new($name, $handler)
            $(.with_middleware($mw))*
            .build()
    }};

    (<mixed> $name:literal => $handler:expr $(, [$($mw:expr),*])?) => {
        compile_error!("`mixed` is only valid in input positions; source outputs must be exact")
    };
    (<$out:ty> $name:literal => placeholder!()) => {
        $crate::async_source!(<$out> $name => placeholder!(), [])
    };
    (<$out:ty> $name:literal => placeholder!(), [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            None,
        );
        let __descriptor = $crate::async_source!(
            @untyped
            $name => $crate::dsl::typing::PlaceholderAsyncSource::<$out>::new(None),
            [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<$out:ty> $name:literal => placeholder!($msg:expr)) => {
        $crate::async_source!(<$out> $name => placeholder!($msg), [])
    };
    (<$out:ty> $name:literal => placeholder!($msg:expr), [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::async_source!(
            @untyped
            $name => $crate::dsl::typing::PlaceholderAsyncSource::<$out>::new(Some($msg)),
            [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<$out:ty> $name:literal => placeholder!(), ($poll_timeout:expr)) => {
        compile_error!("typed async_source! timeout syntax is `=> (handler, timeout)`")
    };
    (<$out:ty> $name:literal => ($handler:expr, $poll_timeout:expr)) => {
        $crate::async_source!(<$out> $name => ($handler, $poll_timeout), [])
    };
    (<$out:ty> $name:literal => ($handler:expr, $poll_timeout:expr), [$($mw:expr),*]) => {{
        let __handler = $handler;
        ::obzenflow_runtime::typing::assert_source_output::<_, $out>(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            false,
            None,
        );
        let __descriptor =
            $crate::async_source!(@untyped $name => (__handler, $poll_timeout), [$($mw),*]);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<$out:ty> $name:literal => $handler:expr) => {
        $crate::async_source!(<$out> $name => $handler, [])
    };
    (<$out:ty> $name:literal => $handler:expr, [$($mw:expr),*]) => {{
        let __handler = $handler;
        ::obzenflow_runtime::typing::assert_source_output::<_, $out>(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            false,
            None,
        );
        let __descriptor = $crate::async_source!(@untyped $name => __handler, [$($mw),*]);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};

    ($name:literal => ($handler:expr, $poll_timeout:expr)) => {
        $crate::async_source!(@untyped $name => ($handler, $poll_timeout), [])
    };
    ($name:literal => ($handler:expr, $poll_timeout:expr), [$($mw:expr),*]) => {
        $crate::async_source!(@untyped $name => ($handler, $poll_timeout), [$($mw),*])
    };
    ($name:literal => $handler:expr) => {
        $crate::async_source!(@untyped $name => $handler, [])
    };
    ($name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::async_source!(@untyped $name => $handler, [$($mw),*])
    };
}

/// Create an infinite source stage descriptor.
#[macro_export]
macro_rules! infinite_source {
    (@untyped $name:literal => $handler:expr, [$($mw:expr),*]) => {{
        use $crate::dsl::stage_descriptor::{InfiniteSourceDescriptor, StageDescriptor};
        Box::new(InfiniteSourceDescriptor {
            name: $name.to_string(),
            handler: $handler,
            middleware: vec![$(Box::new($mw)),*],
        }) as Box<dyn StageDescriptor>
    }};

    (<mixed> $name:literal => $handler:expr $(, [$($mw:expr),*])?) => {
        compile_error!("`mixed` is only valid in input positions; source outputs must be exact")
    };
    (<$out:ty> $name:literal => placeholder!()) => {
        $crate::infinite_source!(<$out> $name => placeholder!(), [])
    };
    (<$out:ty> $name:literal => placeholder!(), [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            None,
        );
        let __descriptor = $crate::infinite_source!(
            @untyped
            $name => $crate::dsl::typing::PlaceholderInfiniteSource::<$out>::new(None),
            [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<$out:ty> $name:literal => placeholder!($msg:expr)) => {
        $crate::infinite_source!(<$out> $name => placeholder!($msg), [])
    };
    (<$out:ty> $name:literal => placeholder!($msg:expr), [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::infinite_source!(
            @untyped
            $name => $crate::dsl::typing::PlaceholderInfiniteSource::<$out>::new(Some($msg)),
            [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<$out:ty> $name:literal => $handler:expr) => {
        $crate::infinite_source!(<$out> $name => $handler, [])
    };
    (<$out:ty> $name:literal => $handler:expr, [$($mw:expr),*]) => {{
        let __handler = $handler;
        ::obzenflow_runtime::typing::assert_source_output::<_, $out>(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            false,
            None,
        );
        let __descriptor = $crate::infinite_source!(@untyped $name => __handler, [$($mw),*]);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};

    ($name:literal => $handler:expr) => {
        $crate::infinite_source!(@untyped $name => $handler, [])
    };
    ($name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::infinite_source!(@untyped $name => $handler, [$($mw),*])
    };
}

/// Create an async infinite source stage descriptor.
#[macro_export]
macro_rules! async_infinite_source {
    (@untyped $name:literal => ($handler:expr, $poll_timeout:expr), [$($mw:expr),*]) => {{
        $crate::dsl::stage_descriptor::AsyncInfiniteSourceDescriptor::new($name, $handler)
            .with_poll_timeout($poll_timeout)
            $(.with_middleware($mw))*
            .build()
    }};
    (@untyped $name:literal => $handler:expr, [$($mw:expr),*]) => {{
        $crate::dsl::stage_descriptor::AsyncInfiniteSourceDescriptor::new($name, $handler)
            $(.with_middleware($mw))*
            .build()
    }};

    (<mixed> $name:literal => $handler:expr $(, [$($mw:expr),*])?) => {
        compile_error!("`mixed` is only valid in input positions; source outputs must be exact")
    };
    (<$out:ty> $name:literal => placeholder!()) => {
        $crate::async_infinite_source!(<$out> $name => placeholder!(), [])
    };
    (<$out:ty> $name:literal => placeholder!(), [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            None,
        );
        let __descriptor = $crate::async_infinite_source!(
            @untyped
            $name => $crate::dsl::typing::PlaceholderAsyncSource::<$out>::new(None),
            [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<$out:ty> $name:literal => placeholder!($msg:expr)) => {
        $crate::async_infinite_source!(<$out> $name => placeholder!($msg), [])
    };
    (<$out:ty> $name:literal => placeholder!($msg:expr), [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::async_infinite_source!(
            @untyped
            $name => $crate::dsl::typing::PlaceholderAsyncSource::<$out>::new(Some($msg)),
            [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<$out:ty> $name:literal => ($handler:expr, $poll_timeout:expr)) => {
        $crate::async_infinite_source!(<$out> $name => ($handler, $poll_timeout), [])
    };
    (<$out:ty> $name:literal => ($handler:expr, $poll_timeout:expr), [$($mw:expr),*]) => {{
        let __handler = $handler;
        ::obzenflow_runtime::typing::assert_source_output::<_, $out>(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            false,
            None,
        );
        let __descriptor = $crate::async_infinite_source!(
            @untyped
            $name => (__handler, $poll_timeout),
            [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<$out:ty> $name:literal => $handler:expr) => {
        $crate::async_infinite_source!(<$out> $name => $handler, [])
    };
    (<$out:ty> $name:literal => $handler:expr, [$($mw:expr),*]) => {{
        let __handler = $handler;
        ::obzenflow_runtime::typing::assert_source_output::<_, $out>(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::source(
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            false,
            None,
        );
        let __descriptor =
            $crate::async_infinite_source!(@untyped $name => __handler, [$($mw),*]);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};

    ($name:literal => ($handler:expr, $poll_timeout:expr)) => {
        $crate::async_infinite_source!(@untyped $name => ($handler, $poll_timeout), [])
    };
    ($name:literal => ($handler:expr, $poll_timeout:expr), [$($mw:expr),*]) => {
        $crate::async_infinite_source!(@untyped $name => ($handler, $poll_timeout), [$($mw),*])
    };
    ($name:literal => $handler:expr) => {
        $crate::async_infinite_source!(@untyped $name => $handler, [])
    };
    ($name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::async_infinite_source!(@untyped $name => $handler, [$($mw),*])
    };
}

/// Create a transform stage descriptor.
#[macro_export]
macro_rules! transform {
    (@untyped $name:literal => $handler:expr, [$($mw:expr),*]) => {{
        use $crate::dsl::stage_descriptor::{StageDescriptor, TransformDescriptor};
        Box::new(TransformDescriptor {
            name: $name.to_string(),
            handler: $handler,
            middleware: vec![$(Box::new($mw)),*],
        }) as Box<dyn StageDescriptor>
    }};

    (<mixed, mixed> $name:literal => $handler:expr $(, [$($mw:expr),*])?) => {
        compile_error!("`mixed` is not valid in output positions")
    };
    (<$in:ty, mixed> $name:literal => $handler:expr $(, [$($mw:expr),*])?) => {
        compile_error!("`mixed` is not valid in output positions")
    };
    (<mixed, $out:ty> $name:literal => placeholder!()) => {
        $crate::transform!(<mixed, $out> $name => placeholder!(), [])
    };
    (<mixed, $out:ty> $name:literal => placeholder!(), [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            None,
        );
        let __descriptor = $crate::transform!(
            @untyped
            $name => $crate::dsl::typing::PlaceholderTransform::<::obzenflow_runtime::typing::MixedInput, $out>::new(None),
            [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<mixed, $out:ty> $name:literal => placeholder!($msg:expr)) => {
        $crate::transform!(<mixed, $out> $name => placeholder!($msg), [])
    };
    (<mixed, $out:ty> $name:literal => placeholder!($msg:expr), [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::transform!(
            @untyped
            $name => $crate::dsl::typing::PlaceholderTransform::<::obzenflow_runtime::typing::MixedInput, $out>::new(Some($msg)),
            [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<mixed, $out:ty> $name:literal => $handler:expr) => {
        $crate::transform!(<mixed, $out> $name => $handler, [])
    };
    (<mixed, $out:ty> $name:literal => $handler:expr, [$($mw:expr),*]) => {{
        let __handler = $handler;
        ::obzenflow_runtime::typing::assert_transform_output::<_, $out>(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            false,
            None,
        );
        let __descriptor = $crate::transform!(@untyped $name => __handler, [$($mw),*]);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<$in:ty, $out:ty> $name:literal => placeholder!()) => {
        $crate::transform!(<$in, $out> $name => placeholder!(), [])
    };
    (<$in:ty, $out:ty> $name:literal => placeholder!(), [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::exact(stringify!($in)),
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            None,
        );
        let __descriptor = $crate::transform!(
            @untyped
            $name => $crate::dsl::typing::PlaceholderTransform::<$in, $out>::new(None),
            [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<$in:ty, $out:ty> $name:literal => placeholder!($msg:expr)) => {
        $crate::transform!(<$in, $out> $name => placeholder!($msg), [])
    };
    (<$in:ty, $out:ty> $name:literal => placeholder!($msg:expr), [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::exact(stringify!($in)),
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::transform!(
            @untyped
            $name => $crate::dsl::typing::PlaceholderTransform::<$in, $out>::new(Some($msg)),
            [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<$in:ty, $out:ty> $name:literal => $handler:expr) => {
        $crate::transform!(<$in, $out> $name => $handler, [])
    };
    (<$in:ty, $out:ty> $name:literal => $handler:expr, [$($mw:expr),*]) => {{
        let __handler = $handler;
        ::obzenflow_runtime::typing::assert_transform_contract::<_, $in, $out>(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::exact(stringify!($in)),
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            false,
            None,
        );
        let __descriptor = $crate::transform!(@untyped $name => __handler, [$($mw),*]);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};

    ($name:literal => $handler:expr) => {
        $crate::transform!(@untyped $name => $handler, [])
    };
    ($name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::transform!(@untyped $name => $handler, [$($mw),*])
    };
}

/// Create an async transform stage descriptor.
#[macro_export]
macro_rules! async_transform {
    (@untyped $name:literal => $handler:expr, [$($mw:expr),*]) => {{
        use $crate::dsl::stage_descriptor::{AsyncTransformDescriptor, StageDescriptor};
        Box::new(AsyncTransformDescriptor {
            name: $name.to_string(),
            handler: $handler,
            middleware: vec![$(Box::new($mw)),*],
        }) as Box<dyn StageDescriptor>
    }};

    (<mixed, mixed> $name:literal => $handler:expr $(, [$($mw:expr),*])?) => {
        compile_error!("`mixed` is not valid in output positions")
    };
    (<$in:ty, mixed> $name:literal => $handler:expr $(, [$($mw:expr),*])?) => {
        compile_error!("`mixed` is not valid in output positions")
    };
    (<mixed, $out:ty> $name:literal => placeholder!()) => {
        $crate::async_transform!(<mixed, $out> $name => placeholder!(), [])
    };
    (<mixed, $out:ty> $name:literal => placeholder!(), [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            None,
        );
        let __descriptor = $crate::async_transform!(
            @untyped
            $name => $crate::dsl::typing::PlaceholderAsyncTransform::<::obzenflow_runtime::typing::MixedInput, $out>::new(None),
            [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<mixed, $out:ty> $name:literal => placeholder!($msg:expr)) => {
        $crate::async_transform!(<mixed, $out> $name => placeholder!($msg), [])
    };
    (<mixed, $out:ty> $name:literal => placeholder!($msg:expr), [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::async_transform!(
            @untyped
            $name => $crate::dsl::typing::PlaceholderAsyncTransform::<::obzenflow_runtime::typing::MixedInput, $out>::new(Some($msg)),
            [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<mixed, $out:ty> $name:literal => $handler:expr) => {
        $crate::async_transform!(<mixed, $out> $name => $handler, [])
    };
    (<mixed, $out:ty> $name:literal => $handler:expr, [$($mw:expr),*]) => {{
        let __handler = $handler;
        ::obzenflow_runtime::typing::assert_transform_output::<_, $out>(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            false,
            None,
        );
        let __descriptor = $crate::async_transform!(@untyped $name => __handler, [$($mw),*]);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<$in:ty, $out:ty> $name:literal => placeholder!()) => {
        $crate::async_transform!(<$in, $out> $name => placeholder!(), [])
    };
    (<$in:ty, $out:ty> $name:literal => placeholder!(), [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::exact(stringify!($in)),
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            None,
        );
        let __descriptor = $crate::async_transform!(
            @untyped
            $name => $crate::dsl::typing::PlaceholderAsyncTransform::<$in, $out>::new(None),
            [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<$in:ty, $out:ty> $name:literal => placeholder!($msg:expr)) => {
        $crate::async_transform!(<$in, $out> $name => placeholder!($msg), [])
    };
    (<$in:ty, $out:ty> $name:literal => placeholder!($msg:expr), [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::exact(stringify!($in)),
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::async_transform!(
            @untyped
            $name => $crate::dsl::typing::PlaceholderAsyncTransform::<$in, $out>::new(Some($msg)),
            [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<$in:ty, $out:ty> $name:literal => $handler:expr) => {
        $crate::async_transform!(<$in, $out> $name => $handler, [])
    };
    (<$in:ty, $out:ty> $name:literal => $handler:expr, [$($mw:expr),*]) => {{
        let __handler = $handler;
        ::obzenflow_runtime::typing::assert_transform_contract::<_, $in, $out>(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::transform(
            $crate::dsl::typing::TypeHint::exact(stringify!($in)),
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            false,
            None,
        );
        let __descriptor = $crate::async_transform!(@untyped $name => __handler, [$($mw),*]);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};

    ($name:literal => $handler:expr) => {
        $crate::async_transform!(@untyped $name => $handler, [])
    };
    ($name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::async_transform!(@untyped $name => $handler, [$($mw),*])
    };
}

/// Create a sink stage descriptor.
#[macro_export]
macro_rules! sink {
    (@untyped $name:literal => |$arg:ident : $ty:ty| $body:block, [$($mw:expr),*]) => {{
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
    (@untyped $name:literal => move |$arg:ident : $ty:ty| $body:block, [$($mw:expr),*]) => {{
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
    (@untyped $name:literal => $handler:expr, [$($mw:expr),*]) => {{
        use $crate::dsl::stage_descriptor::{SinkDescriptor, StageDescriptor};
        Box::new(SinkDescriptor {
            name: $name.to_string(),
            handler: $handler,
            middleware: vec![$(Box::new($mw)),*],
        }) as Box<dyn StageDescriptor>
    }};

    (<mixed> $name:literal => |$arg:ident : $ty:ty| $body:block $(, [$($mw:expr),*])?) => {
        compile_error!("typed sink! does not support closure shorthand; wrap the closure in SinkTyped::new(...) or FallibleSinkTyped::new(...)")
    };
    (<mixed> $name:literal => move |$arg:ident : $ty:ty| $body:block $(, [$($mw:expr),*])?) => {
        compile_error!("typed sink! does not support closure shorthand; wrap the closure in SinkTyped::new(...) or FallibleSinkTyped::new(...)")
    };
    (<$in:ty> $name:literal => |$arg:ident : $ty:ty| $body:block $(, [$($mw:expr),*])?) => {
        compile_error!("typed sink! does not support closure shorthand; wrap the closure in SinkTyped::new(...) or FallibleSinkTyped::new(...)")
    };
    (<$in:ty> $name:literal => move |$arg:ident : $ty:ty| $body:block $(, [$($mw:expr),*])?) => {
        compile_error!("typed sink! does not support closure shorthand; wrap the closure in SinkTyped::new(...) or FallibleSinkTyped::new(...)")
    };
    (<mixed> $name:literal => placeholder!()) => {
        $crate::sink!(<mixed> $name => placeholder!(), [])
    };
    (<mixed> $name:literal => placeholder!(), [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::sink(
            $crate::dsl::typing::TypeHint::Mixed,
            true,
            None,
        );
        let __descriptor = $crate::sink!(
            @untyped
            $name => $crate::dsl::typing::PlaceholderSink::<::obzenflow_runtime::typing::MixedInput>::new(None),
            [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<mixed> $name:literal => placeholder!($msg:expr)) => {
        $crate::sink!(<mixed> $name => placeholder!($msg), [])
    };
    (<mixed> $name:literal => placeholder!($msg:expr), [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::sink(
            $crate::dsl::typing::TypeHint::Mixed,
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::sink!(
            @untyped
            $name => $crate::dsl::typing::PlaceholderSink::<::obzenflow_runtime::typing::MixedInput>::new(Some($msg)),
            [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<mixed> $name:literal => $handler:expr) => {
        $crate::sink!(<mixed> $name => $handler, [])
    };
    (<mixed> $name:literal => $handler:expr, [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::sink(
            $crate::dsl::typing::TypeHint::Mixed,
            false,
            None,
        );
        let __descriptor = $crate::sink!(@untyped $name => $handler, [$($mw),*]);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<$in:ty> $name:literal => placeholder!()) => {
        $crate::sink!(<$in> $name => placeholder!(), [])
    };
    (<$in:ty> $name:literal => placeholder!(), [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::sink(
            $crate::dsl::typing::TypeHint::exact(stringify!($in)),
            true,
            None,
        );
        let __descriptor = $crate::sink!(
            @untyped
            $name => $crate::dsl::typing::PlaceholderSink::<$in>::new(None),
            [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<$in:ty> $name:literal => placeholder!($msg:expr)) => {
        $crate::sink!(<$in> $name => placeholder!($msg), [])
    };
    (<$in:ty> $name:literal => placeholder!($msg:expr), [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::sink(
            $crate::dsl::typing::TypeHint::exact(stringify!($in)),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::sink!(
            @untyped
            $name => $crate::dsl::typing::PlaceholderSink::<$in>::new(Some($msg)),
            [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<$in:ty> $name:literal => $handler:expr) => {
        $crate::sink!(<$in> $name => $handler, [])
    };
    (<$in:ty> $name:literal => $handler:expr, [$($mw:expr),*]) => {{
        let __handler = $handler;
        ::obzenflow_runtime::typing::assert_sink_input::<_, $in>(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::sink(
            $crate::dsl::typing::TypeHint::exact(stringify!($in)),
            false,
            None,
        );
        let __descriptor = $crate::sink!(@untyped $name => __handler, [$($mw),*]);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};

    ($name:literal => |$arg:ident : $ty:ty| $body:block) => {
        $crate::sink!(@untyped $name => |$arg: $ty| $body, [])
    };
    ($name:literal => move |$arg:ident : $ty:ty| $body:block) => {
        $crate::sink!(@untyped $name => move |$arg: $ty| $body, [])
    };
    ($name:literal => |$arg:ident : $ty:ty| $body:block, [$($mw:expr),*]) => {
        $crate::sink!(@untyped $name => |$arg: $ty| $body, [$($mw),*])
    };
    ($name:literal => move |$arg:ident : $ty:ty| $body:block, [$($mw:expr),*]) => {
        $crate::sink!(@untyped $name => move |$arg: $ty| $body, [$($mw),*])
    };
    ($name:literal => $handler:expr) => {
        $crate::sink!(@untyped $name => $handler, [])
    };
    ($name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::sink!(@untyped $name => $handler, [$($mw),*])
    };
}

/// Create a stateful stage descriptor.
#[macro_export]
macro_rules! stateful {
    (@untyped $name:literal => $handler:expr, emit_interval = $emit_interval:expr, [$($mw:expr),*]) => {{
        use $crate::dsl::stage_descriptor::StatefulDescriptor;
        StatefulDescriptor::new($name, $handler)
            .with_emit_interval($emit_interval)
            $(.with_middleware($mw))*
            .build()
    }};
    (@untyped $name:literal => $handler:expr, [$($mw:expr),*]) => {{
        use $crate::dsl::stage_descriptor::StatefulDescriptor;
        StatefulDescriptor::new($name, $handler)
            $(.with_middleware($mw))*
            .build()
    }};

    (<mixed, mixed> $name:literal => $handler:expr $(, emit_interval = $emit:expr)? $(, [$($mw:expr),*])?) => {
        compile_error!("`mixed` is not valid in output positions")
    };
    (<$in:ty, mixed> $name:literal => $handler:expr $(, emit_interval = $emit:expr)? $(, [$($mw:expr),*])?) => {
        compile_error!("`mixed` is not valid in output positions")
    };
    (<mixed, $out:ty> $name:literal => placeholder!()) => {
        $crate::stateful!(<mixed, $out> $name => placeholder!(), [])
    };
    (<mixed, $out:ty> $name:literal => placeholder!(), [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            None,
        );
        let __descriptor = $crate::stateful!(
            @untyped
            $name => $crate::dsl::typing::PlaceholderStateful::<::obzenflow_runtime::typing::MixedInput, $out>::new(None),
            [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<mixed, $out:ty> $name:literal => placeholder!(), emit_interval = $emit_interval:expr) => {
        $crate::stateful!(<mixed, $out> $name => placeholder!(), emit_interval = $emit_interval, [])
    };
    (<mixed, $out:ty> $name:literal => placeholder!(), emit_interval = $emit_interval:expr, [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            None,
        );
        let __descriptor = $crate::stateful!(
            @untyped
            $name => $crate::dsl::typing::PlaceholderStateful::<::obzenflow_runtime::typing::MixedInput, $out>::new(None),
            emit_interval = $emit_interval,
            [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<mixed, $out:ty> $name:literal => placeholder!($msg:expr)) => {
        $crate::stateful!(<mixed, $out> $name => placeholder!($msg), [])
    };
    (<mixed, $out:ty> $name:literal => placeholder!($msg:expr), [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::stateful!(
            @untyped
            $name => $crate::dsl::typing::PlaceholderStateful::<::obzenflow_runtime::typing::MixedInput, $out>::new(Some($msg)),
            [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<mixed, $out:ty> $name:literal => placeholder!($msg:expr), emit_interval = $emit_interval:expr) => {
        $crate::stateful!(<mixed, $out> $name => placeholder!($msg), emit_interval = $emit_interval, [])
    };
    (<mixed, $out:ty> $name:literal => placeholder!($msg:expr), emit_interval = $emit_interval:expr, [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::stateful!(
            @untyped
            $name => $crate::dsl::typing::PlaceholderStateful::<::obzenflow_runtime::typing::MixedInput, $out>::new(Some($msg)),
            emit_interval = $emit_interval,
            [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<mixed, $out:ty> $name:literal => $handler:expr) => {
        $crate::stateful!(<mixed, $out> $name => $handler, [])
    };
    (<mixed, $out:ty> $name:literal => $handler:expr, [$($mw:expr),*]) => {{
        let __handler = $handler;
        ::obzenflow_runtime::typing::assert_stateful_output::<_, $out>(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            false,
            None,
        );
        let __descriptor = $crate::stateful!(@untyped $name => __handler, [$($mw),*]);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<mixed, $out:ty> $name:literal => $handler:expr, emit_interval = $emit_interval:expr) => {
        $crate::stateful!(<mixed, $out> $name => $handler, emit_interval = $emit_interval, [])
    };
    (<mixed, $out:ty> $name:literal => $handler:expr, emit_interval = $emit_interval:expr, [$($mw:expr),*]) => {{
        let __handler = $handler;
        ::obzenflow_runtime::typing::assert_stateful_output::<_, $out>(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            false,
            None,
        );
        let __descriptor =
            $crate::stateful!(@untyped $name => __handler, emit_interval = $emit_interval, [$($mw),*]);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<$in:ty, $out:ty> $name:literal => placeholder!()) => {
        $crate::stateful!(<$in, $out> $name => placeholder!(), [])
    };
    (<$in:ty, $out:ty> $name:literal => placeholder!(), [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::exact(stringify!($in)),
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            None,
        );
        let __descriptor = $crate::stateful!(
            @untyped
            $name => $crate::dsl::typing::PlaceholderStateful::<$in, $out>::new(None),
            [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<$in:ty, $out:ty> $name:literal => placeholder!(), emit_interval = $emit_interval:expr) => {
        $crate::stateful!(<$in, $out> $name => placeholder!(), emit_interval = $emit_interval, [])
    };
    (<$in:ty, $out:ty> $name:literal => placeholder!(), emit_interval = $emit_interval:expr, [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::exact(stringify!($in)),
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            None,
        );
        let __descriptor = $crate::stateful!(
            @untyped
            $name => $crate::dsl::typing::PlaceholderStateful::<$in, $out>::new(None),
            emit_interval = $emit_interval,
            [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<$in:ty, $out:ty> $name:literal => placeholder!($msg:expr)) => {
        $crate::stateful!(<$in, $out> $name => placeholder!($msg), [])
    };
    (<$in:ty, $out:ty> $name:literal => placeholder!($msg:expr), [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::exact(stringify!($in)),
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::stateful!(
            @untyped
            $name => $crate::dsl::typing::PlaceholderStateful::<$in, $out>::new(Some($msg)),
            [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<$in:ty, $out:ty> $name:literal => placeholder!($msg:expr), emit_interval = $emit_interval:expr) => {
        $crate::stateful!(<$in, $out> $name => placeholder!($msg), emit_interval = $emit_interval, [])
    };
    (<$in:ty, $out:ty> $name:literal => placeholder!($msg:expr), emit_interval = $emit_interval:expr, [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::exact(stringify!($in)),
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::stateful!(
            @untyped
            $name => $crate::dsl::typing::PlaceholderStateful::<$in, $out>::new(Some($msg)),
            emit_interval = $emit_interval,
            [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<$in:ty, $out:ty> $name:literal => $handler:expr) => {
        $crate::stateful!(<$in, $out> $name => $handler, [])
    };
    (<$in:ty, $out:ty> $name:literal => $handler:expr, [$($mw:expr),*]) => {{
        let __handler = $handler;
        ::obzenflow_runtime::typing::assert_stateful_contract::<_, $in, $out>(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::exact(stringify!($in)),
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            false,
            None,
        );
        let __descriptor = $crate::stateful!(@untyped $name => __handler, [$($mw),*]);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<$in:ty, $out:ty> $name:literal => $handler:expr, emit_interval = $emit_interval:expr) => {
        $crate::stateful!(<$in, $out> $name => $handler, emit_interval = $emit_interval, [])
    };
    (<$in:ty, $out:ty> $name:literal => $handler:expr, emit_interval = $emit_interval:expr, [$($mw:expr),*]) => {{
        let __handler = $handler;
        ::obzenflow_runtime::typing::assert_stateful_contract::<_, $in, $out>(&__handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::stateful(
            $crate::dsl::typing::TypeHint::exact(stringify!($in)),
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            false,
            None,
        );
        let __descriptor =
            $crate::stateful!(@untyped $name => __handler, emit_interval = $emit_interval, [$($mw),*]);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};

    ($name:literal => $handler:expr, emit_interval = $emit_interval:expr) => {
        $crate::stateful!(@untyped $name => $handler, emit_interval = $emit_interval, [])
    };
    ($name:literal => $handler:expr, emit_interval = $emit_interval:expr, [$($mw:expr),*]) => {
        $crate::stateful!(@untyped $name => $handler, emit_interval = $emit_interval, [$($mw),*])
    };
    ($name:literal => $handler:expr) => {
        $crate::stateful!(@untyped $name => $handler, [])
    };
    ($name:literal => $handler:expr, [$($mw:expr),*]) => {
        $crate::stateful!(@untyped $name => $handler, [$($mw),*])
    };
}

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

/// Create a join stage descriptor.
#[macro_export]
macro_rules! join {
    (@untyped $name:literal => $join_with_ref:expr, [$($mw:expr),*]) => {{
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

    (<mixed, mixed, mixed> $name:literal => $join_with_ref:expr $(, [$($mw:expr),*])?) => {
        compile_error!("`mixed` is not valid in output positions")
    };
    (<mixed, $stream:ty, mixed> $name:literal => $join_with_ref:expr $(, [$($mw:expr),*])?) => {
        compile_error!("`mixed` is not valid in output positions")
    };
    (<$reference:ty, mixed, mixed> $name:literal => $join_with_ref:expr $(, [$($mw:expr),*])?) => {
        compile_error!("`mixed` is not valid in output positions")
    };
    (<$reference:ty, $stream:ty, mixed> $name:literal => $join_with_ref:expr $(, [$($mw:expr),*])?) => {
        compile_error!("`mixed` is not valid in output positions")
    };

    (<mixed, mixed, $out:ty> $name:literal => with_ref!($ref_var:ident, placeholder!())) => {
        $crate::join!(<mixed, mixed, $out> $name => with_ref!($ref_var, placeholder!()), [])
    };
    (<mixed, mixed, $out:ty> $name:literal => with_ref!($ref_var:ident, placeholder!()), [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::join(
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            None,
        );
        let __descriptor = $crate::join!(
            @untyped
            $name => $crate::with_ref!(
                $ref_var,
                $crate::dsl::typing::PlaceholderJoin::<::obzenflow_runtime::typing::MixedInput, ::obzenflow_runtime::typing::MixedInput, $out>::new(None)
            ),
            [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<mixed, mixed, $out:ty> $name:literal => with_ref!($ref_var:ident, placeholder!($msg:expr))) => {
        $crate::join!(<mixed, mixed, $out> $name => with_ref!($ref_var, placeholder!($msg)), [])
    };
    (<mixed, mixed, $out:ty> $name:literal => with_ref!($ref_var:ident, placeholder!($msg:expr)), [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::join(
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::join!(
            @untyped
            $name => $crate::with_ref!(
                $ref_var,
                $crate::dsl::typing::PlaceholderJoin::<::obzenflow_runtime::typing::MixedInput, ::obzenflow_runtime::typing::MixedInput, $out>::new(Some($msg))
            ),
            [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<mixed, mixed, $out:ty> $name:literal => $join_with_ref:expr) => {
        $crate::join!(<mixed, mixed, $out> $name => $join_with_ref, [])
    };
    (<mixed, mixed, $out:ty> $name:literal => $join_with_ref:expr, [$($mw:expr),*]) => {{
        let __join_with_ref = $join_with_ref;
        ::obzenflow_runtime::typing::assert_join_output::<_, $out>(&__join_with_ref.handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::join(
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            false,
            None,
        );
        let __descriptor = $crate::join!(@untyped $name => __join_with_ref, [$($mw),*]);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};

    (<mixed, $stream:ty, $out:ty> $name:literal => with_ref!($ref_var:ident, placeholder!())) => {
        $crate::join!(<mixed, $stream, $out> $name => with_ref!($ref_var, placeholder!()), [])
    };
    (<mixed, $stream:ty, $out:ty> $name:literal => with_ref!($ref_var:ident, placeholder!()), [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::join(
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($stream)),
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            None,
        );
        let __descriptor = $crate::join!(
            @untyped
            $name => $crate::with_ref!(
                $ref_var,
                $crate::dsl::typing::PlaceholderJoin::<::obzenflow_runtime::typing::MixedInput, $stream, $out>::new(None)
            ),
            [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<mixed, $stream:ty, $out:ty> $name:literal => with_ref!($ref_var:ident, placeholder!($msg:expr))) => {
        $crate::join!(<mixed, $stream, $out> $name => with_ref!($ref_var, placeholder!($msg)), [])
    };
    (<mixed, $stream:ty, $out:ty> $name:literal => with_ref!($ref_var:ident, placeholder!($msg:expr)), [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::join(
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($stream)),
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::join!(
            @untyped
            $name => $crate::with_ref!(
                $ref_var,
                $crate::dsl::typing::PlaceholderJoin::<::obzenflow_runtime::typing::MixedInput, $stream, $out>::new(Some($msg))
            ),
            [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<mixed, $stream:ty, $out:ty> $name:literal => $join_with_ref:expr) => {
        $crate::join!(<mixed, $stream, $out> $name => $join_with_ref, [])
    };
    (<mixed, $stream:ty, $out:ty> $name:literal => $join_with_ref:expr, [$($mw:expr),*]) => {{
        let __join_with_ref = $join_with_ref;
        ::obzenflow_runtime::typing::assert_join_stream_output::<_, $stream, $out>(&__join_with_ref.handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::join(
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($stream)),
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            false,
            None,
        );
        let __descriptor = $crate::join!(@untyped $name => __join_with_ref, [$($mw),*]);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};

    (<$reference:ty, mixed, $out:ty> $name:literal => with_ref!($ref_var:ident, placeholder!())) => {
        $crate::join!(<$reference, mixed, $out> $name => with_ref!($ref_var, placeholder!()), [])
    };
    (<$reference:ty, mixed, $out:ty> $name:literal => with_ref!($ref_var:ident, placeholder!()), [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::join(
            $crate::dsl::typing::TypeHint::exact(stringify!($reference)),
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            None,
        );
        let __descriptor = $crate::join!(
            @untyped
            $name => $crate::with_ref!(
                $ref_var,
                $crate::dsl::typing::PlaceholderJoin::<$reference, ::obzenflow_runtime::typing::MixedInput, $out>::new(None)
            ),
            [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<$reference:ty, mixed, $out:ty> $name:literal => with_ref!($ref_var:ident, placeholder!($msg:expr))) => {
        $crate::join!(<$reference, mixed, $out> $name => with_ref!($ref_var, placeholder!($msg)), [])
    };
    (<$reference:ty, mixed, $out:ty> $name:literal => with_ref!($ref_var:ident, placeholder!($msg:expr)), [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::join(
            $crate::dsl::typing::TypeHint::exact(stringify!($reference)),
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::join!(
            @untyped
            $name => $crate::with_ref!(
                $ref_var,
                $crate::dsl::typing::PlaceholderJoin::<$reference, ::obzenflow_runtime::typing::MixedInput, $out>::new(Some($msg))
            ),
            [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<$reference:ty, mixed, $out:ty> $name:literal => $join_with_ref:expr) => {
        $crate::join!(<$reference, mixed, $out> $name => $join_with_ref, [])
    };
    (<$reference:ty, mixed, $out:ty> $name:literal => $join_with_ref:expr, [$($mw:expr),*]) => {{
        let __join_with_ref = $join_with_ref;
        ::obzenflow_runtime::typing::assert_join_reference_output::<_, $reference, $out>(&__join_with_ref.handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::join(
            $crate::dsl::typing::TypeHint::exact(stringify!($reference)),
            $crate::dsl::typing::TypeHint::Mixed,
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            false,
            None,
        );
        let __descriptor = $crate::join!(@untyped $name => __join_with_ref, [$($mw),*]);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};

    (<$reference:ty, $stream:ty, $out:ty> $name:literal => with_ref!($ref_var:ident, placeholder!())) => {
        $crate::join!(<$reference, $stream, $out> $name => with_ref!($ref_var, placeholder!()), [])
    };
    (<$reference:ty, $stream:ty, $out:ty> $name:literal => with_ref!($ref_var:ident, placeholder!()), [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::join(
            $crate::dsl::typing::TypeHint::exact(stringify!($reference)),
            $crate::dsl::typing::TypeHint::exact(stringify!($stream)),
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            None,
        );
        let __descriptor = $crate::join!(
            @untyped
            $name => $crate::with_ref!(
                $ref_var,
                $crate::dsl::typing::PlaceholderJoin::<$reference, $stream, $out>::new(None)
            ),
            [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<$reference:ty, $stream:ty, $out:ty> $name:literal => with_ref!($ref_var:ident, placeholder!($msg:expr))) => {
        $crate::join!(<$reference, $stream, $out> $name => with_ref!($ref_var, placeholder!($msg)), [])
    };
    (<$reference:ty, $stream:ty, $out:ty> $name:literal => with_ref!($ref_var:ident, placeholder!($msg:expr)), [$($mw:expr),*]) => {{
        let __metadata = $crate::dsl::typing::StageTypingMetadata::join(
            $crate::dsl::typing::TypeHint::exact(stringify!($reference)),
            $crate::dsl::typing::TypeHint::exact(stringify!($stream)),
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            true,
            Some(($msg).to_string()),
        );
        let __descriptor = $crate::join!(
            @untyped
            $name => $crate::with_ref!(
                $ref_var,
                $crate::dsl::typing::PlaceholderJoin::<$reference, $stream, $out>::new(Some($msg))
            ),
            [$($mw),*]
        );
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};
    (<$reference:ty, $stream:ty, $out:ty> $name:literal => $join_with_ref:expr) => {
        $crate::join!(<$reference, $stream, $out> $name => $join_with_ref, [])
    };
    (<$reference:ty, $stream:ty, $out:ty> $name:literal => $join_with_ref:expr, [$($mw:expr),*]) => {{
        let __join_with_ref = $join_with_ref;
        ::obzenflow_runtime::typing::assert_join_contract::<_, $reference, $stream, $out>(&__join_with_ref.handler);
        let __metadata = $crate::dsl::typing::StageTypingMetadata::join(
            $crate::dsl::typing::TypeHint::exact(stringify!($reference)),
            $crate::dsl::typing::TypeHint::exact(stringify!($stream)),
            $crate::dsl::typing::TypeHint::exact(stringify!($out)),
            false,
            None,
        );
        let __descriptor = $crate::join!(@untyped $name => __join_with_ref, [$($mw),*]);
        $crate::dsl::typing::wrap_typed_descriptor(__descriptor, __metadata)
    }};

    ($name:literal => $join_with_ref:expr) => {
        $crate::join!(@untyped $name => $join_with_ref, [])
    };
    ($name:literal => $join_with_ref:expr, [$($mw:expr),*]) => {
        $crate::join!(@untyped $name => $join_with_ref, [$($mw),*])
    };
}
