// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Type-explicit macros for creating stage descriptors
//!
//! These macros are the user-facing API for the let bindings approach.
//! Each returns a boxed StageDescriptor that knows its type.

/// Create a finite source stage descriptor
#[macro_export]
macro_rules! source {
    ($name:literal => $handler:expr) => {
        $crate::source!($name => $handler, [])
    };
    ($name:literal => $handler:expr, [$($mw:expr),*]) => {{
        use $crate::dsl::stage_descriptor::{StageDescriptor, FiniteSourceDescriptor};
        Box::new(FiniteSourceDescriptor {
            name: $name.to_string(),
            handler: $handler,
            middleware: vec![$(Box::new($mw)),*],
        }) as Box<dyn StageDescriptor>
    }};
}

/// Create an async finite source stage descriptor
#[macro_export]
macro_rules! async_source {
    ($name:literal => ($handler:expr, $poll_timeout:expr)) => {
        $crate::dsl::stage_descriptor::AsyncFiniteSourceDescriptor::new($name, $handler)
            .with_poll_timeout($poll_timeout)
            .build()
    };
    ($name:literal => ($handler:expr, $poll_timeout:expr), [$($mw:expr),*]) => {{
        $crate::dsl::stage_descriptor::AsyncFiniteSourceDescriptor::new($name, $handler)
            .with_poll_timeout($poll_timeout)
            $(.with_middleware($mw))*
            .build()
    }};
    ($name:literal => $handler:expr) => {
        $crate::async_source!($name => $handler, [])
    };
    ($name:literal => $handler:expr, [$($mw:expr),*]) => {{
        $crate::dsl::stage_descriptor::AsyncFiniteSourceDescriptor::new($name, $handler)
            $(.with_middleware($mw))*
            .build()
    }};
}

/// Create an infinite source stage descriptor
#[macro_export]
macro_rules! infinite_source {
    ($name:literal => $handler:expr) => {
        $crate::infinite_source!($name => $handler, [])
    };
    ($name:literal => $handler:expr, [$($mw:expr),*]) => {{
        use $crate::dsl::stage_descriptor::{StageDescriptor, InfiniteSourceDescriptor};
        Box::new(InfiniteSourceDescriptor {
            name: $name.to_string(),
            handler: $handler,
            middleware: vec![$(Box::new($mw)),*],
        }) as Box<dyn StageDescriptor>
    }};
}

/// Create an async infinite source stage descriptor
#[macro_export]
macro_rules! async_infinite_source {
    ($name:literal => ($handler:expr, $poll_timeout:expr)) => {
        $crate::dsl::stage_descriptor::AsyncInfiniteSourceDescriptor::new($name, $handler)
            .with_poll_timeout($poll_timeout)
            .build()
    };
    ($name:literal => ($handler:expr, $poll_timeout:expr), [$($mw:expr),*]) => {{
        $crate::dsl::stage_descriptor::AsyncInfiniteSourceDescriptor::new($name, $handler)
            .with_poll_timeout($poll_timeout)
            $(.with_middleware($mw))*
            .build()
    }};
    ($name:literal => $handler:expr) => {
        $crate::async_infinite_source!($name => $handler, [])
    };
    ($name:literal => $handler:expr, [$($mw:expr),*]) => {{
        $crate::dsl::stage_descriptor::AsyncInfiniteSourceDescriptor::new($name, $handler)
            $(.with_middleware($mw))*
            .build()
    }};
}

/// Create a transform stage descriptor
#[macro_export]
macro_rules! transform {
    ($name:literal => $handler:expr) => {
        $crate::transform!($name => $handler, [])
    };
    ($name:literal => $handler:expr, [$($mw:expr),*]) => {{
        use $crate::dsl::stage_descriptor::{StageDescriptor, TransformDescriptor};
        Box::new(TransformDescriptor {
            name: $name.to_string(),
            handler: $handler,
            middleware: vec![$(Box::new($mw)),*],
        }) as Box<dyn StageDescriptor>
    }};
}

/// Create an async transform stage descriptor
#[macro_export]
macro_rules! async_transform {
    ($name:literal => $handler:expr) => {
        $crate::async_transform!($name => $handler, [])
    };
    ($name:literal => $handler:expr, [$($mw:expr),*]) => {{
        use $crate::dsl::stage_descriptor::{AsyncTransformDescriptor, StageDescriptor};
        Box::new(AsyncTransformDescriptor {
            name: $name.to_string(),
            handler: $handler,
            middleware: vec![$(Box::new($mw)),*],
        }) as Box<dyn StageDescriptor>
    }};
}

/// Create a sink stage descriptor
#[macro_export]
macro_rules! sink {
    ($name:literal => |$arg:ident : $ty:ty| $body:block) => {
        $crate::sink!($name => |$arg: $ty| $body, [])
    };
    ($name:literal => move |$arg:ident : $ty:ty| $body:block) => {
        $crate::sink!($name => move |$arg: $ty| $body, [])
    };
    ($name:literal => |$arg:ident : $ty:ty| $body:block, [$($mw:expr),*]) => {{
        use $crate::dsl::stage_descriptor::{StageDescriptor, SinkDescriptor};
        Box::new(SinkDescriptor {
            name: $name.to_string(),
            handler: ::obzenflow_runtime_services::stages::sink::SinkTyped::new(move |$arg: $ty| {
                $body;
                async move {}
            }),
            middleware: vec![$(Box::new($mw)),*],
        }) as Box<dyn StageDescriptor>
    }};
    ($name:literal => move |$arg:ident : $ty:ty| $body:block, [$($mw:expr),*]) => {{
        use $crate::dsl::stage_descriptor::{StageDescriptor, SinkDescriptor};
        Box::new(SinkDescriptor {
            name: $name.to_string(),
            handler: ::obzenflow_runtime_services::stages::sink::SinkTyped::new(move |$arg: $ty| {
                $body;
                async move {}
            }),
            middleware: vec![$(Box::new($mw)),*],
        }) as Box<dyn StageDescriptor>
    }};
    ($name:literal => $handler:expr) => {
        $crate::sink!($name => $handler, [])
    };
    ($name:literal => $handler:expr, [$($mw:expr),*]) => {{
        use $crate::dsl::stage_descriptor::{StageDescriptor, SinkDescriptor};
        Box::new(SinkDescriptor {
            name: $name.to_string(),
            handler: $handler,
            middleware: vec![$(Box::new($mw)),*],
        }) as Box<dyn StageDescriptor>
    }};
}

/// Create a stateful stage descriptor
#[macro_export]
macro_rules! stateful {
    ($name:literal => $handler:expr, emit_interval = $emit_interval:expr) => {
        $crate::stateful!($name => $handler, emit_interval = $emit_interval, [])
    };
    ($name:literal => $handler:expr, emit_interval = $emit_interval:expr, [$($mw:expr),*]) => {{
        use $crate::dsl::stage_descriptor::StatefulDescriptor;
        StatefulDescriptor::new($name, $handler)
            .with_emit_interval($emit_interval)
            $(.with_middleware($mw))*
            .build()
    }};
    ($name:literal => $handler:expr) => {
        $crate::stateful!($name => $handler, [])
    };
    ($name:literal => $handler:expr, [$($mw:expr),*]) => {{
        use $crate::dsl::stage_descriptor::StatefulDescriptor;
        StatefulDescriptor::new($name, $handler)
            $(.with_middleware($mw))*
            .build()
    }};
}

/// Helper struct to pass reference stage variable and handler to join! macro
pub struct JoinWithRef<H> {
    pub reference_stage_var: &'static str,
    pub handler: H,
}

/// Create a JoinWithRef struct for use with join! macro
/// Takes the stage binding variable (identifier), not a string literal
#[macro_export]
macro_rules! with_ref {
    ($ref_var:ident, $handler:expr) => {
        $crate::dsl::JoinWithRef {
            reference_stage_var: stringify!($ref_var),
            handler: $handler,
        }
    };
}

/// Create a join stage descriptor
/// For DSL usage, use with_ref! macro to specify reference
/// Example: join!("enricher" => with_ref!(carriers, handler))
#[macro_export]
macro_rules! join {
    ($name:literal => $join_with_ref:expr) => {
        $crate::join!($name => $join_with_ref, [])
    };
    ($name:literal => $join_with_ref:expr, [$($mw:expr),*]) => {{
        use $crate::dsl::stage_descriptor::{StageDescriptor, JoinDescriptor};
        use obzenflow_core::id::StageId;
        let jwr = $join_with_ref;
        Box::new(JoinDescriptor {
            name: $name.to_string(),
            reference_stage_id: StageId::new(), // Placeholder, will be replaced by DSL
            reference_stage_var: Some(jwr.reference_stage_var),
            handler: jwr.handler,
            middleware: vec![$(Box::new($mw)),*],
        }) as Box<dyn StageDescriptor>
    }};
}
