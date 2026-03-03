// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Type-explicit macros for creating stage descriptors
//!
//! These macros are the user-facing API for the let bindings approach.
//! Each returns a boxed StageDescriptor that knows its type.

/// Create a finite source stage descriptor.
///
/// ```rust,ignore
/// // Emit a vector of typed payloads:
/// let s = source!("readings" => FiniteSourceTyped::new(vec![reading1, reading2]));
///
/// // With stage-level middleware:
/// let s = source!("readings" => FiniteSourceTyped::new(data), [rate_limit(10.0)]);
/// ```
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

/// Create an async finite source stage descriptor.
///
/// Accepts an optional poll timeout as a tuple `(handler, timeout)`.
///
/// ```rust,ignore
/// let s = async_source!("http_fetch" => my_async_source);
/// let s = async_source!("http_fetch" => (my_async_source, Duration::from_secs(5)));
/// ```
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

/// Create an infinite source stage descriptor.
///
/// Unlike finite sources, infinite sources never signal completion on their
/// own. The pipeline runs until shut down externally.
///
/// ```rust,ignore
/// let s = infinite_source!("ticker" => my_infinite_handler);
/// ```
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

/// Create an async infinite source stage descriptor.
///
/// Combines infinite-source semantics with an async poll loop. Accepts an
/// optional poll timeout as a tuple `(handler, timeout)`.
///
/// ```rust,ignore
/// let s = async_infinite_source!("ws_stream" => my_ws_handler);
/// ```
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

/// Create a transform stage descriptor.
///
/// ```rust,ignore
/// // Using a TransformHandler implementation:
/// let t = transform!("validator" => MyValidator::new());
///
/// // Using a typed one-to-one mapper:
/// let t = transform!("double" => MapTyped::new(|e: Input| Output { v: e.v * 2 }));
/// ```
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

/// Create an async transform stage descriptor.
///
/// Use this when the transform needs to perform async I/O (HTTP calls,
/// database lookups, etc.).
///
/// ```rust,ignore
/// let t = async_transform!("enrich" => MyAsyncEnricher::new());
/// ```
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

/// Create a sink stage descriptor.
///
/// Accepts a `SinkHandler` implementation, a `ConsoleSink`, or a typed
/// closure `|event: MyType| { ... }`.
///
/// ```rust,ignore
/// // Struct-based sink:
/// let s = sink!("output" => ConsoleSink::<MyEvent>::json());
///
/// // Closure-based sink (typed):
/// let s = sink!("log" => |e: MyEvent| { println!("{:?}", e); });
/// ```
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
            handler: ::obzenflow_runtime::stages::sink::SinkTyped::new(move |$arg: $ty| {
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
            handler: ::obzenflow_runtime::stages::sink::SinkTyped::new(move |$arg: $ty| {
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

/// Create a stateful stage descriptor.
///
/// Stateful stages accumulate events into internal state and periodically
/// emit aggregate results. Supports an optional `emit_interval` for
/// time-based emission.
///
/// ```rust,ignore
/// // Custom StatefulHandler:
/// let s = stateful!("aggregator" => MyAggregator::new());
///
/// // With a time-based emit interval:
/// let s = stateful!("counter" => MyCounter::new(), emit_interval = Duration::from_secs(5));
/// ```
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

/// Create a join stage descriptor.
///
/// Joins enrich a stream with reference data loaded by another stage. Use
/// the [`with_ref!`] macro to bind the reference stage variable and handler
/// together.
///
/// ```rust,ignore
/// // In the stages block, `carriers` is a source stage variable:
/// let j = join!("enricher" => with_ref!(carriers,
///     InnerJoinBuilder::<Carrier, Order, Enriched>::new()
///         .catalog_key(|c: &Carrier| c.code.clone())
///         .stream_key(|o: &Order| o.carrier_code.clone())
///         .build(|carrier, order| Enriched { /* ... */ })
/// ));
/// ```
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
