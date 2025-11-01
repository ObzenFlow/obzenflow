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

/// Create a sink stage descriptor
#[macro_export]
macro_rules! sink {
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
    ($name:literal => $handler:expr) => {
        $crate::stateful!($name => $handler, [])
    };
    ($name:literal => $handler:expr, [$($mw:expr),*]) => {{
        use $crate::dsl::stage_descriptor::{StageDescriptor, StatefulDescriptor};
        Box::new(StatefulDescriptor {
            name: $name.to_string(),
            handler: $handler,
            middleware: vec![$(Box::new($mw)),*],
        }) as Box<dyn StageDescriptor>
    }};
}