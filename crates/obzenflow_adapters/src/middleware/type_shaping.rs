// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Type-shaping middleware declarations for the `output_middleware:` macro
//! lane (FLOWIP-120h).
//!
//! A type-shaping middleware can author branch facts (fallback, rejection)
//! that are not produced by the stage's effect. The declaration carries those
//! branch fact types as nameable type parameters so the DSL macro can
//! validate them against the arrow contract at build time and register them
//! with the effects runtime for guarded-perform coordination.

use crate::middleware::MiddlewareFactory;
use obzenflow_core::event::schema::{TypedFactType, TypedPayload};
use obzenflow_runtime::effects::SynthesizedOutcomeRegistration;
use std::marker::PhantomData;

/// A middleware factory plus the branch fact types it may author.
///
/// `F` is the fallback fact type and `R` the rejection fact type. The type
/// parameters exist so the macro lane can name them; the erased factory and a
/// runtime registration are extracted with [`into_registration_parts`].
///
/// [`into_registration_parts`]: TypeShapingMiddleware::into_registration_parts
pub struct TypeShapingMiddleware<F, R> {
    factory: Box<dyn MiddlewareFactory>,
    source_label: &'static str,
    config_error: Option<String>,
    _branches: PhantomData<fn() -> (F, R)>,
}

impl<F, R> TypeShapingMiddleware<F, R>
where
    F: TypedPayload + 'static,
    R: TypedPayload + 'static,
{
    pub fn new(
        factory: Box<dyn MiddlewareFactory>,
        source_label: &'static str,
        config_error: Option<String>,
    ) -> Self {
        Self {
            factory,
            source_label,
            config_error,
            _branches: PhantomData,
        }
    }

    /// Decompose into the erased factory, the runtime registration, and any
    /// configuration error detected while the branch types were still
    /// nameable. The DSL macro lane pushes the factory into the ordinary
    /// middleware stack, threads the registration to the effects runtime, and
    /// surfaces the error as a flow build failure.
    pub fn into_registration_parts(
        self,
    ) -> (
        Box<dyn MiddlewareFactory>,
        SynthesizedOutcomeRegistration,
        Option<String>,
    ) {
        let registration = SynthesizedOutcomeRegistration {
            effect_type: None,
            fact_types: vec![TypedFactType::of::<F>(), TypedFactType::of::<R>()],
            source_label: self.source_label.to_string(),
        };
        (self.factory, registration, self.config_error)
    }
}
