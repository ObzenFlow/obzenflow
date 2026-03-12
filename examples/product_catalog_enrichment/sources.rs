// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::fixtures;
use obzenflow::typed::sources;
use obzenflow_runtime::stages::common::handlers::FiniteSourceHandler;

pub fn categories_source() -> impl FiniteSourceHandler + Clone + std::fmt::Debug + 'static {
    sources::finite(fixtures::categories())
}

pub fn products_source() -> impl FiniteSourceHandler + Clone + std::fmt::Debug + 'static {
    sources::finite(fixtures::products())
}

pub fn skus_source() -> impl FiniteSourceHandler + Clone + std::fmt::Debug + 'static {
    sources::finite(fixtures::skus())
}

pub fn promotions_source() -> impl FiniteSourceHandler + Clone + std::fmt::Debug + 'static {
    sources::finite(fixtures::promotions())
}

pub fn payment_methods_source() -> impl FiniteSourceHandler + Clone + std::fmt::Debug + 'static {
    sources::finite(fixtures::payments())
}

pub fn orders_source() -> impl FiniteSourceHandler + Clone + std::fmt::Debug + 'static {
    let inject_bad = std::env::var("INJECT_BAD_PAYMENT").is_ok();
    sources::finite(fixtures::orders(inject_bad))
}
