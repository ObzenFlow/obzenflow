// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::{Category, OrderEvent, PaymentMethod, Product, Promotion, Sku};
use super::fixtures;
use obzenflow::stages::sources;
use obzenflow::stages::sources::TypedFiniteSourceHandler;

pub fn categories_source(
) -> impl TypedFiniteSourceHandler<Output = Category> + std::fmt::Debug + 'static {
    sources::ValuesSource::new(fixtures::categories())
}

pub fn products_source(
) -> impl TypedFiniteSourceHandler<Output = Product> + std::fmt::Debug + 'static {
    sources::ValuesSource::new(fixtures::products())
}

pub fn skus_source() -> impl TypedFiniteSourceHandler<Output = Sku> + std::fmt::Debug + 'static {
    sources::ValuesSource::new(fixtures::skus())
}

pub fn promotions_source(
) -> impl TypedFiniteSourceHandler<Output = Promotion> + std::fmt::Debug + 'static {
    sources::ValuesSource::new(fixtures::promotions())
}

pub fn payment_methods_source(
) -> impl TypedFiniteSourceHandler<Output = PaymentMethod> + std::fmt::Debug + 'static {
    sources::ValuesSource::new(fixtures::payments())
}

pub fn orders_source(
) -> impl TypedFiniteSourceHandler<Output = OrderEvent> + std::fmt::Debug + 'static {
    let inject_bad = std::env::var("INJECT_BAD_PAYMENT").is_ok();
    sources::ValuesSource::new(fixtures::orders(inject_bad))
}
