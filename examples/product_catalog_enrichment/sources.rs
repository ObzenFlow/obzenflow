// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::{Category, OrderEvent, PaymentMethod, Product, Promotion, Sku};
use super::fixtures;
use obzenflow::stages::sources::{self, TypedFiniteSourceHandler};

pub fn categories_source() -> impl TypedFiniteSourceHandler<Output = Category> {
    sources::ValuesSource::new(fixtures::categories())
}

pub fn products_source() -> impl TypedFiniteSourceHandler<Output = Product> {
    sources::ValuesSource::new(fixtures::products())
}

pub fn skus_source() -> impl TypedFiniteSourceHandler<Output = Sku> {
    sources::ValuesSource::new(fixtures::skus())
}

pub fn promotions_source() -> impl TypedFiniteSourceHandler<Output = Promotion> {
    sources::ValuesSource::new(fixtures::promotions())
}

pub fn payment_methods_source() -> impl TypedFiniteSourceHandler<Output = PaymentMethod> {
    sources::ValuesSource::new(fixtures::payments())
}

pub fn orders_source(
    inject_bad_payment: bool,
) -> impl TypedFiniteSourceHandler<Output = OrderEvent> {
    sources::ValuesSource::new(fixtures::orders(inject_bad_payment))
}
