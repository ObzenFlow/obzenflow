// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::{Category, OrderEvent, PaymentMethod, Product, Promotion, Sku};
use super::fixtures;
use obzenflow::effects::Port;
use obzenflow::stages::sources::{self, FiniteSource, Refuse};

pub fn categories_source() -> Port<dyn FiniteSource<Category, Refuse>> {
    Port::owned(sources::ValuesSource::new(fixtures::categories()))
}

pub fn products_source() -> Port<dyn FiniteSource<Product, Refuse>> {
    Port::owned(sources::ValuesSource::new(fixtures::products()))
}

pub fn skus_source() -> Port<dyn FiniteSource<Sku, Refuse>> {
    Port::owned(sources::ValuesSource::new(fixtures::skus()))
}

pub fn promotions_source() -> Port<dyn FiniteSource<Promotion, Refuse>> {
    Port::owned(sources::ValuesSource::new(fixtures::promotions()))
}

pub fn payment_methods_source() -> Port<dyn FiniteSource<PaymentMethod, Refuse>> {
    Port::owned(sources::ValuesSource::new(fixtures::payments()))
}

pub fn orders_source(inject_bad_payment: bool) -> Port<dyn FiniteSource<OrderEvent, Refuse>> {
    Port::owned(sources::ValuesSource::new(fixtures::orders(
        inject_bad_payment,
    )))
}
