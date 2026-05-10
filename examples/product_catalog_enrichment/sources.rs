// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::{Category, OrderEvent, PaymentMethod, Product, Promotion, Sku};
use super::fixtures;
use obzenflow::typed::sources;
use obzenflow_runtime::stages::common::handlers::FiniteSourceHandler;
use obzenflow_runtime::typing::SourceTyping;

pub fn categories_source(
) -> impl FiniteSourceHandler + SourceTyping<Output = Category> + Clone + std::fmt::Debug + 'static
{
    sources::finite(fixtures::categories())
}

pub fn products_source(
) -> impl FiniteSourceHandler + SourceTyping<Output = Product> + Clone + std::fmt::Debug + 'static {
    sources::finite(fixtures::products())
}

pub fn skus_source(
) -> impl FiniteSourceHandler + SourceTyping<Output = Sku> + Clone + std::fmt::Debug + 'static {
    sources::finite(fixtures::skus())
}

pub fn promotions_source(
) -> impl FiniteSourceHandler + SourceTyping<Output = Promotion> + Clone + std::fmt::Debug + 'static
{
    sources::finite(fixtures::promotions())
}

pub fn payment_methods_source(
) -> impl FiniteSourceHandler + SourceTyping<Output = PaymentMethod> + Clone + std::fmt::Debug + 'static
{
    sources::finite(fixtures::payments())
}

pub fn orders_source(
) -> impl FiniteSourceHandler + SourceTyping<Output = OrderEvent> + Clone + std::fmt::Debug + 'static
{
    let inject_bad = std::env::var("INJECT_BAD_PAYMENT").is_ok();
    sources::finite(fixtures::orders(inject_bad))
}
