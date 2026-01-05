use super::fixtures;
use obzenflow_runtime_services::stages::common::handlers::FiniteSourceHandler;
use obzenflow_runtime_services::stages::source::FiniteSourceTyped;

pub fn categories_source() -> impl FiniteSourceHandler + Clone + std::fmt::Debug + 'static {
    FiniteSourceTyped::new(fixtures::categories())
}

pub fn products_source() -> impl FiniteSourceHandler + Clone + std::fmt::Debug + 'static {
    FiniteSourceTyped::new(fixtures::products())
}

pub fn skus_source() -> impl FiniteSourceHandler + Clone + std::fmt::Debug + 'static {
    FiniteSourceTyped::new(fixtures::skus())
}

pub fn promotions_source() -> impl FiniteSourceHandler + Clone + std::fmt::Debug + 'static {
    FiniteSourceTyped::new(fixtures::promotions())
}

pub fn payment_methods_source() -> impl FiniteSourceHandler + Clone + std::fmt::Debug + 'static {
    FiniteSourceTyped::new(fixtures::payments())
}

pub fn orders_source() -> impl FiniteSourceHandler + Clone + std::fmt::Debug + 'static {
    let inject_bad = std::env::var("INJECT_BAD_PAYMENT").is_ok();
    FiniteSourceTyped::new(fixtures::orders(inject_bad))
}
