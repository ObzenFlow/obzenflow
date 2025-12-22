use super::domain::*;
use super::fixtures;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    id::StageId,
    TypedPayload, WriterId,
};
use obzenflow_runtime_services::stages::common::handlers::FiniteSourceHandler;
use serde_json::json;

#[derive(Clone, Debug)]
pub struct CategorySource {
    categories: Vec<Category>,
    current_index: usize,
    writer_id: WriterId,
}

impl CategorySource {
    pub fn new() -> Self {
        Self {
            categories: fixtures::categories(),
            current_index: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for CategorySource {
    fn next(
        &mut self,
    ) -> Result<
        Option<Vec<ChainEvent>>,
        obzenflow_runtime_services::stages::common::handlers::source::traits::SourceError,
    > {
        if self.current_index < self.categories.len() {
            let category = &self.categories[self.current_index];
            self.current_index += 1;

            Ok(Some(vec![ChainEventFactory::data_event(
                self.writer_id.clone(),
                Category::EVENT_TYPE,
                json!(category),
            )]))
        } else {
            Ok(None)
        }
    }
}

#[derive(Clone, Debug)]
pub struct ProductSource {
    products: Vec<Product>,
    current_index: usize,
    writer_id: WriterId,
}

impl ProductSource {
    pub fn new() -> Self {
        Self {
            products: fixtures::products(),
            current_index: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for ProductSource {
    fn next(
        &mut self,
    ) -> Result<
        Option<Vec<ChainEvent>>,
        obzenflow_runtime_services::stages::common::handlers::source::traits::SourceError,
    > {
        if self.current_index < self.products.len() {
            let product = &self.products[self.current_index];
            self.current_index += 1;

            Ok(Some(vec![ChainEventFactory::data_event(
                self.writer_id.clone(),
                Product::EVENT_TYPE,
                json!(product),
            )]))
        } else {
            Ok(None)
        }
    }
}

#[derive(Clone, Debug)]
pub struct SKUSource {
    skus: Vec<SKU>,
    current_index: usize,
    writer_id: WriterId,
}

impl SKUSource {
    pub fn new() -> Self {
        Self {
            skus: fixtures::skus(),
            current_index: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for SKUSource {
    fn next(
        &mut self,
    ) -> Result<
        Option<Vec<ChainEvent>>,
        obzenflow_runtime_services::stages::common::handlers::source::traits::SourceError,
    > {
        if self.current_index < self.skus.len() {
            let sku = &self.skus[self.current_index];
            self.current_index += 1;

            Ok(Some(vec![ChainEventFactory::data_event(
                self.writer_id.clone(),
                SKU::EVENT_TYPE,
                json!(sku),
            )]))
        } else {
            Ok(None)
        }
    }
}

#[derive(Clone, Debug)]
pub struct PromotionSource {
    promotions: Vec<Promotion>,
    current_index: usize,
    writer_id: WriterId,
}

impl PromotionSource {
    pub fn new() -> Self {
        Self {
            promotions: fixtures::promotions(),
            current_index: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for PromotionSource {
    fn next(
        &mut self,
    ) -> Result<
        Option<Vec<ChainEvent>>,
        obzenflow_runtime_services::stages::common::handlers::source::traits::SourceError,
    > {
        if self.current_index < self.promotions.len() {
            let promo = &self.promotions[self.current_index];
            self.current_index += 1;

            Ok(Some(vec![ChainEventFactory::data_event(
                self.writer_id.clone(),
                Promotion::EVENT_TYPE,
                json!(promo),
            )]))
        } else {
            Ok(None)
        }
    }
}

#[derive(Clone, Debug)]
pub struct PaymentSource {
    payments: Vec<PaymentMethod>,
    current_index: usize,
    writer_id: WriterId,
}

impl PaymentSource {
    pub fn new() -> Self {
        Self {
            payments: fixtures::payments(),
            current_index: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for PaymentSource {
    fn next(
        &mut self,
    ) -> Result<
        Option<Vec<ChainEvent>>,
        obzenflow_runtime_services::stages::common::handlers::source::traits::SourceError,
    > {
        if self.current_index < self.payments.len() {
            let payment = &self.payments[self.current_index];
            self.current_index += 1;

            Ok(Some(vec![ChainEventFactory::data_event(
                self.writer_id.clone(),
                PaymentMethod::EVENT_TYPE,
                json!(payment),
            )]))
        } else {
            Ok(None)
        }
    }
}

#[derive(Clone, Debug)]
pub struct OrderSource {
    orders: Vec<OrderEvent>,
    current_index: usize,
    writer_id: WriterId,
}

impl OrderSource {
    pub fn new() -> Self {
        let inject_bad = std::env::var("INJECT_BAD_PAYMENT").is_ok();
        Self {
            orders: fixtures::orders(inject_bad),
            current_index: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for OrderSource {
    fn next(
        &mut self,
    ) -> Result<
        Option<Vec<ChainEvent>>,
        obzenflow_runtime_services::stages::common::handlers::source::traits::SourceError,
    > {
        if self.current_index < self.orders.len() {
            let order = &self.orders[self.current_index];
            self.current_index += 1;

            Ok(Some(vec![ChainEventFactory::data_event(
                self.writer_id.clone(),
                OrderEvent::EVENT_TYPE,
                json!(order),
            )]))
        } else {
            Ok(None)
        }
    }
}
