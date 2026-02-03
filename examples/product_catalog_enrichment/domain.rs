// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::TypedPayload;
use serde::{Deserialize, Serialize};

// Dimension Tables (Reference Data)

/// Category dimension (top of hierarchy)
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Category {
    pub category_id: String,
    pub category_name: String,
    pub department: String,
    pub margin_target: f64, // Target profit margin
}

impl TypedPayload for Category {
    const EVENT_TYPE: &'static str = "catalog.category";
    const SCHEMA_VERSION: u32 = 1;
}

/// Product dimension (middle of hierarchy)
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Product {
    pub product_id: String,
    pub product_name: String,
    pub category_id: String, // FK to Category
    pub brand: String,
    pub base_price: f64,
}

impl TypedPayload for Product {
    const EVENT_TYPE: &'static str = "catalog.product";
    const SCHEMA_VERSION: u32 = 1;
}

/// SKU dimension (bottom of hierarchy - actual sellable items)
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Sku {
    pub sku_id: String,
    pub product_id: String, // FK to Product
    pub variant: String,    // e.g., "16GB/512GB"
    pub unit_cost: f64,
    pub current_price: f64,
}

impl TypedPayload for Sku {
    const EVENT_TYPE: &'static str = "catalog.sku";
    const SCHEMA_VERSION: u32 = 1;
}

/// Promotion dimension (optional - for LeftJoin demo)
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Promotion {
    pub sku_id: String,
    pub promo_code: String,
    pub discount_pct: f64,
    pub promo_type: String,
}

impl TypedPayload for Promotion {
    const EVENT_TYPE: &'static str = "catalog.promotion";
    const SCHEMA_VERSION: u32 = 1;
}

/// Payment method dimension (critical - for StrictJoin demo)
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PaymentMethod {
    pub payment_id: String,
    pub card_type: String,
    pub risk_score: f64,
    pub approved: bool,
}

impl TypedPayload for PaymentMethod {
    const EVENT_TYPE: &'static str = "payment.method";
    const SCHEMA_VERSION: u32 = 1;
}

// Fact Stream

/// Order event (fact - streaming data)
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OrderEvent {
    pub order_id: String,
    pub sku_id: String, // Will join with SKU dimension
    pub quantity: u32,
    pub payment_id: String, // Will join with Payment dimension (StrictJoin!)
    pub timestamp: u64,
}

impl TypedPayload for OrderEvent {
    const EVENT_TYPE: &'static str = "order.placed";
    const SCHEMA_VERSION: u32 = 1;
}

// Intermediate Enriched Types (Join Outputs)

/// SKU enriched with Product data (InnerJoin output)
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SKUWithProduct {
    pub sku_id: String,
    pub variant: String,
    pub unit_cost: f64,
    pub current_price: f64,
    pub product_id: String,
    pub product_name: String,
    pub brand: String,
    pub category_id: String, // Will join with Category next
}

impl TypedPayload for SKUWithProduct {
    const EVENT_TYPE: &'static str = "enriched.sku_product";
    const SCHEMA_VERSION: u32 = 1;
}

/// SKU+Product enriched with Category data (InnerJoin output)
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SKUFullDimension {
    pub sku_id: String,
    pub variant: String,
    pub unit_cost: f64,
    pub current_price: f64,
    pub product_name: String,
    pub brand: String,
    pub category_name: String,
    pub department: String,
    pub margin_target: f64,
}

impl TypedPayload for SKUFullDimension {
    const EVENT_TYPE: &'static str = "enriched.sku_full_dimension";
    const SCHEMA_VERSION: u32 = 1;
}

/// Order validated with Payment data (StrictJoin output)
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ValidatedOrder {
    pub order_id: String,
    pub sku_id: String,
    pub quantity: u32,
    pub payment_id: String,
    pub card_type: String,
    pub risk_score: f64,
    pub timestamp: u64,
}

impl TypedPayload for ValidatedOrder {
    const EVENT_TYPE: &'static str = "order.validated";
    const SCHEMA_VERSION: u32 = 1;
}

/// Order enriched with full SKU dimensions (InnerJoin output)
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EnrichedOrder {
    pub order_id: String,
    pub quantity: u32,
    pub timestamp: u64,
    pub sku_id: String,
    pub variant: String,
    pub product_name: String,
    pub brand: String,
    pub category_name: String,
    pub department: String,
    pub margin_target: f64,
    pub unit_cost: f64,
    pub current_price: f64,
    pub payment_id: String,
    pub card_type: String,
    pub risk_score: f64,
    pub revenue: f64,
    pub cost: f64,
    pub margin: f64,
    pub margin_pct: f64,
}

impl TypedPayload for EnrichedOrder {
    const EVENT_TYPE: &'static str = "order.enriched";
    const SCHEMA_VERSION: u32 = 1;
}

/// Final order enriched with promotions (LeftJoin output)
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EnrichedOrderWithPromo {
    pub order_id: String,
    pub quantity: u32,
    pub timestamp: u64,
    pub sku_id: String,
    pub variant: String,
    pub product_name: String,
    pub brand: String,
    pub category_name: String,
    pub department: String,
    pub margin_target: f64,
    pub unit_cost: f64,
    pub current_price: f64,
    pub payment_id: String,
    pub card_type: String,
    pub risk_score: f64,
    pub revenue: f64,
    pub cost: f64,
    pub margin: f64,
    pub margin_pct: f64,
    // Optional promotion fields (LeftJoin - may be None!)
    pub promo_code: Option<String>,
    pub discount_pct: Option<f64>,
    pub promo_type: Option<String>,
    pub discounted_revenue: f64,
    pub final_margin: f64,
}

impl TypedPayload for EnrichedOrderWithPromo {
    const EVENT_TYPE: &'static str = "order.final";
    const SCHEMA_VERSION: u32 = 1;
}
