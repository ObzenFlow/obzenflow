// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::*;

pub fn enrich_sku(product: Product, sku: Sku) -> SKUWithProduct {
    SKUWithProduct {
        sku_id: sku.sku_id,
        variant: sku.variant,
        unit_cost: sku.unit_cost,
        current_price: sku.current_price,
        product_id: sku.product_id,
        product_name: product.product_name,
        brand: product.brand,
        category_id: product.category_id,
    }
}

pub fn add_category(category: Category, sku_product: SKUWithProduct) -> SKUFullDimension {
    SKUFullDimension {
        sku_id: sku_product.sku_id,
        variant: sku_product.variant,
        unit_cost: sku_product.unit_cost,
        current_price: sku_product.current_price,
        product_name: sku_product.product_name,
        brand: sku_product.brand,
        category_name: category.category_name,
        department: category.department,
        margin_target: category.margin_target,
    }
}

pub fn validate_payment(payment: PaymentMethod, order: OrderEvent) -> ValidatedOrder {
    ValidatedOrder {
        order_id: order.order_id,
        sku_id: order.sku_id,
        quantity: order.quantity,
        payment_id: order.payment_id,
        card_type: payment.card_type,
        risk_score: payment.risk_score,
        timestamp: order.timestamp,
    }
}

pub fn enrich_order(dimension: SKUFullDimension, order: ValidatedOrder) -> EnrichedOrder {
    let revenue = dimension.current_price * order.quantity as f64;
    let cost = dimension.unit_cost * order.quantity as f64;
    let margin = revenue - cost;
    let margin_pct = if revenue > 0.0 { margin / revenue } else { 0.0 };

    EnrichedOrder {
        order_id: order.order_id,
        quantity: order.quantity,
        timestamp: order.timestamp,
        sku_id: order.sku_id,
        variant: dimension.variant,
        product_name: dimension.product_name,
        brand: dimension.brand,
        category_name: dimension.category_name,
        department: dimension.department,
        margin_target: dimension.margin_target,
        unit_cost: dimension.unit_cost,
        current_price: dimension.current_price,
        payment_id: order.payment_id,
        card_type: order.card_type,
        risk_score: order.risk_score,
        revenue,
        cost,
        margin,
        margin_pct,
    }
}

pub fn apply_promotion(promo: Option<Promotion>, order: EnrichedOrder) -> EnrichedOrderWithPromo {
    let (promo_code, discount_pct, promo_type) = promo
        .map(|p| (Some(p.promo_code), Some(p.discount_pct), Some(p.promo_type)))
        .unwrap_or((None, None, None));

    let discounted_revenue = if let Some(discount) = discount_pct {
        order.revenue * (1.0 - discount)
    } else {
        order.revenue
    };

    let final_margin = discounted_revenue - order.cost;

    EnrichedOrderWithPromo {
        order_id: order.order_id,
        quantity: order.quantity,
        timestamp: order.timestamp,
        sku_id: order.sku_id,
        variant: order.variant,
        product_name: order.product_name,
        brand: order.brand,
        category_name: order.category_name,
        department: order.department,
        margin_target: order.margin_target,
        unit_cost: order.unit_cost,
        current_price: order.current_price,
        payment_id: order.payment_id,
        card_type: order.card_type,
        risk_score: order.risk_score,
        revenue: order.revenue,
        cost: order.cost,
        margin: order.margin,
        margin_pct: order.margin_pct,
        promo_code,
        discount_pct,
        promo_type,
        discounted_revenue,
        final_margin,
    }
}

pub fn summarise_order(summary: &mut CatalogAnalyticsSummary, order: &EnrichedOrderWithPromo) {
    summary.order_count += 1;
    summary.total_revenue += order.discounted_revenue;
    summary.total_margin += order.final_margin;

    if order.promo_code.is_some() && order.discount_pct.is_some() {
        summary.promo_orders += 1;
    }
}
