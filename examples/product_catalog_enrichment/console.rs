// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Pure presentation mappings for the flow's console sinks.

use super::domain::{CatalogAnalyticsSummary, EnrichedOrderWithPromo};

pub fn format_order(order: &EnrichedOrderWithPromo) -> String {
    let separator = "=".repeat(60);
    let risk = if order.risk_score < 0.05 {
        "Low Risk ✅"
    } else {
        "Medium Risk ⚠️"
    };
    let mut output = format!(
        concat!(
            "\n{separator}\n📊 ORDER: {}\n{separator}\n",
            "🏷️  Product: {}\n   Variant: {}\n",
            "   Brand: {} | Category: {} ({})\n   Quantity: {}\n",
            "\n💳 Payment: {} ({})\n   Risk Score: {:.3} ({risk})\n"
        ),
        order.order_id,
        order.product_name,
        order.variant,
        order.brand,
        order.category_name,
        order.department,
        order.quantity,
        order.payment_id,
        order.card_type,
        order.risk_score,
        separator = separator,
        risk = risk
    );
    match (&order.promo_code, order.discount_pct) {
        (Some(code), Some(discount)) => output.push_str(&format!(
            concat!(
                "\n🎟️  Promotion: {} ({})\n   Discount: {:.0}%\n",
                "   Original Revenue: ${:.2}\n   Discounted Revenue: ${:.2} ✨\n",
                "   Savings: ${:.2}\n"
            ),
            code,
            order.promo_type.as_deref().unwrap_or("Unknown"),
            discount * 100.0,
            order.revenue,
            order.discounted_revenue,
            order.revenue - order.discounted_revenue
        )),
        _ => output.push_str(&format!(
            "\n⚪ No Promotion Applied\n   Revenue: ${:.2}\n",
            order.revenue
        )),
    }
    output.push_str(&format!(
        concat!(
            "\n💰 Financial Summary:\n   Cost: ${:.2}\n   Revenue: ${:.2}\n",
            "   Margin: ${:.2} ({:.1}%)\n{separator}"
        ),
        order.cost,
        order.discounted_revenue,
        order.final_margin,
        order.margin_pct * 100.0,
        separator = separator
    ));
    output
}

pub fn format_summary(summary: &CatalogAnalyticsSummary) -> String {
    let separator = "=".repeat(60);
    let promo_pct = if summary.order_count > 0 {
        (summary.promo_orders as f64 / summary.order_count as f64) * 100.0
    } else {
        0.0
    };
    let margin_pct = if summary.total_revenue > 0.0 {
        (summary.total_margin / summary.total_revenue) * 100.0
    } else {
        0.0
    };
    format!(
        concat!(
            "\n\n{separator}\n🎯 FINAL ANALYTICS DASHBOARD\n{separator}\n",
            "Total Orders Processed: {}\nOrders with Promotions: {} ({promo_pct:.0}%)\n",
            "\n💰 Revenue Summary:\n   Total Revenue: ${:.2}\n   Total Margin: ${:.2}\n",
            "   Avg Margin %: {margin_pct:.1}%\n{separator}\n",
            "\n💡 Join Strategy Summary:\n",
            "   ✅ InnerJoin (SKU→Product→Category): All orders matched\n",
            "   🛡️  StrictJoin (Payment Validation): Only catalogued payments crossed the boundary\n",
            "   ✨ LeftJoin (Promotions): {}/{} orders had promos\n",
            "\n   Note: LeftJoin preserved all orders, even without promotions!\n",
            "   Note: An invalid payment makes StrictJoin emit Poison after its exact committed prefix"
        ),
        summary.order_count, summary.promo_orders, summary.total_revenue,
        summary.total_margin, summary.promo_orders, summary.order_count,
        separator = separator, promo_pct = promo_pct, margin_pct = margin_pct
    )
}
