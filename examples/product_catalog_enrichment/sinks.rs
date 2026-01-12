use super::domain::EnrichedOrderWithPromo;
use obzenflow_core::TypedPayload;
use obzenflow_runtime_services::stages::common::handlers::SinkHandler;
use obzenflow_runtime_services::stages::sink::SinkTyped;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CatalogAnalyticsSummary {
    pub order_count: usize,
    pub total_revenue: f64,
    pub total_margin: f64,
    pub promo_orders: usize,
}

impl TypedPayload for CatalogAnalyticsSummary {
    const EVENT_TYPE: &'static str = "catalog.analytics_summary";
    const SCHEMA_VERSION: u32 = 1;
}

pub fn per_order_printer() -> impl SinkHandler + Clone + std::fmt::Debug + 'static {
    SinkTyped::new(|order: EnrichedOrderWithPromo| async move {
        println!("\n{}", "=".repeat(60));
        println!("📊 ORDER: {}", order.order_id);
        println!("{}", "=".repeat(60));

        println!("🏷️  Product: {}", order.product_name);
        println!("   Variant: {}", order.variant);
        println!(
            "   Brand: {} | Category: {} ({})",
            order.brand, order.category_name, order.department
        );
        println!("   Quantity: {}", order.quantity);

        println!("\n💳 Payment: {} ({})", order.payment_id, order.card_type);
        println!(
            "   Risk Score: {:.3} ({})",
            order.risk_score,
            if order.risk_score < 0.05 {
                "Low Risk ✅"
            } else {
                "Medium Risk ⚠️"
            }
        );

        match (&order.promo_code, order.discount_pct) {
            (Some(code), Some(discount)) => {
                println!(
                    "\n🎟️  Promotion: {} ({})",
                    code,
                    order.promo_type.as_deref().unwrap_or("Unknown")
                );
                println!("   Discount: {:.0}%", discount * 100.0);
                println!("   Original Revenue: ${:.2}", order.revenue);
                println!("   Discounted Revenue: ${:.2} ✨", order.discounted_revenue);
                println!(
                    "   Savings: ${:.2}",
                    order.revenue - order.discounted_revenue
                );
            }
            _ => {
                println!("\n⚪ No Promotion Applied");
                println!("   Revenue: ${:.2}", order.revenue);
            }
        }

        println!("\n💰 Financial Summary:");
        println!("   Cost: ${:.2}", order.cost);
        println!("   Revenue: ${:.2}", order.discounted_revenue);
        println!(
            "   Margin: ${:.2} ({:.1}%)",
            order.final_margin,
            order.margin_pct * 100.0
        );
        println!("{}", "=".repeat(60));
    })
}

pub fn summary_printer() -> impl SinkHandler + Clone + std::fmt::Debug + 'static {
    SinkTyped::new(|summary: CatalogAnalyticsSummary| async move {
        println!("\n\n{}", "=".repeat(60));
        println!("🎯 FINAL ANALYTICS DASHBOARD");
        println!("{}", "=".repeat(60));
        println!("Total Orders Processed: {}", summary.order_count);
        println!(
            "Orders with Promotions: {} ({:.0}%)",
            summary.promo_orders,
            if summary.order_count > 0 {
                (summary.promo_orders as f64 / summary.order_count as f64) * 100.0
            } else {
                0.0
            }
        );
        println!("\n💰 Revenue Summary:");
        println!("   Total Revenue: ${:.2}", summary.total_revenue);
        println!("   Total Margin: ${:.2}", summary.total_margin);
        println!(
            "   Avg Margin %: {:.1}%",
            if summary.total_revenue > 0.0 {
                (summary.total_margin / summary.total_revenue) * 100.0
            } else {
                0.0
            }
        );
        println!("{}", "=".repeat(60));

        println!("\n💡 Join Strategy Summary:");
        println!("   ✅ InnerJoin (SKU→Product→Category): All orders matched");
        println!("   ✅ StrictJoin (Payment Validation): All payments valid");
        println!(
            "   ✨ LeftJoin (Promotions): {}/{} orders had promos",
            summary.promo_orders, summary.order_count
        );
        println!("\n   Note: LeftJoin preserved all orders, even without promotions!");
        println!(
            "   Note: StrictJoin would have failed on invalid payment (try INJECT_BAD_PAYMENT=1)"
        );
    })
}
