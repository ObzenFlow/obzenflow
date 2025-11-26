use super::domain::EnrichedOrderWithPromo;
use async_trait::async_trait;
use obzenflow_core::{
    event::{
        chain_event::ChainEvent,
        payloads::delivery_payload::{DeliveryMethod, DeliveryPayload},
    },
    TypedPayload,
};
use obzenflow_runtime_services::stages::common::handlers::SinkHandler;

#[derive(Clone, Debug)]
pub struct DashboardSink {
    order_count: usize,
    total_revenue: f64,
    total_margin: f64,
    promo_orders: usize,
}

impl DashboardSink {
    pub fn new() -> Self {
        Self {
            order_count: 0,
            total_revenue: 0.0,
            total_margin: 0.0,
            promo_orders: 0,
        }
    }
}

#[async_trait]
impl SinkHandler for DashboardSink {
    async fn consume(&mut self, event: ChainEvent) -> obzenflow_core::Result<DeliveryPayload> {
        if let Some(order) = EnrichedOrderWithPromo::from_event(&event) {
            self.order_count += 1;

            println!("\n{}", "=".repeat(60));
            println!("📊 ORDER #{}: {}", self.order_count, order.order_id);
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
                    self.promo_orders += 1;
                    println!(
                        "\n🎟️  Promotion: {} ({})",
                        code,
                        order.promo_type.as_ref().unwrap()
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

            self.total_revenue += order.discounted_revenue;
            self.total_margin += order.final_margin;

            println!("{}", "=".repeat(60));
        }
        // Note: EOF events are not passed to consume() - use drain() for final summary

        Ok(DeliveryPayload::success(
            "dashboard",
            DeliveryMethod::Custom("Display".to_string()),
            None,
        ))
    }

    /// Called during graceful shutdown - print final analytics summary
    async fn drain(&mut self) -> obzenflow_core::Result<Option<DeliveryPayload>> {
        println!("\n\n{}", "=".repeat(60));
        println!("🎯 FINAL ANALYTICS DASHBOARD");
        println!("{}", "=".repeat(60));
        println!("Total Orders Processed: {}", self.order_count);
        println!(
            "Orders with Promotions: {} ({:.0}%)",
            self.promo_orders,
            if self.order_count > 0 {
                (self.promo_orders as f64 / self.order_count as f64) * 100.0
            } else {
                0.0
            }
        );
        println!("\n💰 Revenue Summary:");
        println!("   Total Revenue: ${:.2}", self.total_revenue);
        println!("   Total Margin: ${:.2}", self.total_margin);
        println!(
            "   Avg Margin %: {:.1}%",
            if self.total_revenue > 0.0 {
                (self.total_margin / self.total_revenue) * 100.0
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
            self.promo_orders, self.order_count
        );
        println!("\n   Note: LeftJoin preserved all orders, even without promotions!");
        println!(
            "   Note: StrictJoin would have failed on invalid payment (try INJECT_BAD_PAYMENT=1)"
        );

        Ok(None)
    }
}
