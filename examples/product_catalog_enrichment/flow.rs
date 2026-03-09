// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::*;
use super::sinks::{per_order_printer, summary_printer, CatalogAnalyticsSummary};
use super::sources::*;
use anyhow::Result;
use obzenflow_adapters::middleware::RateLimiterBuilder;
use obzenflow_dsl::{flow, join, sink, source, stateful, with_ref};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::join::{InnerJoinBuilder, LeftJoinBuilder, StrictJoinBuilder};
use obzenflow_runtime::stages::stateful::strategies::accumulators::ReduceTyped;

fn build_flow() -> obzenflow_dsl::FlowDefinition {
    flow! {
        name: "product_catalog_enrichment",
        journals: disk_journals(std::path::PathBuf::from("target/catalog-logs")),
        middleware: [],

        stages: {
            categories = source!("categories" => categories_source());
            products = source!("products" => products_source());
            skus = source!("skus" => skus_source());
            promotions = source!("promotions" => promotions_source());
            payment_methods = source!("payment_methods" => payment_methods_source());

            orders = source!("orders" => orders_source());

            sku_products = join!("sku_products" =>
                with_ref!(products,
                    InnerJoinBuilder::<Product, Sku, SKUWithProduct>::new()
                        .catalog_key(|p: &Product| p.product_id.clone())
                        .stream_key(|s: &Sku| s.product_id.clone())
                        .build(|product: Product, sku: Sku| SKUWithProduct {
                            sku_id: sku.sku_id,
                            variant: sku.variant,
                            unit_cost: sku.unit_cost,
                            current_price: sku.current_price,
                            product_id: sku.product_id,
                            product_name: product.product_name,
                            brand: product.brand,
                            category_id: product.category_id,
                        })
                )
            );

            sku_full_dim = join!("sku_full_dim" =>
                with_ref!(categories,
                    InnerJoinBuilder::<Category, SKUWithProduct, SKUFullDimension>::new()
                        .catalog_key(|c: &Category| c.category_id.clone())
                        .stream_key(|sp: &SKUWithProduct| sp.category_id.clone())
                        .build(|category: Category, sku_product: SKUWithProduct| SKUFullDimension {
                            sku_id: sku_product.sku_id,
                            variant: sku_product.variant,
                            unit_cost: sku_product.unit_cost,
                            current_price: sku_product.current_price,
                            product_name: sku_product.product_name,
                            brand: sku_product.brand,
                            category_name: category.category_name,
                            department: category.department,
                            margin_target: category.margin_target,
                        })
                )
            );

            payment_validated = join!("payment_validated" =>
                with_ref!(payment_methods,
                    StrictJoinBuilder::<PaymentMethod, OrderEvent, ValidatedOrder>::new()
                        .catalog_key(|pm: &PaymentMethod| pm.payment_id.clone())
                        .stream_key(|order: &OrderEvent| order.payment_id.clone())
                        .build(|payment: PaymentMethod, order: OrderEvent| ValidatedOrder {
                            order_id: order.order_id,
                            sku_id: order.sku_id,
                            quantity: order.quantity,
                            payment_id: order.payment_id,
                            card_type: payment.card_type,
                            risk_score: payment.risk_score,
                            timestamp: order.timestamp,
                        })
                )
            );

            enriched_orders = join!("enriched_orders" =>
                with_ref!(sku_full_dim,
                    InnerJoinBuilder::<SKUFullDimension, ValidatedOrder, EnrichedOrder>::new()
                        .catalog_key(|dim: &SKUFullDimension| dim.sku_id.clone())
                        .stream_key(|order: &ValidatedOrder| order.sku_id.clone())
                        .build(|dimension: SKUFullDimension, order: ValidatedOrder| {
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
                        })
                )
            );

            promo_enriched = join!("promo_enriched" =>
                with_ref!(promotions,
                    LeftJoinBuilder::<Promotion, EnrichedOrder, EnrichedOrderWithPromo>::new()
                        .catalog_key(|promo: &Promotion| promo.sku_id.clone())
                        .stream_key(|order: &EnrichedOrder| order.sku_id.clone())
                        .build(|promo: Option<Promotion>, order: EnrichedOrder| {
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
                        })
                )
            );

            per_order = sink!(
                "per_order_printer" => per_order_printer(),
                [RateLimiterBuilder::new(0.5).build()]
            );

            stats = stateful!("catalog_stats" =>
                ReduceTyped::new(
                    CatalogAnalyticsSummary::default(),
                    |summary: &mut CatalogAnalyticsSummary, order: &EnrichedOrderWithPromo| {
                        summary.order_count += 1;
                        summary.total_revenue += order.discounted_revenue;
                        summary.total_margin += order.final_margin;

                        if order.promo_code.is_some() && order.discount_pct.is_some() {
                            summary.promo_orders += 1;
                        }
                    }
                ).emit_on_eof()
            );

            summary = sink!("summary_printer" => summary_printer());
        },

        topology: {
            skus |> sku_products;
            sku_products |> sku_full_dim;

            orders |> payment_validated;
            payment_validated |> enriched_orders;
            enriched_orders |> promo_enriched;
            promo_enriched |> per_order;
            promo_enriched |> stats;
            stats |> summary;
        }
    }
}

#[cfg(not(test))]
pub fn run_example() -> Result<()> {
    println!("🛒 FlowState RS - Product Catalog Enrichment");
    println!("{}", "=".repeat(60));
    println!("✨ Demonstrating All Three Join Strategies:");
    println!("   • InnerJoin: Core dimension enrichment (Category→Product→SKU)");
    println!("   • LeftJoin: Optional promotion enrichment");
    println!("   • StrictJoin: Critical payment validation (Jonestown Protocol)");
    println!("\n📚 Based on industrial-scale product catalog patterns");
    println!("{}", "=".repeat(60));

    if std::env::var("INJECT_BAD_PAYMENT").is_ok() {
        println!("\n🚨 WARNING: INJECT_BAD_PAYMENT is set!");
        println!("   Pipeline will trigger Jonestown Protocol on invalid payment.");
        println!("   StrictJoin will emit poison EOF and cascade shutdown.\n");
    }

    println!("\n📂 Loading Reference Data (Dimensions)...\n");

    obzenflow_infra::application::FlowApplication::builder()
        .with_log_level(obzenflow_infra::application::LogLevel::Info)
        .run_blocking(build_flow())?;

    println!("\n✅ Product catalog enrichment completed!");
    println!("\n💡 Try setting INJECT_BAD_PAYMENT=1 to see StrictJoin trigger Jonestown Protocol!");
    println!("📝 Journal written to: target/catalog-logs/");

    Ok(())
}

/// Test-friendly runner that bypasses CLI parsing (clap would otherwise read cargo test args)
#[cfg(test)]
pub fn run_example_in_tests() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to create runtime: {e:?}"))?;

    runtime.block_on(async {
        let handle = build_flow().await?;
        handle
            .run()
            .await
            .map_err(|e| anyhow::anyhow!("Flow execution failed: {e:?}"))
    })
}
