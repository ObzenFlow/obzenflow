// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{console, domain::*, handlers, sources::*};
use obzenflow::flow::{flow, join, sink, source, stateful, FlowDefinition};
use obzenflow::journal::disk_journals;
use obzenflow::middleware::RateLimiterBuilder;
use obzenflow::stages::sinks::SinkTyped;
use obzenflow::stages::{joins, stateful};
use std::path::PathBuf;

pub fn build_flow(journal_root: PathBuf) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let sku_products_handler = joins::inner(
            |p: &Product| p.product_id.clone(),
            |s: &Sku| s.product_id.clone(),
            handlers::enrich_sku,
        );

        let sku_full_dim_handler = joins::inner(
            |c: &Category| c.category_id.clone(),
            |sp: &SKUWithProduct| sp.category_id.clone(),
            handlers::add_category,
        );

        let payment_validated_handler = joins::strict(
            |pm: &PaymentMethod| pm.payment_id.clone(),
            |order: &OrderEvent| order.payment_id.clone(),
            handlers::validate_payment,
        );

        let enriched_orders_handler = joins::inner(
            |dim: &SKUFullDimension| dim.sku_id.clone(),
            |order: &ValidatedOrder| order.sku_id.clone(),
            handlers::enrich_order,
        );

        let promo_enriched_handler = joins::left(
            |promo: &Promotion| promo.sku_id.clone(),
            |order: &EnrichedOrder| order.sku_id.clone(),
            handlers::apply_promotion,
        );

        let categories_handler = categories_source();
        let products_handler = products_source();
        let skus_handler = skus_source();
        let promotions_handler = promotions_source();
        let payment_methods_handler = payment_methods_source();
        let orders_handler = orders_source();
        let per_order_printer_handler =
            SinkTyped::new(|order: EnrichedOrderWithPromo| async move {
                console::print_order(&order);
            })
            .idempotent();
        let catalog_stats_handler = stateful::reduce(
            CatalogAnalyticsSummary::default(),
            handlers::summarise_order,
        )
        .emit_on_eof();
        let summary_printer_handler =
            SinkTyped::new(|summary: CatalogAnalyticsSummary| async move {
                console::print_summary(&summary);
            })
            .idempotent();

        Ok(flow! {
            name: "product_catalog_enrichment",
            journals: disk_journals(journal_root),

            stages: {
                categories = source!(Category => categories_handler);
                products = source!(Product => products_handler);
                skus = source!(Sku => skus_handler);
                promotions = source!(Promotion => promotions_handler);
                payment_methods = source!(PaymentMethod => payment_methods_handler);

                orders = source!(OrderEvent => orders_handler);

                sku_products =
                    join!(catalog products: Product, Sku -> SKUWithProduct => sku_products_handler);

                sku_full_dim = join!(
                    catalog categories: Category,
                    SKUWithProduct -> SKUFullDimension => sku_full_dim_handler
                );

                payment_validated = join!(
                    catalog payment_methods: PaymentMethod,
                    OrderEvent -> ValidatedOrder => payment_validated_handler
                );

                enriched_orders = join!(
                    catalog sku_full_dim: SKUFullDimension,
                    ValidatedOrder -> EnrichedOrder => enriched_orders_handler
                );

                promo_enriched = join!(
                    catalog promotions: Promotion,
                    EnrichedOrder -> EnrichedOrderWithPromo => promo_enriched_handler
                );

                per_order_printer = sink!(
                    EnrichedOrderWithPromo => per_order_printer_handler with [
                        RateLimiterBuilder::new(0.5).build()
                    ]
                );

                catalog_stats = stateful!(
                    EnrichedOrderWithPromo -> CatalogAnalyticsSummary => catalog_stats_handler
                );

                summary_printer = sink!(CatalogAnalyticsSummary => summary_printer_handler);
            },

            topology: {
                (products, skus) |> sku_products;
                (categories, sku_products) |> sku_full_dim;

                (payment_methods, orders) |> payment_validated;
                (sku_full_dim, payment_validated) |> enriched_orders;
                (promotions, enriched_orders) |> promo_enriched;
                promo_enriched |> per_order_printer;
                promo_enriched |> catalog_stats;
                catalog_stats |> summary_printer;
            }
        })
    })
}
