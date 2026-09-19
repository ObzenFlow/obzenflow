// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Instrumented catalog flow for rejection and replay tests.
//! Business transformations, domain types and input fixtures come from the demo.

use crate::product_catalog_enrichment::{console, domain::*, handlers, sources::*};
use anyhow::Result;
use obzenflow::{joins, stateful};
use obzenflow_adapters::middleware::RateLimiterBuilder;
use obzenflow_dsl::{flow, join, sink, source, stateful, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::sink::SinkTyped;

pub fn build_for_proof(journal_root: std::path::PathBuf, probe: ProofProbe) -> FlowDefinition {
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
        let (
            categories_handler,
            products_handler,
            skus_handler,
            promotions_handler,
            payment_methods_handler,
            orders_handler,
        ) = (
            probe.source(categories_handler),
            probe.source(products_handler),
            probe.source(skus_handler),
            probe.source(promotions_handler),
            probe.source(payment_methods_handler),
            probe.source(orders_handler),
        );
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

        // The proof's rejected control changes only the enrichment input below.
        macro_rules! catalog_flow {
            ($($enrichment_input:tt)*) => { flow! {
        name: "product_catalog_enrichment",
        journals: {
            probe.journal_providers.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            disk_journals(journal_root)
        },

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

            catalog_stats = stateful!(EnrichedOrderWithPromo -> CatalogAnalyticsSummary => catalog_stats_handler);

            summary_printer = sink!(CatalogAnalyticsSummary => summary_printer_handler);
        },

        topology: {
            (products, skus) |> sku_products;
            (categories, sku_products) |> sku_full_dim;

            (payment_methods, orders) |> payment_validated;
            $($enrichment_input)*
            (promotions, enriched_orders) |> promo_enriched;
            promo_enriched |> per_order_printer;
            promo_enriched |> catalog_stats;
            catalog_stats |> summary_printer;
        }
        }};
        }
        if probe.invalid_wiring {
            return Ok(catalog_flow!(payment_validated |> enriched_orders;));
        }
        Ok(catalog_flow!((sku_full_dim, payment_validated) |> enriched_orders;))
    })
}

/// Counts source reads and journal-provider evaluation in the test fixture.
/// The only control mutation removes the explicit catalog input to enriched_orders.
#[derive(Clone, Debug, Default)]
pub struct ProofProbe {
    pub source_reads: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    pub journal_providers: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    pub invalid_wiring: bool,
}

impl ProofProbe {
    fn source<H>(&self, inner: H) -> CountedSource<H> {
        CountedSource {
            inner,
            calls: self.source_reads.clone(),
        }
    }
}

#[derive(Clone, Debug)]
struct CountedSource<H> {
    inner: H,
    calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl<H: obzenflow_runtime::stages::common::handlers::TypedFiniteSourceHandler>
    obzenflow_runtime::stages::common::handlers::TypedFiniteSourceHandler for CountedSource<H>
{
    type Output = H::Output;
    fn next(
        &mut self,
    ) -> Result<Option<Vec<Self::Output>>, obzenflow_runtime::stages::common::handlers::SourceError>
    {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.next()
    }
}
