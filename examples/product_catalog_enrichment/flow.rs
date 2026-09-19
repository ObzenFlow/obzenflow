// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::console;
use super::domain::*;
use super::sources::*;
use anyhow::Result;
use obzenflow::{joins, stateful};
use obzenflow_adapters::middleware::RateLimiterBuilder;
use obzenflow_dsl::{flow, join, sink, source, stateful, FlowDefinition};
#[cfg(not(test))]
use obzenflow_infra::application::{FlowApplication, LogLevel, Presentation};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::sink::SinkTyped;

fn build_flow() -> FlowDefinition {
    build_flow_at(
        std::path::PathBuf::from("target/catalog-logs"),
        #[cfg(test)]
        ProofProbe::default(),
    )
}

fn build_flow_at(
    journal_root: std::path::PathBuf,
    #[cfg(test)] probe: ProofProbe,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let sku_products_handler = joins::inner(
            |p: &Product| p.product_id.clone(),
            |s: &Sku| s.product_id.clone(),
            |product: Product, sku: Sku| SKUWithProduct {
                sku_id: sku.sku_id,
                variant: sku.variant,
                unit_cost: sku.unit_cost,
                current_price: sku.current_price,
                product_id: sku.product_id,
                product_name: product.product_name,
                brand: product.brand,
                category_id: product.category_id,
            },
        );

        let sku_full_dim_handler = joins::inner(
            |c: &Category| c.category_id.clone(),
            |sp: &SKUWithProduct| sp.category_id.clone(),
            |category: Category, sku_product: SKUWithProduct| SKUFullDimension {
                sku_id: sku_product.sku_id,
                variant: sku_product.variant,
                unit_cost: sku_product.unit_cost,
                current_price: sku_product.current_price,
                product_name: sku_product.product_name,
                brand: sku_product.brand,
                category_name: category.category_name,
                department: category.department,
                margin_target: category.margin_target,
            },
        );

        let payment_validated_handler = joins::strict(
            |pm: &PaymentMethod| pm.payment_id.clone(),
            |order: &OrderEvent| order.payment_id.clone(),
            |payment: PaymentMethod, order: OrderEvent| ValidatedOrder {
                order_id: order.order_id,
                sku_id: order.sku_id,
                quantity: order.quantity,
                payment_id: order.payment_id,
                card_type: payment.card_type,
                risk_score: payment.risk_score,
                timestamp: order.timestamp,
            },
        );

        let enriched_orders_handler = joins::inner(
            |dim: &SKUFullDimension| dim.sku_id.clone(),
            |order: &ValidatedOrder| order.sku_id.clone(),
            |dimension: SKUFullDimension, order: ValidatedOrder| {
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
            },
        );

        let promo_enriched_handler = joins::left(
            |promo: &Promotion| promo.sku_id.clone(),
            |order: &EnrichedOrder| order.sku_id.clone(),
            |promo: Option<Promotion>, order: EnrichedOrder| {
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
            },
        );

        let categories_handler = categories_source();
        let products_handler = products_source();
        let skus_handler = skus_source();
        let promotions_handler = promotions_source();
        let payment_methods_handler = payment_methods_source();
        let orders_handler = orders_source();
        #[cfg(test)]
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
            |summary: &mut CatalogAnalyticsSummary, order: &EnrichedOrderWithPromo| {
                summary.order_count += 1;
                summary.total_revenue += order.discounted_revenue;
                summary.total_margin += order.final_margin;

                if order.promo_code.is_some() && order.discount_pct.is_some() {
                    summary.promo_orders += 1;
                }
            },
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
            #[cfg(test)]
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
        #[cfg(test)]
        if probe.invalid_wiring {
            return Ok(catalog_flow!(payment_validated |> enriched_orders;));
        }
        Ok(catalog_flow!((sku_full_dim, payment_validated) |> enriched_orders;))
    })
}

#[cfg(not(test))]
const CONFIG_FILE: &str = "examples/product_catalog_enrichment/obzenflow.toml";

#[cfg(not(test))]
pub fn run_example(presentation: Presentation) -> Result<()> {
    FlowApplication::builder()
        .with_log_level(LogLevel::Info)
        .with_presentation(presentation)
        .with_config_file(CONFIG_FILE)
        .run_blocking(build_flow())?;

    Ok(())
}

/// Test-friendly runner that bypasses CLI parsing (clap would otherwise read cargo test args)
#[cfg(test)]
pub fn run_example_in_tests() -> Result<()> {
    run_flow_in_tests(build_flow(), "Flow execution failed")
}

#[cfg(test)]
fn run_flow_in_tests(
    flow: obzenflow_dsl::FlowDefinition,
    error_context: &'static str,
) -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    // These helpers intentionally bypass FlowApplication:
    // - `FlowApplication` parses CLI args, which clashes with `cargo test` arguments.
    // - `FlowApplication` installs global tracing via `.init()`, while tests need a
    //   best-effort tracer that stays harmless across repeated runs in one process.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to create runtime: {e:?}"))?;

    runtime.block_on(async move {
        let handle = flow
            .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
            .await?;
        handle
            .run()
            .await
            .map_err(|e| anyhow::anyhow!("{error_context}: {e:?}"))
    })
}

/// Counts the real example's source reads and journal-provider evaluation.
/// The only control mutation removes the explicit catalog input to enriched_orders.
#[cfg(test)]
#[derive(Clone, Debug, Default)]
pub struct ProofProbe {
    pub source_reads: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    pub journal_providers: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    pub invalid_wiring: bool,
}

#[cfg(test)]
impl ProofProbe {
    fn source<H>(&self, inner: H) -> CountedSource<H> {
        CountedSource {
            inner,
            calls: self.source_reads.clone(),
        }
    }
}

#[cfg(test)]
#[derive(Clone, Debug)]
struct CountedSource<H> {
    inner: H,
    calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

#[cfg(test)]
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

#[cfg(test)]
#[allow(dead_code)]
pub fn build_for_proof(journal_root: std::path::PathBuf, probe: ProofProbe) -> FlowDefinition {
    build_flow_at(journal_root, probe)
}
