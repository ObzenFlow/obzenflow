// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow::application::Banner;

pub fn banner() -> Banner {
    Banner::new("Product Catalog Enrichment")
        .description("The flow adds catalog data to each order with inner, left and strict joins.")
        .bullets(
            "Join strategies",
            [
                "An inner join adds the SKU, product and category to each order",
                "A left join adds a promotion to an order when its SKU has one",
                "A strict join checks that each order names a known payment method",
            ],
        )
        .section(
            "Final summary",
            "SingleSummaryDelivery, a policy that this example defines, lets summary_printer print the analytics summary only once.",
        )
        .section(
            "Fault injection",
            "Run with --config examples/product_catalog_enrichment/obzenflow.bad_payment.toml to set [app] inject_bad_payment = true. One order then names an unknown payment method.\nThe strict join starts the Jonestown Protocol when it reads that order. It keeps the orders that it joined before that order, and then it sends a Poison EOF downstream.",
        )
}
