#[path = "../examples/product_catalog_enrichment/support.rs"]
mod product_catalog_enrichment;

#[test]
#[ignore = "long-running end-to-end flow; enable when validating example behavior"]
fn product_catalog_enrichment_completes() {
    std::env::remove_var("INJECT_BAD_PAYMENT");
    product_catalog_enrichment::flow::run_example_in_tests()
        .expect("product_catalog_enrichment example should complete");
}
