#[path = "domain.rs"]
mod domain;

#[path = "fixtures.rs"]
mod fixtures;

#[path = "flow.rs"]
mod flow;

pub use flow::run_example;

