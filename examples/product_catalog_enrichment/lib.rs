#[path = "domain.rs"]
pub mod domain;

#[path = "fixtures.rs"]
pub mod fixtures;

#[path = "sources.rs"]
pub mod sources;

#[path = "sinks.rs"]
pub mod sinks;

#[path = "flow.rs"]
pub mod flow;

pub use flow::run_example;
