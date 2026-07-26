use obzenflow_adapters::middleware::control::ControlMiddlewareAggregator;
use obzenflow_adapters::middleware::MiddlewareFactory;
use obzenflow_runtime::pipeline::config::StageConfig;
use std::sync::Arc;

fn removed_create(factory: &dyn MiddlewareFactory, config: &StageConfig) {
    let control = Arc::new(ControlMiddlewareAggregator::new());
    let _ = factory.create(config, control.clone());
    let _ = factory.create_for_effect(config, control, "example.effect");
}

fn main() {}
