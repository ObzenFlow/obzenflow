#[path = "support/typed_effectful.rs"]
mod support;
use support::{First, Input, StatefulFirstAndSecond};

fn main() {
    let _ = obzenflow_dsl::effectful_stateful!(
        Input -> { First } => StatefulFirstAndSecond,
        effects: [],
        middleware: []
    );
}
