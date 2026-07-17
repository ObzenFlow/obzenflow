#[path = "support/typed_effectful.rs"]
mod support;
use support::{First, Input, StatefulAllowsFirstEffect};

fn main() {
    let _ = obzenflow_dsl::effectful_stateful!(
        Input -> { First } => StatefulAllowsFirstEffect,
        effects: [],
        middleware: []
    );
}
