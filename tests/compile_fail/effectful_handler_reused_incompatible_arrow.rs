#[path = "support/typed_effectful.rs"]
mod support;
use support::{First, FirstOnly, Input, Second};

fn main() {
    let handler = FirstOnly;
    let _ = obzenflow_dsl::effectful_transform!(
        Input -> { First } => handler.clone(),
        effects: [],
        middleware: []
    );
    let _ = obzenflow_dsl::effectful_transform!(
        Input -> { First, Second } => handler,
        effects: [],
        middleware: []
    );
}
