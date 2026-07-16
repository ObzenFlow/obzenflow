#[path = "support/typed_effectful.rs"]
mod support;
use support::{First, FirstOnly, Input, Second};

fn main() {
    let _ = obzenflow_dsl::effectful_transform!(
        Input -> { First, Second } => FirstOnly,
        effects: [],
        middleware: []
    );
}
