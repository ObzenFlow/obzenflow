#[path = "support/typed_effectful.rs"]
mod support;
use support::{First, FirstAndSecond, Input};

fn main() {
    let _ = obzenflow_dsl::effectful_transform!(
        Input -> { First } => FirstAndSecond,
        effects: [],
        middleware: []
    );
}
