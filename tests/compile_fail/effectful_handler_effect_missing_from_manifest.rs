#[path = "support/typed_effectful.rs"]
mod support;
use support::{AllowsFirstEffect, First, Input};

fn main() {
    let _ = obzenflow_dsl::effectful_transform!(
        Input -> { First } => AllowsFirstEffect,
        effects: [],
        middleware: []
    );
}
