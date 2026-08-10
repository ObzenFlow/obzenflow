// SPDX-License-Identifier: MIT OR Apache-2.0

#[path = "../support/typed_join.rs"]
mod support;
use support::{Exact, First, Reference, Stream};

fn main() {
    let _ = obzenflow_dsl::join!(
        catalog reference_stage: Reference,
        Stream -> { First, First } => Exact
    );
}
