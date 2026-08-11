#[path = "../support/typed_source.rs"]
mod support;
use support::*;

fn main() {
    let _ = obzenflow_dsl::async_source!(First => RawAsyncFinite);
}
