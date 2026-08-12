use obzenflow_adapters::sinks::csv::CsvSink;
use obzenflow_core::TypedPayload;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct Input {
    value: u64,
}

impl TypedPayload for Input {
    const EVENT_TYPE: &'static str = "compile.csv.input";
}

fn main() {
    let sink = CsvSink::<Input>::new("unused.csv").unwrap();
    let _other_stage = sink.clone();
}
