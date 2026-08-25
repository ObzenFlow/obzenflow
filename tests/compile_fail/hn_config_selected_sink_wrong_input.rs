use obzenflow::sinks;
use obzenflow_core::ai::{AiMapReduceChunkFailed, AiMapReducePlanningManifest};
use obzenflow_dsl::sink;

enum DigestOutput {
    Console,
    Postgres,
}

fn main() {
    let configured_output = DigestOutput::Console;

    let _digest_summary = match configured_output {
        DigestOutput::Console => {
            let output = sinks::console(|_: &AiMapReducePlanningManifest| String::new());
            sink!(AiMapReducePlanningManifest => output)
        }
        DigestOutput::Postgres => {
            let wrong_input = sinks::console(|_: &AiMapReduceChunkFailed| String::new());
            sink!(AiMapReducePlanningManifest => wrong_input)
        }
    };
}
