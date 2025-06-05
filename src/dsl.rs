// src/dsl.rs

#[macro_export]
macro_rules! source {
    ($path:expr, $ty:expr) => {
        $crate::io::LogSource::new(std::path::PathBuf::from($path), $ty)
    };
}

#[macro_export]
macro_rules! sink {
    ($path:expr) => {
        $crate::io::LogSink::new(std::path::PathBuf::from($path))
    };
}

#[macro_export]
macro_rules! stage {
    ($expr:expr) => {
        $expr
    };
}

#[macro_export]
macro_rules! flow {
    (
        Source($src:expr)
        $( |> stage!($st:expr) )+
        |> Sink($sink:expr)
    ) => {{
        async {
            use $crate::prelude::*;

            let mut builder = PipelineBuilder::new();

            // Add source
            builder = builder.stage("source", $src);

            // Add intermediate stages
            let mut stage_num = 0;
            $(
                builder = builder.stage(&format!("stage_{}", stage_num), $st);
                stage_num += 1;
            )+

            // Add sink
            builder = builder.stage("sink", $sink);

            // Build and run the pipeline
            let pipeline = builder.build().await?;
            pipeline.wait().await?;

            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        }.await
    }};
}
