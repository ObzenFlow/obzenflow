# Hacker News AI digest

Summarize the top Hacker News stories with a local model, then resume a killed
live run and replay it with the model stopped.

Every run except a replay needs Ollama serving `llama3.1:8b`.

## Run against the mock

```sh
cargo run -p obzenflow --example hn_ai_digest_demo --features "http-pull ai postgres" -- \
  --config examples/hn_ai_digest_demo/obzenflow.toml
```

## Kill, resume and replay a live run

```sh
cargo run -p obzenflow --example hn_ai_digest_demo --features "http-pull ai postgres" -- \
  --config examples/hn_ai_digest_demo/obzenflow.live.toml
```

Kill the run once `hn_stories` logs that it sent EOF. A live run killed before
then can't resume, because the live feed can't be read again from where it
stopped. Resume it with the archive path it printed:

```sh
cargo run -p obzenflow --example hn_ai_digest_demo --features "http-pull ai postgres" -- \
  --config examples/hn_ai_digest_demo/obzenflow.live.toml --resume-from <archive>
```

Stop Ollama, then replay the resumed run with no AI configuration:

```sh
cargo run -p obzenflow --example hn_ai_digest_demo --features "http-pull ai postgres" -- \
  --config examples/hn_ai_digest_demo/obzenflow.replay.toml \
  --replay-from <resumed-archive> --verify
```

For PostgreSQL output, use [its configuration](obzenflow.postgres.toml) and the
[local PostgreSQL guide](../../dev/postgres/README.md).

Source: [flow](flow.rs) and [settings](config.rs).
