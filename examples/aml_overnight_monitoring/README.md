# Overnight AML transaction monitoring

Score every transaction of one day through a risk engine whose rules the flow never
reads, open a case for each flagged transaction, and resume a killed run without
scoring a transaction or opening a case twice.

The engine's [rules](risk_engine.rs) are illustrative, not an AML model, and the
dataset's laundering label never reaches the engine. The example makes no compliance
claim.

## Run the fixture

Run from the repository root. The checked-in fixture is a synthetic day of 279
transactions, planted so every rule fires, one transaction trips two rules, and some
accounts have no history. It was written for this example, contains no AMLworld rows,
and is covered by the repository's MIT or Apache-2.0 licence. It needs no download and
no extra Cargo feature:

```sh
rm -f target/aml-risk-calls.jsonl target/aml-case-calls.jsonl
cargo run -p obzenflow --example aml_overnight_monitoring
```

Cleared transactions go to `target/aml-cleared.csv` and cases to `target/aml-cases.csv`.
Each simulated system appends every call it receives to its own ledger,
`target/aml-risk-calls.jsonl` or `target/aml-case-calls.jsonl`.

A ledger holds the calls of one fresh run and the resumes that continue it, so remove
both ledgers before each fresh run and never before a resume. Replay a run without
calling either system, using the archive path the run prints:

```sh
cargo run -p obzenflow --example aml_overnight_monitoring -- \
  --replay-from <archive> --verify
```

## A bad night and unreadable replies

Each of two configurations changes one setting. The bad night reads a copy of
the fixture in which three rows carry their timestamp in another format. The
unreadable replies make the case system answer each whole-day alert without a
case number. Both nights complete, and `show` lists each rejected row or failed
alert from its error journal:

```sh
rm -f target/aml-risk-calls.jsonl target/aml-case-calls.jsonl
cargo run -p obzenflow --example aml_overnight_monitoring -- \
  --config examples/aml_overnight_monitoring/obzenflow.bad-night.toml
cargo run -p obzenflow --features cli --bin obzenflow -- show <archive>
```

Run the unreadable replies the same way with
`examples/aml_overnight_monitoring/obzenflow.unreadable-replies.toml`. Replay
either night with the configuration that recorded it.

Source: [flow](flow.rs), [stages](stages.rs), [risk engine](risk_engine.rs) and
[case system](case_system.rs).
See the [examples index](../README.md) for published tutorials.
