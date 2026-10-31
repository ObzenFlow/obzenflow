# Piggy Bank

Post a merchant's retried payment, see the fee it cost, and replay the run
through a corrected fee rule.

## Run

```sh
cargo run -p obzenflow --example http_ingestion_piggy_bank_demo --features prometheus,web-host
```

Accounts open from [accounts.yaml](accounts.yaml), entries post from
[transactions.csv](transactions.csv), and merchants' ACH debits from
[ach-debits.csv](ach-debits.csv). The fee report prints each fee charged under
`fee_per_return`, the default rule.

## Post a retry

While the run is up, post NORTHSIDE FIT's retry. Resending it under the same
key posts it once:

```sh
curl -XPOST http://127.0.0.1:9090/api/bank/ach/events \
  -H 'content-type: application/json' \
  -H 'Idempotency-Key: "ach-20260306-000412"' \
  -d '{"event_type":"bank.ach_debit","data":{
        "account_id":"chk-58213",
        "company_name":"NORTHSIDE FIT",
        "company_id":"1834577921",
        "entry_description":"RETRY PYMT",
        "amount_cents":4999}}'
```

## Show a fee

Stop the run with Ctrl-C, then list its records with the archive path it
printed:

```sh
cargo run -p obzenflow --features cli --bin obzenflow -- show <archive>
```

## Replay through the corrected rule

```sh
cargo run -p obzenflow --example http_ingestion_piggy_bank_demo --features prometheus,web-host -- \
  --config examples/http_ingestion_piggy_bank_demo/obzenflow.fee-rule-v2.toml \
  --replay-from <archive> --verify
```

The replay decides every presentment again under `one_fee_per_payment`. It
prints each fee it still charges and each retry it now returns with no fee.
`--verify` lists where they differ and exits with an error, because the
decisions changed.

Source: [flow](flow.rs), [fees](handlers.rs) and [records](domain.rs).
