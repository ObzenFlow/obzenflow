# Captured current logical records

These 48 JSONL records come from the format-3 Prometheus proof archive
`flow_01M2KKNJ0V3Z5Z66KD4BA584CM`, recorded on 2026-09-15: the first 16
successful source facts, transformed facts and delivery receipts in its
supported logical export. They contain synthetic example data.

The provider suite checks every field against a fresh encode/decode and uses
the identical captured records for a test-only uncompressed control. Repeating
this small stream measures a warm, low-cardinality case; it is not the 100k
archive-size or cache-eviction proof. The production-flow integration test
provides those separate measurements. These are logical fixtures, independent
of the physical metadata addresses in the original development archive.
