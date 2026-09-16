# Captured logical records from format 3

These 48 JSONL records come from the format-3 Prometheus proof archive
`flow_01M2KKNJ0V3Z5Z66KD4BA584CM`, recorded on 2026-09-15: the first 16
successful source facts, transformed facts and delivery receipts in its
supported logical export. They contain synthetic example data.

The source fixtures remain unchanged. The test loader explicitly removes the
approved format-4 retirements: processing placeholders, intent, the two terminal
group counters, the entry-stage label and the generated source metadata object.
It validates that the removed metadata has exactly the former framework shape.
This test-only projection is not a production migration or evidence that a
format-4 reader accepts format-3 archives.

The provider suite checks every remaining field against a fresh encode/decode and
uses the identical projected records for a test-only uncompressed control. Repeating
this small stream measures a warm, low-cardinality case; it is not the 100k
archive-size or cache-eviction proof. The production-flow integration test
provides those separate measurements. These are logical fixtures, independent
of the physical metadata addresses in the original development archive.
