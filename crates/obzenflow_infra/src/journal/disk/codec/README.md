# Journal format

The current schema is **5.0**. Core's `JOURNAL_SCHEMA_VERSION` is the single
version for records, frame encoding, archive interpretation, and the run
manifest. `run_manifest.json` records `journal_schema_version: "5.0"`.
Frame markers and disposable observation
checkpoint stamps derive from that same authority. A breaking change to any of
these contracts bumps the one version. Package versions remain provenance.

Earlier development archives must be re-recorded. Readers reject unsupported
schemas before typed manifest decoding or record interpretation, including with
`--allow-incomplete-archive`. There are no legacy readers or conversion paths.
The logical field layout retains absolute numbers and complete immutable
definitions; JSONL export remains the expanded current logical record.

## Framing

All fixed-width integers are little endian. A frame consists of:

| Position | Bytes | Meaning |
|---|---:|---|
| 0 | 6 | Magic `OJF` followed by `JOURNAL_SCHEMA_VERSION` (`OJF5.0`) |
| 6 | 8 | Body length |
| 14 | 4 | CRC32 of magic and body length |
| 18 | body length | Compact body |
| after body | 4 | CRC32 of header and body |
| after checksum | 8 | Complete frame length, including header and trailer |
| final | 6 | Reversed header magic (`0.5FJO`) |

The 18-byte trailer commits the entire ordinary record or atomic group. A reader
validates both lengths, magic values and checksums before exposing members.
Reverse reads first follow checked header lengths from the latest indexed frame
offset to establish the physical tail boundary, then use terminal lengths.
This prevents a torn payload ending with an embedded frame from inventing a
boundary. Reopened journals rebuild the disposable index through the same
forward scanner. No reader uses newline discovery.
An incomplete final frame follows the existing live/incomplete-archive policy;
invalid complete frames are corruption. A partial header with incorrect bytes is
corruption, not a legacy input or a skippable empty line.

## Body layout

Every count, length, ordinal and offset below is a complete unsigned LEB128
value. Slots and ordinals are zero-based unless stated otherwise.

1. External journal-name count and names. Name tag `0` carries a UTF-8 string;
   `1` means `system.log`; `2` carries a UTF-8 prefix and 16-byte ULID and expands
   to `<prefix>_stage_<ULID>.log`. Journal ordinal `0` means the consuming journal;
   external names have ordinals `1..N`. Names must be single archive-local
   basenames. This table is local to the frame and has no external authority.
2. Definition-slot count and entries. Each entry starts with its kind byte:
   writer `0`, flow/stage context `1`, complete origin `2`, descriptor `3`,
   capture scope `4`, ordered clock-key names `5`, physical journal-writer ID
   `6`. Storage tag `0` carries a
   length-delimited complete body. Tag `1` carries a journal ordinal, absolute
   carrier-frame offset and definition slot. Each use in a record is the
   current frame's definition-slot ordinal, checked against its contextual kind.
3. Record tag `0` or group tag `1`. A group carries its nonempty UTF-8 identity
   and nonzero member count. Ordinary frames contain exactly one record.
4. Each record has length-delimited provenance; an observation-presence byte
   (`0` absent, `1` null, `2` followed by length-delimited observations); and
   length-delimited business/protected JSON. Member order is unchanged.

A referenced definition must be local in its carrier, never another reference.
Carrier framing, metadata and record/group section boundaries are checked
without materialising that carrier's records or following their references.

## Values

Unsigned integers use canonical unsigned LEB128 of their full value. Signed
integers use eight little-endian two's-complement bytes of the complete signed
value. Floats retain all 64 IEEE-754 bits, including negative zero. Strings are
UTF-8 with an unsigned length. Typed IDs retain all 128 bits. Opaque JSON is a
length-delimited JSON value; its application keys are never interpreted by the
codec. Timestamps retain their original units and precision.

Core types and their Serde declarations define the logical records.
The checked-in `layout.rs` maps their serialized names to current-schema
field positions, defaults and contextual value types. A structure stores explicit
field-presence/default masks followed by complete non-default values in layout order. Defaults are
fixed literals, never values inherited from a prior record. Absent, null, empty,
measured zero and negative zero remain distinct where the logical type permits
them. Unknown schema IDs, fields, tags, mask bits, overflow, overlong varints and
trailing bytes fail closed.

Each structure's unsigned mask allocates two bits per field, in the explicit
schema order: `0` absent, `1` null, `2` the field's fixed default, `3` a complete
explicit value. State `2` is invalid for fields without a declared default.
Nested non-positional typed values use primitive tags `0` null, `1` false,
`2` true, `3` unsigned, `4` signed, `5` float, `6` text, `7` array, `8` map.
Maps carry a count and key/value pairs. Key token `0` carries literal text;
`1..N` select the checked-in `NAMES` table. Opaque JSON bypasses these tokens.

Packet capture stamps are complete positional structures. A nested snapshot
capture has tag `0` plus a complete structure, or tag `1` for exact equality
with the current record's complete packet capture. Capture state is cleared
between group members. Equal owners or event IDs alone never select this tag.
Clock components always carry their own complete unsigned values.

A clock carries a reference to its complete ordered key-name list, followed by
one complete absolute unsigned value for every key. The definition contains no
clock values. Empty clocks and present zero-valued components remain distinct.
Typed clock-key strings use tag `0` plus text, `1` plus a raw ULID, `2` plus a
stage-writer ULID or `3` plus a system-writer ULID. Prefix encodings require
exact reconstruction of the original string. A stage-identity field can reuse a
complete `Stage` writer definition; a `System` writer is rejected in that slot.

Complete origins use the positional origin structure: complete `entry_time_ns`,
`entry_event_id` and optional opaque `metadata`. The former generated-metadata
alias and its body tag are removed. Application metadata keys are not renamed,
inferred or regenerated, including keys formerly generated by the framework.

## Immutable definitions and commitment

Definitions contain complete origin, context or descriptor values. Definition
bodies cannot contain other definition references. The frame that first needs
a value carries its definition alongside its ordinary records. Same-frame
references use a local slot; later references identify an archive-local journal,
absolute frame offset and definition slot. No EventId uniqueness is assumed.
Locators cannot escape the archive directory, name an uncommitted frame, or
resolve a different definition kind. Required provenance never depends on an
optional observation. No per-record numerical snapshot is interned.

Writer interning and reader definition caches share an 8-MiB retained-memory
budget per active archive. Accounting conservatively includes bucket slack,
both lookup maps, locator strings, scalar metadata and definition bodies. A
budget eviction releases map capacity as well as entries. Cache metrics report
hits, misses, carrier I/O and peak charged bytes. Transient frame/group bodies
are separate and proportional to the addressed frames, including carrier frames.
Cache misses and concurrent first sightings may produce duplicate definitions.
Definitions become reusable only after successful frame commitment. A rollback
publishes none. The provider never waits for another journal's pending definition
while holding its write lock. Required definitions live in retained journal
frames; caches and indexes are disposable accelerators.

Cached values carry file identity, length and modification stamps. Referenced
files must still exist as regular files; replacements, truncations and
same-length edits force validation of the carrier bytes. Open journals retain
their append-only contract. Relocation works because durable references contain
basenames and offsets, never the writer's original absolute directory.

Readers validate directly referenced frames and return full logical values.
They apply zero preceding numerical updates and follow at most one definition
dependency hop. Definition metadata adds no logical record, clock tick, reader
position, transport credit, receipt or execution authority.

`serialize.rs` and `deserialize.rs` stream positional structures through Core's
Serde implementations. They share the same `layout.rs` slots and scalar rules
with the metadata/dynamic-value codec, avoiding a second full JSON object tree.
Payload classification and validation remain Core's `JournalPayload` methods.

## Tests and fixtures

`test_data.rs` builds deterministic records using current Core types. Its stream
contains 16 source facts, 16 transformed facts and 16 delivery receipts, with
distinct event IDs, varying absolute accounting and clocks, and 17 complete
origins reused across related records. Source and transform records carry runtime
measurements; receipts carry snapshots only. Separate tests exercise boundary
values and optional application metadata.

The timing test repeats this 48-record stream to compare the compact codec and
an uncompressed control over identical logical records. It measures a warm,
low-cardinality workload; production-flow cost and cache eviction need separate
measurements.

The current `fixtures/*.frame` files and matching JSON records cover all
observation families and absent attachments. Tests require byte-for-byte encoding
and exact logical decoding within the current schema. After a deliberate schema
bump, regenerate frames from the JSON records with the existing
`current_schema_fixtures_preserve_bytes_and_logical_records` test and
`UPDATE_JOURNAL_FIXTURES=1`, then run it normally to check the captured bytes.
