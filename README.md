# `mathilde-binary-transport`

## Index

- [What this crate is](#what-this-crate-is)
- [What this crate is not](#what-this-crate-is-not)
- [Format summary (`MATHLDBT` v1)](#format-summary-mathldbt-v1)
- [Quickstart](#quickstart)
- [Encoding options (opt-in)](#encoding-options-opt-in)
- [Determinism and correctness](#determinism-and-correctness)
- [Inventory generation](#inventory-generation)
- [Evidence and benchmarks](#evidence-and-benchmarks)
- [Examples](#examples)

## What this crate is

`mathilde-binary-transport` is a Rust library that implements `MATHLDBT` v1: a versioned, lossless, deterministic binary envelope for transporting columnar batches.

It provides:
- an in-memory `ColumnarBatch` model (schema + per-column buffers + validity bitmaps)
- a strict encoder/decoder for the `MATHLDBT` envelope
- workspace-based APIs for deterministic buffer reuse on repeated calls

## What this crate is not

This crate does not:
- connect to databases
- implement Postgres COPY BINARY parsing
- perform lossy compression

Compression (gzip/zstd/etc.) is intended as an external layer, applied to the encoded bytes when needed.

## Format summary (`MATHLDBT` v1)

Magic: `MATHLDBT` (8 bytes)  
Version: `1` (`u16`)  
Endian: all header/descriptor integers are little-endian

The encoder/decoder implements strict validation (bounds, lengths, offsets) and returns `Err` on malformed inputs.

## Quickstart

Add the crate and encode/decode a `ColumnarBatch`:

```rust
use mathilde_binary_transport::codec::mathldbt_v1::{
    decode_mathldbt_v1, encode_mathldbt_v1_into,
};

// Build a ColumnarBatch (see `mathilde_binary_transport::batch`).
// let batch: ColumnarBatch = ...;

let mut bytes = Vec::new();
encode_mathldbt_v1_into(&batch, &mut bytes)?;

let decoded = decode_mathldbt_v1(&bytes)?;
```

For repeated calls, reuse workspaces:

```rust
use mathilde_binary_transport::codec::mathldbt_v1::{
    decode_mathldbt_v1_with_workspace, encode_mathldbt_v1_into_with_workspace,
    MathldbtV1DecodeWorkspace, MathldbtV1EncodeWorkspace,
};

let mut enc_ws = MathldbtV1EncodeWorkspace::default();
let mut dec_ws = MathldbtV1DecodeWorkspace::default();
let mut bytes = Vec::new();

encode_mathldbt_v1_into_with_workspace(&batch, &mut bytes, &mut enc_ws)?;
let decoded = decode_mathldbt_v1_with_workspace(&bytes, &mut dec_ws)?;
```

## Encoding options (opt-in)

The default encoding is plain fixed-width / plain varlen.

Two encodings exist but are opt-in (no silent behavior change):
- `DictUtf8` (`encoding_id=2`) for `Utf8` and `JsonbText` varlen columns
- `DeltaVarintI64` (`encoding_id=3`) for `I64` and `TimestampTzMicros` when the validity bitmap is all-valid

They are controlled through `MathldbtV1EncodeWorkspace`:

```rust
let mut ws = MathldbtV1EncodeWorkspace::default();
ws.set_enable_dict_utf8(true)
  .set_enable_delta_varint_i64(true);
```

Encoding eligibility and determinism rules are specified in:
- `src/codec/mathldbt_v1.rs`
- `src/tests/test_mathldbt_v1.rs`

## Determinism and correctness

Contracts:
- decode validates bounds/lengths/offsets and returns `Err` (no panics in library code paths)
- floats are represented losslessly as IEEE-754 bit patterns (`u32`/`u64`)
- for identical inputs, encoding produces identical bytes (given identical encoder options)

The formal spec references were intentionally not published in this repository. The codec behavior is defined by the implementation and the tests.

## Inventory generation

This repo maintains:
- component inventory: `docs/inventory.md`
- generated global inventory: `inventory.md`

Generate `inventory.md` from `docs/inventory.md`:

```bash
cargo make inventory
```

This runs `bin/generate_global_inventory.rs` in strict mode and fails if any `.rs` file is missing a 1-line purpose in `docs/inventory.md`.

## Evidence and benchmarks

Evidence logs (append-only):
- `docs/evidence/bench_results.md`

Bench harness:
- `cargo bench --bench mathldbt_transport`

## Examples

See the test fixtures for concrete batch construction patterns:
- `src/tests/test_mathldbt_v1.rs`

## License

Licensed under either of:
- Apache License, Version 2.0 (`LICENSE-APACHE`)
- MIT license (`LICENSE-MIT`)
