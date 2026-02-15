# `mathilde-binary-transport`

## Index

- [What this crate is](#what-this-crate-is)
- [What this crate is not](#what-this-crate-is-not)
- [Format summary (`MATHLDBT` v1)](#format-summary-mathldbt-v1)
- [Quickstart (plain)](#quickstart-plain)
- [Quickstart (compressed)](#quickstart-compressed)
- [Workspace APIs](#workspace-apis)
- [Encoding options (opt-in)](#encoding-options-opt-in)
- [Compression model (wire layer)](#compression-model-wire-layer)
- [Determinism and correctness](#determinism-and-correctness)
- [Tests](#tests)
- [Benchmarks and evidence](#benchmarks-and-evidence)
- [WAN estimate tool](#wan-estimate-tool)
- [Inventory generation](#inventory-generation)
- [License](#license)

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
- define a self-describing “compressed frame” format

This crate optionally provides transport compression helpers (gzip/zstd) behind feature flags. Compression algorithm selection is out-of-band (for example via HTTP `Content-Encoding`).

## Format summary (`MATHLDBT` v1)

Magic: `MATHLDBT` (8 bytes)  
Version: `1` (`u16`)  
Endian: all header/descriptor integers are little-endian

The encoder/decoder implements strict validation (bounds, lengths, offsets) and returns `Err` on malformed inputs.

## Quickstart (plain)

Add the crate and encode/decode a `ColumnarBatch`:

```rust
use mathilde_binary_transport::codec::exports::encode_into;
use mathilde_binary_transport::codec::mathldbt_v1::decode_mathldbt_v1;

// Build a ColumnarBatch (see `mathilde_binary_transport::batch`).
// let batch: ColumnarBatch = ...;

let mut bytes = Vec::new();
encode_into(&batch, &mut bytes)?;

let decoded = decode_mathldbt_v1(&bytes)?;
```

## Quickstart (compressed)

If you want to compress the `MATHLDBT` bytes for transport, enable a feature and use the helper module.

This is parameter-driven compression:

`compressed_bytes = compress( encode_mathldbt_v1(batch) )`

Example (zstd):

```rust
use mathilde_binary_transport::codec::mathldbt_v1_compressed::{
    Compression, decode_mathldbt_v1_compressed, encode_mathldbt_v1_compressed_into,
};

let mut bytes = Vec::new();
encode_mathldbt_v1_compressed_into(&batch, &mut bytes, Compression::Zstd { level: 3 })?;

let decoded = decode_mathldbt_v1_compressed(
    &bytes,
    Compression::Zstd { level: 3 },
    64 * 1024 * 1024,
)?;
```

Features:
- `compression-zstd`
- `compression-gzip`

## Workspace APIs

For repeated calls, reuse workspaces to avoid repeated allocations and to keep behavior deterministic:

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

## Compression model (wire layer)

This repository separates two concerns:

- The `MATHLDBT` v1 envelope is the lossless, deterministic payload format.
- Compression (gzip/zstd) is a wire-layer choice.

The compressed helpers in `codec::mathldbt_v1_compressed` are provided for convenience, but the format intentionally does not introduce a second “compression frame” or embedded algorithm id.

The decoder requires `max_uncompressed_len` to bound decompression. The right value is application-dependent (we do not recommend a default in this repo yet).

## Determinism and correctness

Contracts:
- decode validates bounds/lengths/offsets and returns `Err` (no panics in library code paths)
- floats are represented losslessly as IEEE-754 bit patterns (`u32`/`u64`)
- for identical inputs, encoding produces identical bytes (given identical encoder options)

The formal spec references were intentionally not published in this repository. The codec behavior is defined by the implementation and the tests.

## Tests

Run the unit tests:

- `cargo test`

Compression feature matrix tests:

- `cargo test --features compression-zstd`
- `cargo test --features compression-gzip`
- `cargo test --features "compression-zstd compression-gzip"`

Repo convention:

- All tests live under `src/tests/test_*.rs` and are wired via `src/tests/mod.rs`.

## Benchmarks and evidence

Evidence logs (append-only):

- `docs/evidence/bench_results.md`

Bench harnesses:

- `cargo bench --bench mathldbt_transport`
- `cargo bench --bench mathldbt_transport --features "compression-zstd compression-gzip"`
- `cargo bench --bench json_vs_mathldbt`

Bench inputs are deterministic “bars-like” fixtures (not a DB snapshot). JSON baselines use `serde_json` row structs (JSON includes field names).

## WAN estimate tool

The repo includes a small helper tool that prints:

- payload sizes (bytes) and
- a deterministic WAN transfer estimate using: `t_transfer = rtt + bits/bandwidth`

Run it (example with JSON + zstd/gzip enabled):

- `cargo run --bin transport_pipeline_estimator --features "tools-json compression-zstd compression-gzip" -- --rows 100000 --rtt-ms 30 --mbit 100`

## Inventory generation

This repo maintains:
- component inventory: `src/docs/inventory.md`
- generated global inventory: `inventory.md`

Generate `inventory.md` from `src/docs/inventory.md`:

```bash
cargo make inventory
```

This runs `bin/generate_global_inventory.rs` in strict mode and fails if any `.rs` file is missing a 1-line purpose in `src/docs/inventory.md`.

## License

Licensed under either of:
- Apache License, Version 2.0 (`LICENSE-APACHE`)
- MIT license (`LICENSE-MIT`)
