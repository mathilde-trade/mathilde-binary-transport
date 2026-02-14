# `mathilde-binary-transport` — Inventory (DRAFT)

Protocol: `docs/inventory_template.md`

This inventory lists what currently exists in this crate. It describes only implemented code and artifacts.

---

## 0) Module documentation (artifacts)

- Inventory (this file): `docs/inventory.md`
- README (public usage and contracts): `README.md`
- Evidence logs: `docs/evidence/`

## 1) Source Files

- `src/lib.rs`: crate entrypoint (module exports + test module wiring).
- `src/error.rs`: crate error type (`Error`) and `Result<T>` alias.
- `src/schema.rs`: schema types (`ColumnarType`, `ColumnarField`, `ColumnarSchema`).
- `src/batch.rs`: in-memory batch model (`ColumnarBatch`, `ColumnData`, validity bitmap, invariant validation).

- `src/codec/mod.rs`: codec module namespace.
- `src/codec/mathldbt_v1.rs`: `MATHLDBT` v1 encoder/decoder implementation (lossless; strict validation; opt-in DictUtf8 and DeltaVarintI64).
- `src/codec/mathldbt_v1_compressed.rs`: optional compression helpers (zstd/gzip feature-gated) that compress/decompress the v1 encoded bytes with bounded decompression.

- `src/tests/mod.rs`: test module registry (crate-local tests live under `src/tests/`).
- `src/tests/test_batch.rs`: batch invariants unit tests (schema/columns lengths, fixed sizes, var offsets monotonicity).
- `src/tests/test_mathldbt_v1.rs`: `MATHLDBT` round-trip and determinism tests.
- `src/tests/test_mathldbt_v1_adversarial.rs`: adversarial decode tests (truncation/malformed payloads; deterministic errors).
- `src/tests/test_mathldbt_v1_compressed.rs`: tests for compressed helpers (round-trip, determinism, bounds enforcement, and feature-gate errors).

- `bin/generate_global_inventory.rs`: standalone global inventory generator (`rustc`-compiled; strict mode detects missing file purposes).

- `benches/mathldbt_transport.rs`: Criterion transport benches (encode/decode; workspace reuse; optional encodings).
- `benches/json_vs_mathldbt.rs`: Criterion benches comparing MATHLDBT encode/decode vs JSON serialize/deserialize.

- `src/bin/transport_pipeline_estimator.rs`: small CLI to print byte sizes and a deterministic WAN transfer estimate for a fixed RTT/bandwidth model.

## 2) Public API Surface

The public API is defined by modules exported from:

- `src/lib.rs`

Primary entrypoints:
- `mathilde_binary_transport::codec::mathldbt_v1::{encode_mathldbt_v1_into, decode_mathldbt_v1, decode_mathldbt_v1_into}`

## 3) Workspace / `*_into` APIs

Workspace types exist for repeated calls with deterministic buffer reuse:

- `mathilde_binary_transport::codec::mathldbt_v1::MathldbtV1EncodeWorkspace`
- `mathilde_binary_transport::codec::mathldbt_v1::MathldbtV1DecodeWorkspace`

## 4) Determinism policy

Determinism requirements are specified in:

- `src/codec/mathldbt_v1.rs` and the tests under `src/tests/`

## 5) Benchmarks (harness files)

- `benches/mathldbt_transport.rs`
