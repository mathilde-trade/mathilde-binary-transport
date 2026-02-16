# `mathilde-binary-transport` — Global Inventory (GENERATED; DO NOT EDIT)

Generated: 2026-02-15T18:39:51Z
Protocol: `docs/inventory_template.md`

This file is generated from per-component inventories under `*/src/docs/inventory.md`
If a file purpose is missing in a component inventory, this file will mark it as `INVENTORY GAP`.

## Components

- `crate::mathilde-binary-transport`: `src/docs/inventory.md`

---

## `mathilde-binary-transport`

### Artifacts

- Inventory: `src/docs/inventory.md`
- Evidence: `docs/evidence/bench_results.md`
- Benches: `benches/json_vs_mathldbt.rs`
- Benches: `benches/mathldbt_transport.rs`

### Source Files

- `benches/json_vs_mathldbt.rs`: Criterion benches comparing MATHLDBT encode/decode vs JSON serialize/deserialize.
- `benches/mathldbt_transport.rs`: Criterion transport benches (encode/decode; workspace reuse; optional encodings).
- `bin/generate_global_inventory.rs`: standalone global inventory generator (`rustc`-compiled; strict mode detects missing file purposes).
- `src/batch.rs`: in-memory batch model (`ColumnarBatch`, `ColumnData`, validity bitmap, invariant validation).
- `src/bin/transport_pipeline_estimator.rs`: small CLI to print byte sizes and a deterministic WAN transfer estimate for a fixed RTT/bandwidth model.
- `src/codec/exports.rs`: stable convenience entrypoints for common encode/decode operations.
- `src/codec/mathldbt_v1.rs`: `MATHLDBT` v1 encoder/decoder implementation (lossless; strict validation; opt-in DictUtf8 and DeltaVarintI64).
- `src/codec/mathldbt_v1_compressed.rs`: optional compression helpers (zstd/gzip feature-gated) that compress/decompress the v1 encoded bytes with bounded decompression.
- `src/codec/mod.rs`: codec module namespace.
- `src/error.rs`: crate error type (`Error`) and `Result<T>` alias.
- `src/lib.rs`: crate entrypoint (module exports + test module wiring).
- `src/schema.rs`: schema types (`ColumnarType`, `ColumnarField`, `ColumnarSchema`).
- `src/tests/mod.rs`: test module registry (crate-local tests live under `src/tests/`).
- `src/tests/test_batch.rs`: batch invariants unit tests (schema/columns lengths, fixed sizes, var offsets monotonicity).
- `src/tests/test_mathldbt_v1.rs`: `MATHLDBT` round-trip and determinism tests.
- `src/tests/test_mathldbt_v1_adversarial.rs`: adversarial decode tests (truncation/malformed payloads; deterministic errors).
- `src/tests/test_mathldbt_v1_compressed.rs`: tests for compressed helpers (round-trip, determinism, bounds enforcement, and feature-gate errors).
- `src/tests/test_mathldbt_v1_decode_into_equivalence.rs`: `decode_into` correctness vs allocating decode (plain + dict/delta).
- `src/tests/test_mathldbt_v1_decode_into_reuse_smoke.rs`: `decode_into` reuse smoke test (call twice on the same destination).

---
