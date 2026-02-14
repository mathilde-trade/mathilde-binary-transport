# `mathilde-binary-transport` — Global Inventory (GENERATED; DO NOT EDIT)

Generated: 2026-02-14T17:42:15Z
Protocol: `docs/inventory_template.md`

This file is generated from per-component inventories under `*/docs/inventory.md` (workspace crates) and optionally `crates/*/docs/inventory.md` / `services/*/docs/inventory.md`.
If a component does not have a top-level `docs/inventory.md`, this generator may also include module inventories under `<component>/src/*/docs/inventory.md` when present.
If a file purpose is missing in a component inventory, this file will mark it as `INVENTORY GAP`.

## Components

- `crate::mathilde-binary-transport`: `docs/inventory.md`

---

## `mathilde-binary-transport`

### Artifacts

- Inventory: `docs/inventory.md`
- Evidence: `docs/evidence/bench_results.md`
- Benches: `benches/mathldbt_transport.rs`

### Source Files

- `benches/mathldbt_transport.rs`: Criterion transport benches (encode/decode; workspace reuse; optional encodings).
- `bin/generate_global_inventory.rs`: standalone global inventory generator (`rustc`-compiled; strict mode detects missing file purposes).
- `src/batch.rs`: in-memory batch model (`ColumnarBatch`, `ColumnData`, validity bitmap, invariant validation).
- `src/codec/mathldbt_v1.rs`: `MATHLDBT` v1 encoder/decoder implementation (lossless; strict validation; opt-in DictUtf8 and DeltaVarintI64).
- `src/codec/mod.rs`: codec module namespace.
- `src/error.rs`: crate error type (`Error`) and `Result<T>` alias.
- `src/lib.rs`: crate entrypoint (module exports + test module wiring).
- `src/schema.rs`: schema types (`ColumnarType`, `ColumnarField`, `ColumnarSchema`).
- `src/tests/mod.rs`: test module registry (crate-local tests live under `src/tests/`).
- `src/tests/test_batch.rs`: batch invariants unit tests (schema/columns lengths, fixed sizes, var offsets monotonicity).
- `src/tests/test_mathldbt_v1.rs`: `MATHLDBT` round-trip and determinism tests.
- `src/tests/test_mathldbt_v1_adversarial.rs`: adversarial decode tests (truncation/malformed payloads; deterministic errors).

---
