# MBT — Benchmark Results (APPEND-ONLY)

Rules:

- Append-only; never edit old entries.
- Each entry includes exact command(s), machine info, Rust toolchain, and results.
- Any performance claim must reference one of these entries.

---

## TEMPLATE ENTRY

Date (UTC):
Operator:

Machine:

- CPU:
- OS:
- Rust:

Command(s):

- `...`

Profile:

- release/debug:

Results:

- ...

---

## 2026-02-14 — `mathldbt_transport` (criterion, local)

Date (UTC): 2026-02-14T17:26:18Z  
Operator: codex-cli

Machine:

- CPU: Intel(R) Xeon(R) W-2295 CPU @ 3.00GHz (18c/36t)
- OS: Linux 5.15.0-156-generic x86_64 GNU/Linux
- Rust: rustc 1.90.0, cargo 1.90.0

Command(s):

- `cargo bench --bench mathldbt_transport`

Profile:

- release (cargo bench; criterion)

Notes:

- `gnuplot` not found; criterion used plotters backend (this does not affect measured timings).

Results (median; lower is better):

- rows=2,000:
  - `encode_plain_ws`: ~14.99 µs
  - `encode_dict_delta_ws`: ~104.49 µs
  - `decode_plain_ws`: ~59.28 µs
  - `decode_dict_delta_ws`: ~74.37 µs

- rows=100,000:
  - `encode_plain_ws`: ~807.76 µs
  - `encode_dict_delta_ws`: ~5.28 ms
  - `decode_plain_ws`: ~3.02 ms
  - `decode_dict_delta_ws`: ~3.76 ms

---

## 2026-02-14 — `mathldbt_transport` (+ zstd/gzip, criterion, local)

Date (UTC): 2026-02-14T18:37:25Z  
Operator: codex-cli

Machine:

- CPU: Intel(R) Xeon(R) W-2295 CPU @ 3.00GHz (18c/36t)
- OS: Linux 5.15.0-156-generic x86_64 GNU/Linux
- Rust: rustc 1.90.0, cargo 1.90.0

Command(s):

- `cargo bench --bench mathldbt_transport --features "compression-zstd compression-gzip"`

Profile:

- release (cargo bench; criterion)

Notes:

- `gnuplot` not found; criterion used plotters backend (this does not affect measured timings).
- Criterion warned that `encode_gzip_ws/100000` needed a longer target time to collect 100 samples.

Results (median; lower is better):

- rows=2,000:
  - `encode_plain_ws`: ~13.946 µs
  - `encode_dict_delta_ws`: ~100.17 µs
  - `decode_plain_ws`: ~57.365 µs
  - `decode_dict_delta_ws`: ~74.281 µs
  - `encode_zstd_ws` (level=3): ~324.16 µs
  - `decode_zstd_ws` (level=3): ~170.57 µs
  - `encode_gzip_ws` (level=6): ~2.9216 ms
  - `decode_gzip_ws` (level=6): ~216.41 µs

- rows=100,000:
  - `encode_plain_ws`: ~787.96 µs
  - `encode_dict_delta_ws`: ~5.0507 ms
  - `decode_plain_ws`: ~2.8703 ms
  - `decode_dict_delta_ws`: ~3.6479 ms
  - `encode_zstd_ws` (level=3): ~16.896 ms
  - `decode_zstd_ws` (level=3): ~8.5272 ms
  - `encode_gzip_ws` (level=6): ~173.23 ms
  - `decode_gzip_ws` (level=6): ~10.970 ms

---

## 2026-02-14 — `json_vs_mathldbt` (criterion, local)

Date (UTC): 2026-02-14T18:37:25Z  
Operator: codex-cli

Machine:

- CPU: Intel(R) Xeon(R) W-2295 CPU @ 3.00GHz (18c/36t)
- OS: Linux 5.15.0-156-generic x86_64 GNU/Linux
- Rust: rustc 1.90.0, cargo 1.90.0

Command(s):

- `cargo bench --bench json_vs_mathldbt`

Profile:

- release (cargo bench; criterion)

Notes:

- JSON baseline is `serde_json` encoding of a row-struct vector; JSON includes field names.
- `gnuplot` not found; criterion used plotters backend (this does not affect measured timings).

Results (median; lower is better):

- rows=2,000:
  - `mathldbt_encode_ws`: ~100.51 µs
  - `mathldbt_decode_ws`: ~74.020 µs
  - `json_serialize`: ~482.74 µs
  - `json_deserialize`: ~796.47 µs

- rows=100,000:
  - `mathldbt_encode_ws`: ~5.0754 ms
  - `mathldbt_decode_ws`: ~3.6473 ms
  - `json_serialize`: ~24.279 ms
  - `json_deserialize`: ~41.114 ms

---

## 2026-02-14 — `transport_pipeline_estimator` (bytes + deterministic WAN estimate, local)

Date (UTC): 2026-02-14T18:37:25Z  
Operator: codex-cli

Machine:

- CPU: Intel(R) Xeon(R) W-2295 CPU @ 3.00GHz (18c/36t)
- OS: Linux 5.15.0-156-generic x86_64 GNU/Linux
- Rust: rustc 1.90.0, cargo 1.90.0

Command(s):

- `cargo run --bin transport_pipeline_estimator --features "tools-json compression-zstd compression-gzip" -- --rows 2000 --rtt-ms 30 --mbit 100`
- `cargo run --bin transport_pipeline_estimator --features "tools-json compression-zstd compression-gzip" -- --rows 100000 --rtt-ms 30 --mbit 100`

Profile:

- debug (cargo run)

Notes:

- WAN estimate model: `t_transfer = rtt_ms/1000 + (bytes*8)/(mbit_per_s*1e6)`.
- Local encode/decode timings in this tool are debug-profile measurements from one run; use criterion for stable CPU comparisons.

Results (bytes + WAN estimate only):

### rows = 2,000 (rtt=30ms, bandwidth=100Mbit/s)

| Payload  |    Compression |   Bytes | Estimated t_transfer |
| -------- | -------------: | ------: | -------------------: |
| MATHLDBT |           none |  92,263 |            37.381 ms |
| JSON     |           none | 254,001 |            50.320 ms |
| MATHLDBT | zstd (level=3) |   8,120 |            30.650 ms |
| MATHLDBT | gzip (level=6) |  10,769 |            30.862 ms |

### rows = 100,000 (rtt=30ms, bandwidth=100Mbit/s)

| Payload  |    Compression |      Bytes | Estimated t_transfer |
| -------- | -------------: | ---------: | -------------------: |
| MATHLDBT |           none |  4,600,263 |           398.021 ms |
| JSON     |           none | 12,700,001 |           1.046000 s |
| MATHLDBT | zstd (level=3) |    293,862 |            53.509 ms |
| MATHLDBT | gzip (level=6) |    457,652 |            66.612 ms |

---

## 2026-02-14 — Wire-size apples-to-apples (JSON vs MATHLDBT; gzip/zstd, local)

Date (UTC): 2026-02-14T18:47:01Z  
Operator: codex-cli

Machine:

- CPU: Intel(R) Xeon(R) W-2295 CPU @ 3.00GHz (18c/36t)
- OS: Linux 5.15.0-156-generic x86_64 GNU/Linux
- Rust: rustc 1.90.0, cargo 1.90.0

Command(s):

- `cargo run --bin transport_pipeline_estimator --features "tools-json compression-zstd compression-gzip" -- --rows 2000 --rtt-ms 30 --mbit 100`
- `cargo run --bin transport_pipeline_estimator --features "tools-json compression-zstd compression-gzip" -- --rows 100000 --rtt-ms 30 --mbit 100`

WAN model:

- `t_transfer = rtt_ms/1000 + (bytes*8)/(mbit_per_s*1e6)`
- `rtt_ms=30`, `mbit_per_s=100`

Notes:

- This entry compares wire bytes for JSON and MATHLDBT under the same compressors.
- Compression settings: zstd level=3, gzip level=6.
- Compression is lossless; JSON compressed sizes are validated by decompress+deserialize.

### rows = 2,000

| Payload  |    Compression |   Bytes | Estimated t_transfer |
| -------- | -------------: | ------: | -------------------: |
| MATHLDBT |           none |  92,263 |            37.381 ms |
| JSON     |           none | 254,001 |            50.320 ms |
| MATHLDBT | zstd (level=3) |   8,120 |            30.650 ms |
| JSON     | zstd (level=3) |  10,989 |            30.879 ms |
| MATHLDBT | gzip (level=6) |  10,769 |            30.862 ms |
| JSON     | gzip (level=6) |  26,757 |            32.141 ms |

### rows = 100,000

| Payload  |    Compression |      Bytes | Estimated t_transfer |
| -------- | -------------: | ---------: | -------------------: |
| MATHLDBT |           none |  4,600,263 |           398.021 ms |
| JSON     |           none | 12,700,001 |           1.046000 s |
| MATHLDBT | zstd (level=3) |    293,862 |            53.509 ms |
| JSON     | zstd (level=3) |    582,206 |            76.576 ms |
| MATHLDBT | gzip (level=6) |    457,652 |            66.612 ms |
| JSON     | gzip (level=6) |  1,325,434 |           136.035 ms |

---

## 2026-02-15 — Codec parity work (criterion, local; decode-into + bulk-copy)

Date (UTC): 2026-02-15T18:15:00Z  
Operator: codex-cli

Machine:

- CPU: Intel(R) Xeon(R) W-2295 CPU @ 3.00GHz (18c/36t)
- OS: Linux 5.15.0-156-generic x86_64 GNU/Linux
- Rust: rustc 1.90.0, cargo 1.90.0

Command(s):

- `cargo bench --bench mathldbt_transport`
- `cargo bench --bench json_vs_mathldbt`

Profile:

- release (cargo bench; criterion)

Notes:

- This entry is after implementing a true `decode_into_with_workspace` + ORSX-style `unsafe` bulk-copy fast paths on little-endian.
- `gnuplot` not found; criterion used plotters backend (this does not affect measured timings).

### `mathldbt_transport` (median; lower is better)

#### rows = 2,000

| Benchmark                   |    Median |
| --------------------------- | --------: |
| `encode_plain_ws`           | 5.0073 µs |
| `encode_dict_delta_ws`      | 98.677 µs |
| `decode_plain_ws`           | 9.2821 µs |
| `decode_into_plain_ws`      | 8.4636 µs |
| `decode_dict_delta_ws`      | 44.816 µs |
| `decode_into_dict_delta_ws` | 42.881 µs |

#### rows = 100,000

| Benchmark                   |    Median |
| --------------------------- | --------: |
| `encode_plain_ws`           | 554.60 µs |
| `encode_dict_delta_ws`      | 5.1085 ms |
| `decode_plain_ws`           | 851.11 µs |
| `decode_into_plain_ws`      | 798.08 µs |
| `decode_dict_delta_ws`      | 2.4433 ms |
| `decode_into_dict_delta_ws` | 2.4130 ms |

### `json_vs_mathldbt` (median; lower is better)

#### rows = 2,000

| Benchmark            |    Median |
| -------------------- | --------: |
| `mathldbt_encode_ws` | 102.31 µs |
| `mathldbt_decode_ws` | 49.308 µs |
| `json_serialize`     | 530.42 µs |
| `json_deserialize`   | 827.92 µs |

#### rows = 100,000

| Benchmark            |    Median |
| -------------------- | --------: |
| `mathldbt_encode_ws` | 5.2750 ms |
| `mathldbt_decode_ws` | 2.6227 ms |
| `json_serialize`     | 26.241 ms |
| `json_deserialize`   | 41.411 ms |

---

## 2026-02-15 — `mathldbt_transport` (+ zstd/gzip, criterion, local; post-parity)

Date (UTC): 2026-02-15T18:45:00Z  
Operator: codex-cli

Machine:

- CPU: Intel(R) Xeon(R) W-2295 CPU @ 3.00GHz (18c/36t)
- OS: Linux 5.15.0-156-generic x86_64 GNU/Linux
- Rust: rustc 1.90.0, cargo 1.90.0

Command(s):

- `cargo bench --bench mathldbt_transport --features "compression-zstd compression-gzip"`

Profile:

- release (cargo bench; criterion)

Notes:

- This run includes the compression helpers and therefore measures: `encode_v1` + compress, and decompress + `decode_v1`.
- `gnuplot` not found; criterion used plotters backend (this does not affect measured timings).

### rows = 2,000 (median; lower is better)

| Benchmark | Median |
|---|---:|
| `encode_plain_ws` | 4.596 µs |
| `encode_dict_delta_ws` | 99.222 µs |
| `decode_plain_ws` | 8.927 µs |
| `decode_into_plain_ws` | 8.066 µs |
| `decode_dict_delta_ws` | 46.202 µs |
| `decode_into_dict_delta_ws` | 43.933 µs |
| `encode_zstd_ws` (level=3) | 332.401 µs |
| `decode_zstd_ws` (level=3) | 148.749 µs |
| `encode_gzip_ws` (level=6) | 3.075 ms |
| `decode_gzip_ws` (level=6) | 185.618 µs |

### rows = 100,000 (median; lower is better)

| Benchmark | Median |
|---|---:|
| `encode_plain_ws` | 537.506 µs |
| `encode_dict_delta_ws` | 5.155 ms |
| `decode_plain_ws` | 855.788 µs |
| `decode_into_plain_ws` | 763.867 µs |
| `decode_dict_delta_ws` | 2.481 ms |
| `decode_into_dict_delta_ws` | 2.427 ms |
| `encode_zstd_ws` (level=3) | 17.492 ms |
| `decode_zstd_ws` (level=3) | 7.747 ms |
| `encode_gzip_ws` (level=6) | 181.339 ms |
| `decode_gzip_ws` (level=6) | 9.625 ms |
