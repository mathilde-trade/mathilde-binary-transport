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
