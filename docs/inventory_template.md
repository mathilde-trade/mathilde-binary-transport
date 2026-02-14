# Inventory Template

This file documents the inventory format used by this repository.

Rules:
- An inventory is a factual listing of what exists in a component.
- No future plans (only implemented artifacts).
- Every `*.rs` file listed has a single-line purpose.
- Paths are repo-relative.

The global inventory generator reads file-purpose lines using this pattern:

- `path/to/file.rs`: 1-line purpose

