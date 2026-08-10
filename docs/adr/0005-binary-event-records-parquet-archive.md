---
status: accepted
date: 2026-08-10
---

# Event records: binary hot path, Parquet archive

JSON event records (~4 KB each) would cost up to 450 GB for Stage B, and their emission cost lands on the hot path — where A=OFF cells emit ~B× more events than A=ON, a treatment-correlated overhead. Hot paths write fixed-shape length-delimited protobuf records into a per-process ring buffer with background flushing (no allocation, no syscall on the record path). Run finalization converts to Parquet + zstd, which is what S3 stores and what analysis reads (polars/duckdb); the raw stream is kept until the bundle validates.

Timestamps are `optional int64` monotonic nanoseconds plus a stage-presence bitmask: absence is typed, never zero. The validator asserts the presence mask expected for each cell topology (D0 has no proxy or adapter stages; A=OFF has no proxy seal). Expected confirmatory volume drops to ~20–40 GB, superseding the earlier 50–150 GB estimate.
