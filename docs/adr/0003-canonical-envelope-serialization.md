---
status: accepted
date: 2026-08-10
---

# Canonical envelope serialization: single-copy preallocated protobuf bytes

Packing cost is a declared constituent of δ_A, so the serialization implementation is part of the treatment: copy count and streaming strategy change the measured effect (a 128 MiB envelope at P=8 MiB, B=16 differs by milliseconds between one-copy and multi-copy paths). The canonical implementation is pinned: protobuf `bytes` payload fields, preallocated reusable buffers, single copy on the marshal path. gRPC max message size is fixed at 256 MiB on every hop (including the Triton client) and recorded in the manifest, as are transport flow-control window settings.

Stage A includes one sensitivity configuration measuring a chunked/streaming variant at one (P, B) point, bounding the serialization-implementation share of δ_A — the same move as the tracing-overhead bound.
