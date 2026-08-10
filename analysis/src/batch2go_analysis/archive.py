"""Loading and schema-checking batch2go event-record archives.

The archive is the analysis toolchain's only input: a run bundle must be
self-describing, so nothing here reaches for out-of-band context. The schema
below is an independent restatement of the Go event schema — the validator and
the analysis path never share code, because agreeing by construction would make
the agreement worthless as a check (CODEBASE.md §14).
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

SCHEMA_VERSION = 1

#: The 15 timestamps, in schema order (M2-PLAN §4.1). Every one is nullable:
#: absence is typed, and a stage a cell's topology does not have must read as
#: null rather than as the instant zero (ADR-0005).
STAGE_COLUMNS: tuple[str, ...] = (
    "t_sched",
    "t_client_send",
    "t_proxy_recv",
    "t_cohort_seal",
    "t_proxy_send",
    "t_adapter_recv",
    "t_adapter_dispatch",
    "t_queue_start",
    "t_compute_start",
    "t_compute_end",
    "t_adapter_result",
    "t_adapter_send",
    "t_proxy_resp_recv",
    "t_proxy_fanout",
    "t_client_recv",
)

#: Identity and accounting columns, with the polars dtype each must load as.
IDENTITY_COLUMNS: dict[str, pl.DataType] = {
    "schema_version": pl.UInt32,
    "experiment_id": pl.String,
    "session_id": pl.String,
    "run_id": pl.String,
    "cell": pl.String,
    "clock_domain_id": pl.String,
    "emitter": pl.String,
    "writer_id": pl.UInt32,
    "seq": pl.UInt64,
    "cohort_id": pl.UInt32,
    "ordinal": pl.UInt32,
    "envelope_id": pl.UInt64,
    "execution_id": pl.UInt64,
    "triton_request_id": pl.String,
    "presence_mask": pl.UInt32,
    "status": pl.String,
    "logical_bytes": pl.UInt32,
    "envelope_bytes": pl.UInt32,
    "batch_size": pl.UInt32,
    "membership_count": pl.UInt32,
}


class ArchiveSchemaError(Exception):
    """The archive does not match the event-record schema."""


def load(path: str | Path) -> pl.DataFrame:
    """Load an event-record archive."""
    return pl.read_parquet(Path(path))


def stage_presence(mask: int, stage: str) -> bool:
    """Whether ``stage`` is marked present in a record's presence mask."""
    return bool(mask & (1 << STAGE_COLUMNS.index(stage)))


def check_schema(df: pl.DataFrame) -> None:
    """Raise ArchiveSchemaError unless the frame matches the event schema.

    Checks column presence, dtypes, that every timestamp column is nullable
    Int64, and that the presence mask agrees with which timestamps are non-null —
    the last of these is what makes "absent by topology" and "missing timestamp"
    distinguishable downstream instead of both reading as null.
    """
    schema = df.schema

    missing = [c for c in (*IDENTITY_COLUMNS, *STAGE_COLUMNS) if c not in schema]
    if missing:
        raise ArchiveSchemaError(f"archive is missing columns: {missing}")

    for column, dtype in IDENTITY_COLUMNS.items():
        if schema[column] != dtype:
            raise ArchiveSchemaError(
                f"column {column!r} loaded as {schema[column]}, expected {dtype}"
            )

    for column in STAGE_COLUMNS:
        if schema[column] != pl.Int64:
            raise ArchiveSchemaError(
                f"timestamp column {column!r} loaded as {schema[column]}, "
                "expected nullable Int64 monotonic nanoseconds"
            )

    if schema["membership_uids"] != pl.List(pl.Int64):
        raise ArchiveSchemaError(
            f"membership_uids loaded as {schema['membership_uids']}, expected List(Int64)"
        )

    versions = set(df["schema_version"].unique().to_list())
    if versions != {SCHEMA_VERSION}:
        raise ArchiveSchemaError(
            f"archive carries schema versions {sorted(versions)}, expected {SCHEMA_VERSION}"
        )

    for index, stage in enumerate(STAGE_COLUMNS):
        claimed = (df["presence_mask"] & (1 << index)) != 0
        carried = df[stage].is_not_null()
        disagreements = int((claimed != carried).sum())
        if disagreements:
            raise ArchiveSchemaError(
                f"{disagreements} rows disagree about {stage}: the presence mask and "
                "the column must state the same thing, or absence is not typed"
            )


#: The traversal order of each cell's path, as (start, end, name) spans.
#:
#: Schema order is not traversal order, and pairing schema-order neighbours would
#: silently produce nonsense: t_cohort_seal is timestamp 4, but at A=off the load
#: generator emits it at barrier release — before the client sends — so the pair
#: (t_proxy_recv, t_cohort_seal) runs backwards, and (t_cohort_seal,
#: t_proxy_send) spans three unrelated hops. The chains below are an independent
#: restatement of the Go validator's, which is the point: two implementations
#: agreeing is evidence, one agreeing with itself is not.
_SHARED_TAIL: tuple[tuple[str, str, str], ...] = (
    ("t_proxy_send", "t_adapter_recv", "X_req_hop2"),
    ("t_adapter_recv", "t_adapter_dispatch", "adapter_unpack"),
    ("t_adapter_dispatch", "t_queue_start", "X_req_hop3"),
    ("t_queue_start", "t_compute_start", "Q_backend"),
    ("t_compute_start", "t_compute_end", "S_comp"),
    ("t_compute_end", "t_adapter_result", "X_resp_hop1"),
    ("t_adapter_result", "t_adapter_send", "response_pack"),
    ("t_adapter_send", "t_proxy_resp_recv", "X_resp_hop2"),
    ("t_proxy_resp_recv", "t_proxy_fanout", "F_fanout"),
    ("t_proxy_fanout", "t_client_recv", "X_resp_hop3"),
)

CHAINS: dict[str, tuple[tuple[str, str, str], ...]] = {
    # The direct path has one transport hop each way, so its transfer terms are
    # not hop-indexed.
    "D0": (
        ("t_sched", "t_cohort_seal", "barrier_wait"),
        ("t_cohort_seal", "t_client_send", "release_to_send"),
        ("t_client_send", "t_queue_start", "X_req"),
        ("t_queue_start", "t_compute_start", "Q_backend"),
        ("t_compute_start", "t_compute_end", "S_comp"),
        ("t_compute_end", "t_client_recv", "X_resp"),
    ),
    # A=off: the load generator seals at barrier release and the proxy emits none.
    **{
        cell: (
            ("t_sched", "t_cohort_seal", "barrier_wait"),
            ("t_cohort_seal", "t_client_send", "release_to_send"),
            ("t_client_send", "t_proxy_recv", "X_req_hop1"),
            ("t_proxy_recv", "t_proxy_send", "A_pack"),
        )
        + _SHARED_TAIL
        for cell in ("F00", "F01", "F00-seq")
    },
    # A=on: the proxy seals the envelope after receiving the cohort, so W_form —
    # the cycle model's formation term — is measured there. At A=off the
    # corresponding span is the load generator's own barrier wait, which is a
    # different quantity and carries a different name.
    **{
        cell: (
            ("t_sched", "t_client_send", "release_to_send"),
            ("t_client_send", "t_proxy_recv", "X_req_hop1"),
            ("t_proxy_recv", "t_cohort_seal", "W_form"),
            ("t_cohort_seal", "t_proxy_send", "A_pack"),
        )
        + _SHARED_TAIL
        for cell in ("F10", "F11-D", "F11-P")
    },
}


def stage_durations(df: pl.DataFrame, cell: str | None = None) -> pl.DataFrame:
    """Attach the cell's stage spans, leaving spans with an absent endpoint null.

    A duration is only computed where both endpoints exist, so nothing here can
    manufacture a number out of a missing timestamp. Rows are grouped by clock
    domain first: subtracting across domains would yield a plausible-looking
    duration from two unrelated numbers, since CLOCK_MONOTONIC restarts at boot.
    """
    if cell is None:
        cells = set(df["cell"].unique().to_list())
        if len(cells) != 1:
            raise ArchiveSchemaError(
                f"frame spans cells {sorted(cells)}; pass one explicitly to pick its chain"
            )
        cell = cells.pop()

    if cell not in CHAINS:
        raise ArchiveSchemaError(f"no stage chain for cell {cell!r}")

    domains = set(df["clock_domain_id"].unique().to_list())
    if len(domains) > 1:
        raise ArchiveSchemaError(
            f"frame spans clock domains {sorted(domains)}; their timestamps cannot be subtracted"
        )

    return df.with_columns(
        [
            (pl.col(end) - pl.col(start)).alias(f"d_{name}")
            for start, end, name in CHAINS[cell]
        ]
    )
