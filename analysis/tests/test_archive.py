"""Schema checks for event-record archives.

These exercise the analysis side's independent restatement of the event schema.
Independence is the point: the validator and the analysis path never share code,
because two implementations agreeing means something and one implementation
agreeing with itself does not (CODEBASE.md §14).
"""

from __future__ import annotations

import polars as pl
import pytest

from batch2go_analysis.archive import (
    CHAINS,
    DISPATCH_COLUMNS,
    IDENTITY_COLUMNS,
    SCHEMA_VERSION,
    STAGE_COLUMNS,
    ArchiveSchemaError,
    check_schema,
    stage_durations,
    stage_presence,
)


def mask_for(*stages: str) -> int:
    return sum(1 << STAGE_COLUMNS.index(s) for s in stages)


#: The stages D0 traverses. Its proxy and adapter columns are absent by
#: topology — a different fact from a timestamp going missing (ADR-0005).
D0_STAGES = (
    "t_sched",
    "t_client_send",
    "t_cohort_seal",
    "t_queue_start",
    "t_compute_start",
    "t_compute_end",
    "t_client_recv",
)


def d0_frame(rows: int = 4) -> pl.DataFrame:
    data: dict[str, list] = {}
    for column, dtype in IDENTITY_COLUMNS.items():
        if dtype == pl.String:
            data[column] = ["x"] * rows
        else:
            data[column] = [1] * rows

    data["schema_version"] = [SCHEMA_VERSION] * rows
    data["cell"] = ["D0"] * rows
    data["emitter"] = ["loadgen"] * rows
    data["status"] = ["ok"] * rows
    data["presence_mask"] = [mask_for(*D0_STAGES)] * rows
    data["ordinal"] = list(range(rows))

    # Timestamps are laid out in traversal order, not schema order. Building them
    # by schema index would make t_cohort_seal land after t_client_send, which is
    # precisely the confusion these tests exist to rule out.
    columns: dict[str, list[int | None]] = {c: [] for c in STAGE_COLUMNS}
    for row in range(rows):
        cursor = 1_000_000 + row
        instants = {CHAINS["D0"][0][0]: cursor}
        for _, end, _ in CHAINS["D0"]:
            cursor += 1_000
            instants[end] = cursor
        for column in STAGE_COLUMNS:
            columns[column].append(instants.get(column))
    data.update(columns)

    frame = pl.DataFrame(data).with_columns(
        [pl.col(c).cast(dtype) for c, dtype in IDENTITY_COLUMNS.items()]
        + [pl.col(c).cast(pl.Int64) for c in STAGE_COLUMNS]
    )
    frame = frame.with_columns(
        pl.Series("membership_uids", [[1] for _ in range(rows)], dtype=pl.List(pl.Int64))
    )
    # D0's load-generator records observed no dispatch, so the fan-out evidence
    # is null throughout — the shape an adapter-less path actually produces.
    return frame.with_columns(
        [pl.lit(None).cast(dtype).alias(c) for c, dtype in DISPATCH_COLUMNS.items()]
    )


def with_dispatch(
    frame: pl.DataFrame, *, dispatched: int, skew: int, cpu: int, scope: str
) -> pl.DataFrame:
    """Give every row the same fan-out evidence, as an adapter's records carry it."""
    return frame.with_columns(
        pl.lit(dispatched).cast(pl.UInt32).alias("dispatched"),
        pl.lit(skew).cast(pl.Int64).alias("dispatch_skew_nanos"),
        pl.lit(cpu).cast(pl.Int64).alias("adapter_cpu_nanos"),
        pl.lit(scope).cast(pl.String).alias("adapter_cpu_scope"),
    )


def test_well_formed_archive_passes():
    check_schema(d0_frame())


def test_missing_column_is_reported():
    frame = d0_frame().drop("t_compute_end")
    with pytest.raises(ArchiveSchemaError, match="missing columns"):
        check_schema(frame)


def test_timestamp_column_must_be_nullable_int64():
    frame = d0_frame().with_columns(pl.col("t_sched").cast(pl.Float64))
    with pytest.raises(ArchiveSchemaError, match="t_sched"):
        check_schema(frame)


def test_presence_mask_must_agree_with_the_columns():
    """A mask that claims a stage the column does not carry breaks typed absence.

    If the two could disagree, "absent by topology" and "missing timestamp" would
    both read as null and the distinction would be unrecoverable downstream.
    """
    frame = d0_frame().with_columns(
        pl.lit(mask_for(*D0_STAGES, "t_proxy_recv")).cast(pl.UInt32).alias("presence_mask")
    )
    with pytest.raises(ArchiveSchemaError, match="t_proxy_recv"):
        check_schema(frame)


def test_absent_by_topology_stays_null():
    frame = d0_frame()
    for column in ("t_proxy_recv", "t_proxy_send", "t_adapter_recv", "t_adapter_dispatch"):
        assert frame[column].is_null().all(), f"{column} should be absent for D0"
        assert not stage_presence(int(frame["presence_mask"][0]), column)


def test_dispatch_evidence_columns_are_known_to_the_schema():
    """A one-member dispatch measured a skew of zero, and that must load as zero.

    A never-measured skew is null. If the check did not know these columns, an
    archive could carry a fan-out claim in a column nothing validates.
    """
    frame = with_dispatch(d0_frame(), dispatched=1, skew=0, cpu=840_000, scope="process")
    check_schema(frame)

    assert (frame["dispatch_skew_nanos"] == 0).all()
    assert frame["dispatch_skew_nanos"].is_not_null().all()
    assert (frame["adapter_cpu_scope"] == "process").all()


def test_partial_dispatch_evidence_is_reported():
    """The evidence describes one fan-out, so it arrives whole or not at all.

    A skew with no scope beside it would be a CPU number with no definition —
    the exact thing the scope exists to prevent.
    """
    frame = with_dispatch(
        d0_frame(), dispatched=1, skew=0, cpu=840_000, scope="process"
    ).with_columns(pl.lit(None).cast(pl.String).alias("adapter_cpu_scope"))
    with pytest.raises(ArchiveSchemaError, match="adapter_cpu_scope"):
        check_schema(frame)


def test_unmeasured_dispatch_stays_null():
    frame = d0_frame()
    for column in DISPATCH_COLUMNS:
        assert frame[column].is_null().all(), f"{column} should be absent without an adapter"
    check_schema(frame)


def test_unexpected_schema_version_is_reported():
    frame = d0_frame().with_columns(pl.lit(99).cast(pl.UInt32).alias("schema_version"))
    with pytest.raises(ArchiveSchemaError, match="schema versions"):
        check_schema(frame)


def test_stage_durations_follow_traversal_order_not_schema_order():
    """Spans come from the cell's chain, and every one runs forwards.

    Schema order is not traversal order: t_cohort_seal is timestamp 4 but at
    A=off it is emitted before the client sends, so pairing schema-order
    neighbours would produce negative spans and conflate unrelated hops.
    """
    frame = stage_durations(d0_frame())

    for _, _, name in CHAINS["D0"]:
        column = f"d_{name}"
        assert column in frame.columns, f"missing span {name}"
        assert (frame[column] >= 0).all(), f"{name} runs backwards"

    # D0 has no proxy or adapter stages, so no span mentions them.
    assert "d_A_pack" not in frame.columns
    assert "d_X_req_hop2" not in frame.columns


def test_stage_durations_leave_absent_endpoints_null():
    """A duration is only computed where both endpoints exist.

    Reading a missing timestamp as the instant zero would manufacture a stage
    duration out of nothing, which is exactly what typed absence exists to stop.
    """
    frame = d0_frame().with_columns(pl.lit(None, dtype=pl.Int64).alias("t_compute_end"))
    frame = frame.with_columns(
        (pl.col("presence_mask") & ~pl.lit(mask_for("t_compute_end"))).cast(pl.UInt32).alias("presence_mask")
    )
    spans = stage_durations(frame)

    assert spans["d_S_comp"].is_null().all(), "a span with an absent endpoint must stay null"
    assert spans["d_X_resp"].is_null().all()
    assert spans["d_Q_backend"].is_not_null().all(), "spans with both endpoints must still compute"


def test_stage_durations_refuse_to_subtract_across_clock_domains():
    frame = d0_frame(rows=4).with_columns(
        pl.Series("clock_domain_id", ["cd-a", "cd-a", "cd-b", "cd-b"])
    )
    with pytest.raises(ArchiveSchemaError, match="clock domains"):
        stage_durations(frame)


def test_every_cell_chain_covers_only_columns_of_the_schema():
    for cell, chain in CHAINS.items():
        for start, end, name in chain:
            assert start in STAGE_COLUMNS, f"{cell}/{name}: unknown start {start}"
            assert end in STAGE_COLUMNS, f"{cell}/{name}: unknown end {end}"


def test_stage_columns_are_the_fifteen_of_the_schema():
    assert len(STAGE_COLUMNS) == 15
    assert STAGE_COLUMNS[0] == "t_sched"
    assert STAGE_COLUMNS[-1] == "t_client_recv"
