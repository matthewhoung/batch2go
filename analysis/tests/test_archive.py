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
    return frame.with_columns(
        pl.Series("membership_uids", [[1] for _ in range(rows)], dtype=pl.List(pl.Int64))
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
