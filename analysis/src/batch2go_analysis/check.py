"""Check that an event-record archive loads and matches the schema.

Usage:
    python -m batch2go_analysis.check <archive.parquet> [...]

Exits non-zero and names the defect if any archive fails. This is the analysis
toolchain's side of the "Parquet loads in polars with the right types and the
schema's column names" property, so it is run from the Go test suite as well as
by hand.
"""

from __future__ import annotations

import sys
from pathlib import Path

from batch2go_analysis.archive import (
    STAGE_COLUMNS,
    ArchiveSchemaError,
    check_schema,
    load,
)


def main(argv: list[str]) -> int:
    if not argv:
        print("usage: python -m batch2go_analysis.check <archive.parquet> [...]", file=sys.stderr)
        return 2

    for raw in argv:
        path = Path(raw)
        try:
            df = load(path)
            check_schema(df)
        except ArchiveSchemaError as exc:
            print(f"FAIL {path}: {exc}", file=sys.stderr)
            return 1
        except Exception as exc:  # unreadable file, wrong format, missing column
            print(f"FAIL {path}: could not load as an event archive: {exc}", file=sys.stderr)
            return 1

        carried = [c for c in STAGE_COLUMNS if df[c].is_not_null().any()]
        print(
            f"OK {path}: {df.height} rows, {df.width} columns, "
            f"{len(carried)}/{len(STAGE_COLUMNS)} timestamps carried"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
