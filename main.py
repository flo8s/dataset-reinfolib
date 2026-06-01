"""不動産情報ライブラリ API 取得 + dbt build + snapshot pipeline.

Snapshot must run in the SAME Python process as dbt build — see
dataset-shared/README.md for the constraint detail.
"""

from __future__ import annotations

import importlib.util
import logging
import os
import sys
from collections.abc import Generator
from contextlib import contextmanager
from datetime import date
from itertools import product
from pathlib import Path

import duckdb
import pyarrow as pa
from dbt.cli.main import dbtRunner
from reinfolib import ReinfolibClient

SHARED_SCRIPTS = Path(__file__).resolve().parent / "shared" / "scripts"
sys.path.insert(0, str(SHARED_SCRIPTS))
from queria_config import load_target  # noqa: E402

_spec = importlib.util.spec_from_file_location(
    "snapshot_to_r2", SHARED_SCRIPTS / "snapshot-to-r2.py"
)
assert _spec and _spec.loader
snapshot_to_r2 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(snapshot_to_r2)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

type Year = int
type Quarter = int
type YearQuarter = tuple[Year, Quarter]

TABLE = "reinfolib._source.trade_prices"
PRICE_CLASSIFICATION = "01"
START: YearQuarter = (2005, 3)


@contextmanager
def _ducklake_connect(target_name: str) -> Generator[duckdb.DuckDBPyConnection]:
    """Open a fresh DuckDB session with the dataset's Neon DuckLake attached."""
    target = load_target(target_name)
    conn = duckdb.connect(":memory:")
    try:
        conn.execute("INSTALL ducklake; LOAD ducklake;")
        conn.execute("INSTALL postgres; LOAD postgres;")
        conn.execute("INSTALL httpfs; LOAD httpfs;")
        conn.execute(
            "CREATE SECRET r2 (TYPE r2, KEY_ID ?, SECRET ?, ACCOUNT_ID ?)",
            [target.s3_access_key_id, target.s3_secret_access_key, target.cf_account_id],
        )
        conn.execute(
            f"ATTACH '{target.ducklake_uri}' AS \"{target.dataset}\" "
            f"(DATA_PATH '{target.data_path}', META_SCHEMA '{target.meta_schema}')"
        )
        yield conn
    finally:
        conn.close()


def main() -> None:
    target = os.environ.get("DBT_TARGET", sys.argv[1] if len(sys.argv) > 1 else "default")

    api_key = os.environ["REINFOLIB_API_KEY"]
    areas = [f"{a:02d}" for a in range(1, 48)]
    all_quarters = _generate_quarters(START)
    logger.info("start: %d areas × %d quarters", len(areas), len(all_quarters))

    with _ducklake_connect(target) as conn, ReinfolibClient(api_key) as client:
        conn.execute("CREATE SCHEMA IF NOT EXISTS reinfolib._source")
        ingest_trade_prices(conn, client, areas=areas, quarters=all_quarters)

    dbt = dbtRunner()
    for cmd in (
        ["deps"],
        ["run", "--target", target],
        ["docs", "generate", "--target", target],
    ):
        result = dbt.invoke(cmd)
        if not result.success:
            raise SystemExit(f"dbt {' '.join(cmd)} failed")

    snapshot_to_r2.run(target)


def ingest_trade_prices(
    conn: duckdb.DuckDBPyConnection,
    client: ReinfolibClient,
    *,
    areas: list[str],
    quarters: list[YearQuarter],
) -> None:
    """XIT001: 取引価格・成約価格を取得。"""
    current = quarters[-1]
    completed = _completed_pairs(conn)
    total = len(areas) * len(quarters)
    logger.info("completed: %d / %d pairs", len(completed), total)

    fetched = 0
    for area, (year, quarter) in product(areas, quarters):
        if (area, year, quarter) in completed and (year, quarter) != current:
            continue

        rows = client.get_real_estate_prices(
            year=year,
            quarter=quarter,
            area=area,
            price_classification=PRICE_CLASSIFICATION,
        )
        if not rows:
            logger.info("XIT001 area=%s %dQ%d: empty", area, year, quarter)
            continue
        fetched += 1

        for row in rows:
            row["_area_code"] = area
            row["_year"] = year
            row["_quarter"] = quarter

        conn.register("_batch", pa.Table.from_pylist(rows))
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {TABLE} AS SELECT * FROM _batch WITH NO DATA"
        )
        conn.execute("BEGIN")
        conn.execute(
            f"DELETE FROM {TABLE} WHERE _area_code = ? AND _year = ? AND _quarter = ?",
            [area, year, quarter],
        )
        conn.execute(f"INSERT INTO {TABLE} SELECT * FROM _batch")
        conn.execute("COMMIT")
        conn.unregister("_batch")

        logger.info("XIT001 area=%s %dQ%d: %d rows", area, year, quarter, len(rows))

    logger.info("ingest done: %d partitions fetched", fetched)


def _completed_pairs(
    conn: duckdb.DuckDBPyConnection,
) -> set[tuple[str, int, int]]:
    try:
        return {
            (row[0], row[1], row[2])
            for row in conn.execute(
                f"SELECT DISTINCT _area_code, _year, _quarter FROM {TABLE}"
            ).fetchall()
        }
    except duckdb.CatalogException:
        return set()


def _generate_quarters(
    start: YearQuarter,
    end: YearQuarter | None = None,
) -> list[YearQuarter]:
    if end is None:
        today = date.today()
        end = (today.year, (today.month - 1) // 3 + 1)
    return [
        (y, q)
        for y in range(start[0], end[0] + 1)
        for q in range(1, 5)
        if start <= (y, q) <= end
    ]


if __name__ == "__main__":
    main()
