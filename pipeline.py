"""不動産情報ライブラリ API 取得 + dbt ビルドパイプライン。"""

import logging
import os
from datetime import date
from itertools import product

import duckdb
import pyarrow as pa
from dbt.cli.main import dbtRunner
from reinfolib import ReinfolibClient

from fdl.ducklake import connect

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

type Year = int
type Quarter = int
type YearQuarter = tuple[Year, Quarter]

TABLE = "reinfolib._source.trade_prices"
PRICE_CLASSIFICATION = "01"
START: YearQuarter = (2005, 3)


def main():
    api_key = os.environ["REINFOLIB_API_KEY"]
    areas = [f"{a:02d}" for a in range(1, 48)]
    all_quarters = _generate_quarters(START)
    logger.info("start: %d areas × %d quarters", len(areas), len(all_quarters))

    with connect() as conn, ReinfolibClient(api_key) as client:
        conn.execute("CREATE SCHEMA IF NOT EXISTS reinfolib._source")
        ingest_trade_prices(conn, client, areas=areas, quarters=all_quarters)

    logger.info("dbt deps")
    result = dbtRunner().invoke(["deps"])
    if not result.success:
        raise SystemExit("dbt deps failed")

    logger.info("dbt run")
    result = dbtRunner().invoke(["run"])
    if not result.success:
        raise SystemExit("dbt run failed")

    logger.info("dbt docs generate")
    result = dbtRunner().invoke(["docs", "generate"])
    if not result.success:
        raise SystemExit("dbt docs generate failed")


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
        # すでに取得済みの (area, year, quarter) はスキップ。ただし最新の四半期は再取得して更新する
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

        # DELETE-INSERT の冪等性キーとしてリクエストパラメータを付与
        for row in rows:
            row["_area_code"] = area
            row["_year"] = year
            row["_quarter"] = quarter

        conn.register("_batch", pa.Table.from_pylist(rows))
        # Arrow スキーマからテーブルを初回作成（データは入れない）
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {TABLE} AS SELECT * FROM _batch WITH NO DATA"
        )
        # 同一パーティションの既存データを削除して再挿入（冪等性を保証）
        # トランザクションで囲み、中断時にDELETEだけ残らないようにする
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
    """取得済みの (area_code, year, quarter) ペアを返す。"""
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
    """取得対象の四半期 (year, quarter) リストを生成する。"""
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
