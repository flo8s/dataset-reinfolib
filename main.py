"""不動産情報ライブラリ API 取得 + dbt build + snapshot pipeline.

Snapshot must run in the SAME Python process as dbt build — see
dataset-shared/README.md for the constraint detail.
"""

from __future__ import annotations

import csv
import importlib.util
import json
import logging
import math
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

# -- XPT002: 地価公示・地価調査ポイント --
LAND_TABLE = "reinfolib._source.land_prices"
LAND_TILES_TABLE = "reinfolib._source.land_price_tiles"
LAND_TILE_Z = 13
LAND_START_YEAR = 1995

# 走査タイルは nlftp の市区町村境界 bbox (data/municipality_bbox.csv) から生成する。
# 全市区町村域を漏れなくカバーするため、手書き矩形のような端の取りこぼしが起きない。
# CSV は nlftp.boundary.municipality の各ポリゴン bbox を抽出したもの。
LAND_BBOX_CSV = Path(__file__).resolve().parent / "data" / "municipality_bbox.csv"
# 境界の簡略化(ST_CoverageSimplify 0.002)と小島除去の誤差を吸収するバッファ(度)
LAND_BBOX_BUFFER = 0.01


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

        land_latest = _latest_land_year(client)
        land_years = (
            [land_latest]
            if os.environ.get("LAND_LATEST_YEAR_ONLY")
            else list(range(LAND_START_YEAR, land_latest + 1))
        )
        tiles = discover_land_price_tiles(conn, client, year=land_latest)
        ingest_land_prices(conn, client, tiles=tiles, years=land_years)
        _verify_land_coverage(conn)

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


def _lonlat_to_tile(lon: float, lat: float, z: int) -> tuple[int, int]:
    """経緯度を XYZ タイル座標へ変換する。"""
    n = 2**z
    x = int((lon + 180.0) / 360.0 * n)
    y = int((1.0 - math.asinh(math.tan(math.radians(lat))) / math.pi) / 2.0 * n)
    return x, y


def _load_municipality_bboxes() -> list[tuple[float, float, float, float]]:
    """市区町村境界 bbox をバッファ込みで読み込む。"""
    b = LAND_BBOX_BUFFER
    with open(LAND_BBOX_CSV, newline="") as f:
        return [
            (
                float(r["xmin"]) - b,
                float(r["ymin"]) - b,
                float(r["xmax"]) + b,
                float(r["ymax"]) + b,
            )
            for r in csv.DictReader(f)
        ]


def _land_scan_tiles(z: int) -> list[tuple[int, int]]:
    """市区町村境界 bbox を z タイルへ展開した和集合 (昇順)。

    環境変数 LAND_BBOX_OVERRIDE="lon0,lat0,lon1,lat1" で走査範囲を上書きできる
    (検証用に範囲を狭める)。
    """
    override = os.environ.get("LAND_BBOX_OVERRIDE")
    if override:
        nums = [float(v) for v in override.split(",")]
        boxes = [(nums[0], nums[1], nums[2], nums[3])]
    else:
        boxes = _load_municipality_bboxes()
    tiles: set[tuple[int, int]] = set()
    for lon0, lat0, lon1, lat1 in boxes:
        x0, y0 = _lonlat_to_tile(lon0, lat1, z)  # 北西
        x1, y1 = _lonlat_to_tile(lon1, lat0, z)  # 南東
        for x in range(min(x0, x1), max(x0, x1) + 1):
            for y in range(min(y0, y1), max(y0, y1) + 1):
                tiles.add((x, y))
    return sorted(tiles)


def _fetch_land_features(
    client: ReinfolibClient, *, z: int, x: int, y: int, year: int
) -> list[dict]:
    """XPT002 を GeoJSON で取得し features を返す。"""
    resp = client.get_land_prices_point(z=z, x=x, y=y, year=year)
    return resp.get("features", []) if isinstance(resp, dict) else []


def _latest_land_year(client: ReinfolibClient) -> int:
    """当年の地価公示が公開済みかを代表タイルで確認し、最新の有効年を返す。"""
    px, py = _lonlat_to_tile(139.767, 35.681, LAND_TILE_Z)  # 東京駅周辺
    current = date.today().year
    for year in (current, current - 1):
        if _fetch_land_features(client, z=LAND_TILE_Z, x=px, y=py, year=year):
            return year
    return current - 1


def discover_land_price_tiles(
    conn: duckdb.DuckDBPyConnection, client: ReinfolibClient, *, year: int
) -> list[tuple[int, int]]:
    """フェーズ0: 最新年で陸地タイルを走査し、地点が存在するタイルを記録・返す。

    一度記録した有効タイルは再利用する (過去年バックフィルや再ビルドを軽くする)。
    """
    known = _known_land_tiles(conn)
    if known:
        logger.info("land tiles: reuse %d known tiles", len(known))
        return known

    scan = _land_scan_tiles(LAND_TILE_Z)
    logger.info("land tiles: scanning %d candidate tiles (year=%d)", len(scan), year)
    found: list[tuple[int, int]] = []
    for i, (x, y) in enumerate(scan):
        if _fetch_land_features(client, z=LAND_TILE_Z, x=x, y=y, year=year):
            found.append((x, y))
        if (i + 1) % 1000 == 0:
            logger.info("  scanned %d/%d, found %d", i + 1, len(scan), len(found))

    rows = [{"z": LAND_TILE_Z, "x": x, "y": y} for x, y in found]
    conn.register("_tiles", pa.Table.from_pylist(rows))
    conn.execute(f"CREATE OR REPLACE TABLE {LAND_TILES_TABLE} AS SELECT * FROM _tiles")
    conn.unregister("_tiles")
    logger.info("land tiles: %d tiles have points", len(found))
    return found


def _known_land_tiles(conn: duckdb.DuckDBPyConnection) -> list[tuple[int, int]]:
    try:
        return [
            (r[0], r[1])
            for r in conn.execute(
                f"SELECT x, y FROM {LAND_TILES_TABLE} ORDER BY x, y"
            ).fetchall()
        ]
    except duckdb.CatalogException:
        return []


def ingest_land_prices(
    conn: duckdb.DuckDBPyConnection,
    client: ReinfolibClient,
    *,
    tiles: list[tuple[int, int]],
    years: list[int],
) -> None:
    """フェーズ1: 有効タイル×年で地価公示・地価調査ポイントを取得する。"""
    if not tiles:
        logger.warning("land prices: no tiles to ingest")
        return

    latest = years[-1]
    completed = _completed_land_pairs(conn)
    total = len(tiles) * len(years)
    logger.info("land prices: %d completed / %d (tile,year)", len(completed), total)

    fetched = 0
    for (x, y), year in product(tiles, years):
        if (x, y, year) in completed and year != latest:
            continue
        feats = _fetch_land_features(client, z=LAND_TILE_Z, x=x, y=y, year=year)
        if not feats:
            continue
        fetched += 1

        rows = []
        for f in feats:
            row = dict(f.get("properties", {}))
            row.pop("_id", None)
            row.pop("_index", None)
            geom = f.get("geometry") or {}
            coords = geom.get("coordinates") or [None, None]
            row["longitude"] = coords[0]
            row["latitude"] = coords[1]
            row["geometry"] = json.dumps(geom, ensure_ascii=False)
            row["_z"] = LAND_TILE_Z
            row["_x"] = x
            row["_y"] = y
            row["_year"] = year
            rows.append(row)

        conn.register("_batch", pa.Table.from_pylist(rows))
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {LAND_TABLE} AS SELECT * FROM _batch WITH NO DATA"
        )
        conn.execute("BEGIN")
        conn.execute(
            f"DELETE FROM {LAND_TABLE} WHERE _x = ? AND _y = ? AND _year = ?",
            [x, y, year],
        )
        conn.execute(f"INSERT INTO {LAND_TABLE} SELECT * FROM _batch")
        conn.execute("COMMIT")
        conn.unregister("_batch")

    logger.info("land prices ingest done: %d (tile,year) fetched", fetched)


def _completed_land_pairs(
    conn: duckdb.DuckDBPyConnection,
) -> set[tuple[int, int, int]]:
    try:
        return {
            (r[0], r[1], r[2])
            for r in conn.execute(
                f"SELECT DISTINCT _x, _y, _year FROM {LAND_TABLE}"
            ).fetchall()
        }
    except duckdb.CatalogException:
        return set()


def _municipality_codes() -> set[str]:
    """市区町村境界 CSV の全 lg_code (5桁) を返す。"""
    with open(LAND_BBOX_CSV, newline="") as f:
        return {r["lg_code"] for r in csv.DictReader(f)}


def _verify_land_coverage(conn: duckdb.DuckDBPyConnection) -> None:
    """取得した地価データのカバレッジを検証しログ出力する (取りこぼしの安全網)。

    完全な正解数は不明だが、47都道府県の欠落と市区町村カバー率の異常で
    走査範囲の取りこぼしを検出する。
    """
    prefs = {
        r[0]
        for r in conn.execute(
            f"SELECT DISTINCT prefecture_code FROM {LAND_TABLE}"
        ).fetchall()
    }
    missing = {f"{i:02d}" for i in range(1, 48)} - prefs
    if missing:
        logger.warning("land coverage: 地価データが無い都道府県: %s", sorted(missing))
    else:
        logger.info("land coverage: 47都道府県すべてに地価データあり")

    muni = _municipality_codes()
    got = {
        r[0]
        for r in conn.execute(
            f"SELECT DISTINCT city_code FROM {LAND_TABLE}"
        ).fetchall()
    }
    covered = got & muni
    logger.info(
        "land coverage: 市区町村 %d/%d (%.1f%%) に地価地点あり",
        len(covered),
        len(muni),
        100.0 * len(covered) / len(muni) if muni else 0.0,
    )


if __name__ == "__main__":
    main()
