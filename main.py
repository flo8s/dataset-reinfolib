"""不動産情報ライブラリ API 取得 + dbt build パイプライン。

fdl の DuckLake カタログ(FDL_* 環境変数で注入)へ API 取得データを書き込み、
dbt で変換する。R2 への公開は fdl run/sync の publish が担う。
"""

from __future__ import annotations

import json
import logging
import math
import os
import sys
from collections.abc import Generator
from contextlib import contextmanager
from datetime import date
from itertools import product

import duckdb
import pyarrow as pa
from dbt.cli.main import dbtRunner
from reinfolib import ReinfolibClient

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
LAND_SCAN_PROGRESS = "reinfolib._source.land_scan_progress"
LAND_TILE_Z = 13
LAND_START_YEAR = 1995

# 走査タイルは、日本列島を包含する少数の大矩形 (lon_min, lat_min, lon_max, lat_max)
# から z=13 タイルへ展開した和集合。各矩形は四隅がすべて海上にあり陸地を完全に含むため、
# 端の取りこぼしが原理的に起きない。海上の空タイルは取得時に skip する。
LAND_BBOXES: list[tuple[float, float, float, float]] = [
    (128.0, 30.0, 142.5, 41.6),  # 本州・四国・九州
    (139.0, 41.0, 146.2, 45.7),  # 北海道
    (122.8, 24.0, 131.6, 29.6),  # 南西諸島 (奄美〜沖縄〜先島)
    (130.9, 25.4, 131.5, 26.1),  # 大東諸島
    (141.9, 26.4, 142.4, 27.3),  # 小笠原諸島
]


@contextmanager
def _ducklake_connect() -> Generator[duckdb.DuckDBPyConnection]:
    """Open a fresh DuckDB session with the fdl-managed DuckLake attached.

    Uses the ``FDL_*`` environment variables injected by ``fdl run``: the local
    SQLite live catalog (``FDL_CATALOG_PATH``) and the data location
    (``FDL_DATA_URL``, R2 for S3 targets). The catalog is created on first
    attach when it does not exist yet.
    """
    catalog_path = os.environ["FDL_CATALOG_PATH"]
    data_url = os.environ["FDL_DATA_URL"]
    conn = duckdb.connect(":memory:")
    try:
        conn.execute("INSTALL ducklake; LOAD ducklake;")
        conn.execute("INSTALL sqlite; LOAD sqlite;")
        if data_url.startswith("s3://"):
            conn.execute("INSTALL httpfs; LOAD httpfs;")
            conn.execute(
                "CREATE SECRET (TYPE s3, KEY_ID ?, SECRET ?, ENDPOINT ?, "
                "URL_STYLE 'path', REGION 'auto')",
                [
                    os.environ["FDL_S3_ACCESS_KEY_ID"],
                    os.environ["FDL_S3_SECRET_ACCESS_KEY"],
                    os.environ["FDL_S3_ENDPOINT_HOST"],
                ],
            )
        conn.execute(
            f"ATTACH 'ducklake:{catalog_path}' AS reinfolib "
            f"(DATA_PATH '{data_url}', OVERRIDE_DATA_PATH true, "
            f"META_TYPE 'sqlite', META_JOURNAL_MODE 'WAL', BUSY_TIMEOUT 5000)"
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

    with _ducklake_connect() as conn, ReinfolibClient(api_key) as client:
        conn.execute("CREATE SCHEMA IF NOT EXISTS reinfolib._source")
        ingest_trade_prices(conn, client, areas=areas, quarters=all_quarters)

        land_latest = _latest_land_year(client)
        discover_land_price_tiles(conn, client, year=land_latest)

    # フェーズ0 で長時間保持した接続を一度閉じ、フェーズ1 は新しい接続で開始する
    # (DuckLake バックエンドの idle 接続が枯渇するのを避ける)
    land_years = list(range(LAND_START_YEAR, land_latest + 1))
    with _ducklake_connect() as conn, ReinfolibClient(api_key) as client:
        tiles = _known_land_tiles(conn)
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


def _land_scan_tiles(z: int) -> list[tuple[int, int]]:
    """LAND_BBOXES を z タイルへ展開した和集合 (昇順)。

    環境変数 LAND_BBOX_OVERRIDE="lon0,lat0,lon1,lat1" で走査範囲を上書きできる
    (検証用に範囲を狭める)。
    """
    override = os.environ.get("LAND_BBOX_OVERRIDE")
    if override:
        nums = [float(v) for v in override.split(",")]
        boxes = [(nums[0], nums[1], nums[2], nums[3])]
    else:
        boxes = LAND_BBOXES
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
    """フェーズ0: 陸地タイルを走査し、地点が存在するタイルを記録・返す。

    走査進捗と発見タイルを 1000 タイルごとに永続化し、中断後は続きから再開する。
    一度完了した有効タイルは再利用する (過去年バックフィルや再ビルドを軽くする)。
    LAND_BBOX_OVERRIDE 指定時 (検証用) は永続化せずメモリで完結する。
    """
    scan = _land_scan_tiles(LAND_TILE_Z)

    if os.environ.get("LAND_BBOX_OVERRIDE"):
        found = [
            (x, y)
            for x, y in scan
            if _fetch_land_features(client, z=LAND_TILE_Z, x=x, y=y, year=year)
        ]
        logger.info(
            "land tiles (override): %d/%d tiles have points", len(found), len(scan)
        )
        return found

    _ensure_tile_tables(conn)
    scanned, total = _scan_progress(conn)
    if total != len(scan):
        # 走査範囲が変わった or 初回 → やり直し
        _reset_scan(conn)
        scanned = 0
    if scanned >= len(scan):
        known = _known_land_tiles(conn)
        logger.info("land tiles: reuse %d known tiles (scan complete)", len(known))
        return known

    logger.info(
        "land tiles: scanning %d candidate tiles from %d (year=%d)",
        len(scan),
        scanned,
        year,
    )
    batch: list[tuple[int, int]] = []
    for i in range(scanned, len(scan)):
        x, y = scan[i]
        if _fetch_land_features(client, z=LAND_TILE_Z, x=x, y=y, year=year):
            batch.append((x, y))
        if (i + 1) % 1000 == 0:
            _checkpoint_tiles(conn, batch, i + 1, len(scan))
            batch = []
            n = conn.execute(f"SELECT count(*) FROM {LAND_TILES_TABLE}").fetchone()[0]
            logger.info("  scanned %d/%d, found %d", i + 1, len(scan), n)
    _checkpoint_tiles(conn, batch, len(scan), len(scan))

    known = _known_land_tiles(conn)
    logger.info("land tiles: %d tiles have points", len(known))
    return known


def _ensure_tile_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS {LAND_TILES_TABLE} (z INTEGER, x INTEGER, y INTEGER)"
    )
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS {LAND_SCAN_PROGRESS} (scanned BIGINT, total BIGINT)"
    )


def _scan_progress(conn: duckdb.DuckDBPyConnection) -> tuple[int, int]:
    try:
        row = conn.execute(
            f"SELECT scanned, total FROM {LAND_SCAN_PROGRESS} LIMIT 1"
        ).fetchone()
    except duckdb.CatalogException:
        return 0, 0
    return (int(row[0]), int(row[1])) if row else (0, 0)


def _reset_scan(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(f"DELETE FROM {LAND_TILES_TABLE}")
    conn.execute(f"DELETE FROM {LAND_SCAN_PROGRESS}")


def _checkpoint_tiles(
    conn: duckdb.DuckDBPyConnection,
    found: list[tuple[int, int]],
    scanned: int,
    total: int,
) -> None:
    """発見タイルの追記と走査進捗の更新をまとめて永続化する。"""
    conn.execute("BEGIN")
    if found:
        rows = [{"z": LAND_TILE_Z, "x": x, "y": y} for x, y in found]
        conn.register("_tiles", pa.Table.from_pylist(rows))
        conn.execute(f"INSERT INTO {LAND_TILES_TABLE} SELECT * FROM _tiles")
        conn.unregister("_tiles")
    conn.execute(f"DELETE FROM {LAND_SCAN_PROGRESS}")
    conn.execute(f"INSERT INTO {LAND_SCAN_PROGRESS} VALUES (?, ?)", [scanned, total])
    conn.execute("COMMIT")


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
    """フェーズ1: 有効タイルごとに対象年をまとめて取得する。

    タイル単位で対象年を取得し 1 トランザクションで書き込むことで、DuckLake
    バックエンドへのトランザクション数を抑える (接続枯渇対策)。取得済みかどうかで
    取得年を自動で切り替える差分更新:
    - 新規タイル (land_prices に未登録): 全年 (初回バックフィル)
    - 取得済みタイル: 最新年のみ再取得 → 毎年の地価公示(3月)・地価調査(9月)の
      新規公表を取り込む。env フラグ不要でローカル/CI とも同じ挙動。
    """
    if not tiles:
        logger.warning("land prices: no tiles to ingest")
        return

    latest = years[-1]
    ingested = _ingested_land_tiles(conn)
    total = len(tiles)
    logger.info(
        "land prices: %d/%d tiles 取得済み (新規=全年, 既存=最新年%d を再取得)",
        len(ingested),
        total,
        latest,
    )

    for idx, (x, y) in enumerate(tiles):
        tile_years = [latest] if (x, y) in ingested else years
        rows: list[dict] = []
        for year in tile_years:
            for f in _fetch_land_features(client, z=LAND_TILE_Z, x=x, y=y, year=year):
                rows.append(_feature_to_row(f, x, y, year))
        _write_tile_rows(conn, x, y, tile_years, rows)
        if (idx + 1) % 100 == 0:
            n = conn.execute(f"SELECT count(*) FROM {LAND_TABLE}").fetchone()[0]
            logger.info("  tiles %d/%d, rows %d", idx + 1, total, n)

    logger.info("land prices ingest done: %d tiles processed", total)


def _feature_to_row(f: dict, x: int, y: int, year: int) -> dict:
    """GeoJSON feature を _source.land_prices の行 dict に変換する。

    properties はキー構成が地点ごとに変動するため、JSON 文字列のまま保持して
    raw スキーマを固定 8 カラムにする。構造化は stg 層の json_extract に委ねる。
    """
    props = dict(f.get("properties", {}))
    props.pop("_id", None)
    props.pop("_index", None)
    geom = f.get("geometry") or {}
    coords = geom.get("coordinates") or [None, None]
    return {
        "properties": json.dumps(props, ensure_ascii=False),
        "longitude": coords[0],
        "latitude": coords[1],
        "geometry": json.dumps(geom, ensure_ascii=False),
        "_z": LAND_TILE_Z,
        "_x": x,
        "_y": y,
        "_year": year,
    }


def _write_tile_rows(
    conn: duckdb.DuckDBPyConnection,
    x: int,
    y: int,
    years: list[int],
    rows: list[dict],
) -> None:
    """1 タイル分の行を 1 トランザクションで書き込む (対象年を入れ替え)。"""
    if not rows:
        return
    conn.register("_batch", pa.Table.from_pylist(rows))
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS {LAND_TABLE} AS SELECT * FROM _batch WITH NO DATA"
    )
    conn.execute("BEGIN")
    if len(years) == 1:
        conn.execute(
            f"DELETE FROM {LAND_TABLE} WHERE _x = ? AND _y = ? AND _year = ?",
            [x, y, years[0]],
        )
    else:
        conn.execute(f"DELETE FROM {LAND_TABLE} WHERE _x = ? AND _y = ?", [x, y])
    conn.execute(f"INSERT INTO {LAND_TABLE} SELECT * FROM _batch")
    conn.execute("COMMIT")
    conn.unregister("_batch")


def _ingested_land_tiles(
    conn: duckdb.DuckDBPyConnection,
) -> set[tuple[int, int]]:
    try:
        return {
            (r[0], r[1])
            for r in conn.execute(
                f"SELECT DISTINCT _x, _y FROM {LAND_TABLE}"
            ).fetchall()
        }
    except duckdb.CatalogException:
        return set()


def _verify_land_coverage(conn: duckdb.DuckDBPyConnection) -> None:
    """取得した地価データのカバレッジを検証しログ出力する (取りこぼしの安全網)。

    47都道府県の欠落と総地点数で走査範囲の取りこぼしを検出する。
    """
    prefs = {
        r[0]
        for r in conn.execute(
            "SELECT DISTINCT json_extract_string(properties, '$.prefecture_code') "
            f"FROM {LAND_TABLE}"
        ).fetchall()
    }
    missing = {f"{i:02d}" for i in range(1, 48)} - prefs
    if missing:
        logger.warning("land coverage: 地価データが無い都道府県: %s", sorted(missing))
    else:
        logger.info("land coverage: 47都道府県すべてに地価データあり")

    total = conn.execute(f"SELECT count(*) FROM {LAND_TABLE}").fetchone()[0]
    logger.info("land coverage: 総地点数(全年) %d", total)


if __name__ == "__main__":
    main()
