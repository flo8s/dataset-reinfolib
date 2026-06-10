#!/usr/bin/env bash
set -euo pipefail
target="${1:-local}"
# trade_prices / land_prices は area・quarter / タイル単位の増分取得のため、
# 公開済みカタログを取り込んでから差分のみ取得する(初回は未公開なので無視)。
uv run fdl pull "$target" || true
exec "$(dirname "$0")/../shared/scripts/build-dataset.sh" "$target"
