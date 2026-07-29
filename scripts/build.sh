#!/usr/bin/env bash
set -euo pipefail
# trade_prices / land_prices は area・quarter / タイル単位の増分取得で、
# 公開済みカタログの取り込みが前提。queria sync は pull から始まるので、ここで pull しない
exec "$(dirname "$0")/../shared/scripts/build-dataset.sh"
