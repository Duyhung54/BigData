#!/usr/bin/env bash
# Tải bộ dữ liệu MovieLens vào data/raw/
# Dùng: ./scripts/download_data.sh [ml-25m|ml-latest-small]
set -euo pipefail

DATASET="${1:-ml-25m}"
RAW_DIR="$(dirname "$0")/../data/raw"
mkdir -p "$RAW_DIR"

if [ -d "$RAW_DIR/$DATASET" ]; then
  echo "Đã có $RAW_DIR/$DATASET, bỏ qua."
  exit 0
fi

URL="https://files.grouplens.org/datasets/movielens/${DATASET}.zip"
echo "Đang tải $URL ..."
curl -fL "$URL" -o "$RAW_DIR/${DATASET}.zip"
unzip -q "$RAW_DIR/${DATASET}.zip" -d "$RAW_DIR"
rm "$RAW_DIR/${DATASET}.zip"
echo "Xong: $RAW_DIR/$DATASET"
