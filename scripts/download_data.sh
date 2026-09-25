#!/usr/bin/env bash
# Tải bộ dữ liệu MovieLens vào data/raw/
# Dùng: ./scripts/download_data.sh [ml-25m|ml-latest-small]
set -euo pipefail

DATASET="${1:-ml-25m}"
RAW_DIR="$(dirname "$0")/../data/raw"
ZIP="$RAW_DIR/${DATASET}.zip"
mkdir -p "$RAW_DIR"

if [ -d "$RAW_DIR/$DATASET" ]; then
  echo "Đã có $RAW_DIR/$DATASET, bỏ qua."
  exit 0
fi

URL="https://files.grouplens.org/datasets/movielens/${DATASET}.zip"

# Tải có thể nối tiếp (-C -) và thử lại: server GroupLens hay ngắt giữa chừng với
# file lớn. ml-25m.zip nặng 250 MB và lần tải đầu tiên đứt ở 56% với lỗi
# "schannel: server closed abruptly". Không có -C -, mỗi lần đứt là mất trắng
# phần đã tải và phải bắt đầu lại từ đầu.
#   --retry 5              thử lại tối đa 5 lần khi lỗi tạm thời
#   --retry-delay 5        chờ 5 giây giữa các lần
#   --speed-time/-limit    coi là treo nếu tốc độ dưới 1 KB/s suốt 60 giây,
#                          để curl bỏ cuộc và --retry vào cuộc thay vì đứng im
echo "Đang tải $URL ..."
curl -fL --retry 5 --retry-delay 5 --speed-time 60 --speed-limit 1024 \
     -C - "$URL" -o "$ZIP"

# Kiểm tra tính toàn vẹn trước khi giải nén: một file zip tải dở vẫn tồn tại trên
# đĩa và unzip sẽ báo lỗi khó hiểu ở giữa chừng, để lại thư mục giải nén một phần.
if ! unzip -t "$ZIP" >/dev/null 2>&1; then
  echo "LỖI: $ZIP hỏng hoặc chưa tải xong. Xoá đi và chạy lại script." >&2
  exit 1
fi

unzip -q "$ZIP" -d "$RAW_DIR"
rm "$ZIP"
echo "Xong: $RAW_DIR/$DATASET"
