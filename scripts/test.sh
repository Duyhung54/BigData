#!/usr/bin/env bash
# Chạy toàn bộ test bên trong container spark-master.
#
# USE_HDFS=false: unit test kiểm tra CODE, không phụ thuộc chế độ triển khai.
# Container spark-master mặc định nhận USE_HDFS=true (docker/.env), khi đó
# src/config.py trả đường dẫn "hdfs://..." và các test về đường dẫn (vd.
# test_config_paths_are_under_data_root) fail dù code không sai. Các test dùng
# SparkSession local[2] và thư mục tạm, không đụng tới HDFS.
set -euo pipefail
cd "$(dirname "$0")/../docker"
docker compose exec -T spark-master env USE_HDFS=false python3 -m pytest /opt/app/tests -v "$@"
