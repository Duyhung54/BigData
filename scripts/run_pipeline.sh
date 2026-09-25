#!/usr/bin/env bash
# Chạy toàn bộ pipeline batch theo thứ tự.
# Dùng: ./scripts/run_pipeline.sh [ml-25m|ml-latest-small]
set -euo pipefail

# Tắt dịch đường dẫn của MSYS (Git Bash trên Windows). Không có dòng này, Git
# Bash biến đường dẫn TRONG CONTAINER "/opt/spark/bin/spark-submit" thành
# "C:/Program Files/Git/opt/spark/bin/spark-submit" trước khi truyền cho
# docker.exe, và job chết ngay với exit 127 "No such file or directory".
# Đặt ở đây chứ không bắt người gọi tự nhớ: một script chỉ chạy đúng khi người
# dùng nhớ export biến môi trường là một script hỏng.
# Vô hại trên Linux/macOS, nơi MSYS_NO_PATHCONV không có ý nghĩa gì.
export MSYS_NO_PATHCONV=1

DATASET="${1:-ml-25m}"
# Đường dẫn compose để TƯƠNG ĐỐI: MSYS_NO_PATHCONV=1 ở trên lại làm hỏng đường
# dẫn HOST tuyệt đối kiểu /d/... khi truyền cho docker.exe. Đường dẫn tương đối
# không bị ảnh hưởng bởi cờ đó.
COMPOSE="docker compose -f $(dirname "$0")/../docker/docker-compose.yml"
SUBMIT="/opt/spark/bin/spark-submit --master spark://spark-master:7077"

for job in ingest train_als evaluate export_recs; do
  echo ""
  echo "=============================================="
  echo "  Đang chạy: $job  (dataset=$DATASET)"
  echo "=============================================="
  $COMPOSE exec -T spark-master \
    env DATASET="$DATASET" SPARK_MASTER_URL=spark://spark-master:7077 \
    $SUBMIT "/opt/app/src/jobs/${job}.py"
done

echo ""
echo "Pipeline xong. Mở http://localhost:8000 để xem demo."
