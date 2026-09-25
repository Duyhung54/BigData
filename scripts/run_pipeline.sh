#!/usr/bin/env bash
# Chạy toàn bộ pipeline batch theo thứ tự.
# Dùng: ./scripts/run_pipeline.sh [ml-25m|ml-latest-small]
set -euo pipefail

DATASET="${1:-ml-25m}"
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
