#!/usr/bin/env bash
# Chạy toàn bộ pipeline batch theo thứ tự.
# Dùng: ./scripts/run_pipeline.sh [ml-25m|ml-latest-small] [--force]
set -euo pipefail

# Tắt dịch đường dẫn của MSYS (Git Bash trên Windows). Không có dòng này, Git
# Bash biến đường dẫn TRONG CONTAINER "/opt/spark/bin/spark-submit" thành
# "C:/Program Files/Git/opt/spark/bin/spark-submit" trước khi truyền cho
# docker.exe, và job chết ngay với exit 127 "No such file or directory".
# Đặt ở đây chứ không bắt người gọi tự nhớ: một script chỉ chạy đúng khi người
# dùng nhớ export biến môi trường là một script hỏng.
# Vô hại trên Linux/macOS, nơi MSYS_NO_PATHCONV không có ý nghĩa gì.
export MSYS_NO_PATHCONV=1

ROOT="$(dirname "$0")/.."

# Nhận dataset và --force theo bất kỳ thứ tự nào (cả hai đều tuỳ chọn).
DATASET=""
FORCE=0
for arg in "$@"; do
  case "$arg" in
    --force) FORCE=1 ;;
    ml-25m|ml-latest-small) DATASET="$arg" ;;
    *)
      echo "LỖI: dataset không hợp lệ '$arg' — chỉ nhận ml-25m hoặc ml-latest-small." >&2
      exit 1
      ;;
  esac
done
DATASET="${DATASET:-ml-25m}"

# Bảo vệ khỏi ghi đè nhầm dataset: src/config.py scope INPUT theo dataset
# (RAW_DIR/<dataset>/...) nhưng OUTPUT (lake, model, recs.sqlite, các CSV kết
# quả trong report/results/) KHÔNG được scope theo dataset — cố tình, vì
# serving/api.py hardcode ba trong số các đường dẫn đó độc lập với config.py,
# namespacing sẽ làm demo âm thầm hỏng (xem ghi chú trong src/config.py).
# Hệ quả: chạy nhầm dataset (ví dụ đổi từ ml-25m sang ml-latest-small chỉ để
# smoke-test nhanh trước demo) sẽ âm thầm THAY THẾ lake 268 MB, model đã
# train, recs.sqlite 222 MB phục vụ demo, và ghi đè 4 file CSV kết quả đã
# commit — không cảnh báo, không cách nào biết sau đó dataset nào đã sinh ra
# các CSV này vì chúng không có cột dataset (trừ ingest_stats.csv).
# Vì vậy: nếu report/results/ingest_stats.csv đã có và cột dataset của nó
# khác với $DATASET đang yêu cầu, từ chối chạy trừ khi có --force.
STATS_CSV="$ROOT/report/results/ingest_stats.csv"
if [ -f "$STATS_CSV" ] && [ "$FORCE" -ne 1 ]; then
  EXISTING_DATASET="$(awk -F, 'NR==2{print $1}' "$STATS_CSV")"
  if [ -n "$EXISTING_DATASET" ] && [ "$EXISTING_DATASET" != "$DATASET" ]; then
    echo "LỖI: $STATS_CSV hiện chứa kết quả của dataset '$EXISTING_DATASET'," >&2
    echo "nhưng bạn đang yêu cầu chạy pipeline với dataset '$DATASET'." >&2
    echo "" >&2
    echo "Chạy tiếp sẽ GHI ĐÈ (không cảnh báo, không thể hoàn tác):" >&2
    echo "  - lake Parquet đang dùng của '$EXISTING_DATASET' (data/lake/)" >&2
    echo "  - model ALS đang phục vụ (data/output/model/)" >&2
    echo "  - recs.sqlite đang phục vụ demo (data/output/recs.sqlite)" >&2
    echo "  - report/results/{ingest_stats,tuning,best_params,metrics}.csv đã commit" >&2
    echo "" >&2
    echo "Nếu đây đúng là chủ đích (đổi hẳn dataset đang phục vụ), chạy lại kèm --force:" >&2
    echo "  ./scripts/run_pipeline.sh $DATASET --force" >&2
    exit 1
  fi
fi

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
