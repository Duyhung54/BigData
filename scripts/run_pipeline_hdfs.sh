#!/usr/bin/env bash
# Chạy pipeline với TẦNG LAKE đặt trên HDFS thay vì bind mount.
#
# Chỉ lake (ratings.parquet, movies.parquet) đi lên HDFS. raw/, output/
# (recs.sqlite, model/) và checkpoint/ vẫn ở local: SQLite cần random access
# và file locking mà HDFS (write-once-read-many) không có, còn
# src/common/eligibility.py dùng shutil.copytree — Python thuần, không hiểu
# "hdfs://". Xem docs/superpowers/plans/2026-10-01-hdfs-lake-storage.md.
#
# Cần cụm HDFS đang chạy:
#   docker compose -f docker/docker-compose.yml --profile hdfs up -d
#
# Dùng: ./scripts/run_pipeline_hdfs.sh [ml-latest-small|ml-25m] [--force]
set -euo pipefail

# Tắt dịch đường dẫn của MSYS (Git Bash trên Windows) — xem chú thích cùng nội
# dung trong run_pipeline.sh. Không có dòng này "/opt/spark/bin/spark-submit"
# bị dịch thành đường dẫn Windows và job chết với exit 127.
export MSYS_NO_PATHCONV=1

ROOT="$(dirname "$0")/.."

DATASET=""
FORCE=0
for arg in "$@"; do
  case "$arg" in
    --force) FORCE=1 ;;
    ml-25m|ml-latest-small) DATASET="$arg" ;;
    *)
      echo "LỖI: tham số không hợp lệ '$arg' — chỉ nhận ml-25m, ml-latest-small hoặc --force." >&2
      exit 1
      ;;
  esac
done
DATASET="${DATASET:-ml-latest-small}"

# Cùng phép bảo vệ như run_pipeline.sh: lake lên HDFS, nhưng model, recs.sqlite
# đang phục vụ demo và report/results/*.csv vẫn ghi vào ĐÚNG đường dẫn local
# dùng chung cho mọi dataset. Chạy nhầm dataset sẽ âm thầm thay thế kết quả
# ml-25m đã tốn ~80 phút cụm để đo.
STATS_CSV="$ROOT/report/results/ingest_stats.csv"
if [ -f "$STATS_CSV" ] && [ "$FORCE" -ne 1 ]; then
  EXISTING_DATASET="$(awk -F, 'NR==2{print $1}' "$STATS_CSV")"
  if [ -n "$EXISTING_DATASET" ] && [ "$EXISTING_DATASET" != "$DATASET" ]; then
    echo "LỖI: $STATS_CSV hiện chứa kết quả của dataset '$EXISTING_DATASET'," >&2
    echo "nhưng bạn đang yêu cầu chạy với dataset '$DATASET'." >&2
    echo "" >&2
    echo "Chạy tiếp sẽ GHI ĐÈ model, recs.sqlite đang phục vụ demo, và" >&2
    echo "report/results/{ingest_stats,tuning,best_params,metrics}.csv đã commit." >&2
    echo "Nếu đúng chủ đích, chạy lại kèm --force:" >&2
    echo "  ./scripts/run_pipeline_hdfs.sh $DATASET --force" >&2
    exit 1
  fi
fi

if [ "$DATASET" = "ml-25m" ]; then
  echo "CẢNH BÁO: ml-25m cùng lúc với 3 JVM Hadoop có nguy cơ WSL2 OOM-kill worker" >&2
  echo "giữa chừng (container biến mất im lặng, không có exception Spark)." >&2
  echo "Nên dựng cụm với SPARK_WORKER_MEMORY=2g trước khi chạy." >&2
fi

# Đường dẫn compose TƯƠNG ĐỐI: MSYS_NO_PATHCONV=1 làm hỏng đường dẫn host tuyệt
# đối kiểu /d/... khi truyền cho docker.exe.
COMPOSE="docker compose -f $(dirname "$0")/../docker/docker-compose.yml --profile hdfs"
SUBMIT="/opt/spark/bin/spark-submit --master spark://spark-master:7077"

HDFS_LAKE="hdfs://namenode:9000/movielens/lake"

echo "Kiểm tra cụm HDFS..."
REPORT="$($COMPOSE exec -T namenode hdfs dfsadmin -report 2>/dev/null || true)"
LIVE="$(printf '%s\n' "$REPORT" | grep -o 'Live datanodes ([0-9]*)' | grep -o '[0-9]\+' || true)"
if [ -z "$LIVE" ]; then
  echo "LỖI: không liên lạc được với NameNode. Dựng cụm trước:" >&2
  echo "  docker compose -f docker/docker-compose.yml --profile hdfs up -d" >&2
  exit 1
fi
echo "  DataNode đang sống: $LIVE"
# dfs.replication = 2 (docker/hadoop/hdfs-site.xml, config.HDFS_REPLICATION):
# ít DataNode hơn thì block ghi ra bị under-replicated ngay từ đầu.
if [ "$LIVE" -lt 2 ]; then
  echo "LỖI: cần ít nhất 2 DataNode cho replication=2, hiện chỉ có $LIVE." >&2
  echo "Đợi ~30 giây cho DataNode đăng ký rồi chạy lại, hoặc xem 'docker logs namenode'." >&2
  exit 1
fi

$COMPOSE exec -T namenode hdfs dfs -mkdir -p /movielens/lake

for job in ingest train_als evaluate export_recs; do
  echo ""
  echo "=============================================="
  echo "  Đang chạy: $job  (dataset=$DATASET, lake=HDFS)"
  echo "=============================================="
  $COMPOSE exec -T spark-master \
    env DATASET="$DATASET" \
        SPARK_MASTER_URL=spark://spark-master:7077 \
        LAKE_ROOT="$HDFS_LAKE" \
    $SUBMIT "/opt/app/src/jobs/${job}.py"
done

echo ""
echo "Pipeline xong. Lake nằm trên HDFS tại $HDFS_LAKE"
echo "  Block mapping: $COMPOSE exec namenode hdfs fsck /movielens/lake -files -blocks -locations"
echo "  NameNode UI:   http://localhost:9870"
