#!/usr/bin/env bash
# Đo thời gian huấn luyện ALS theo số worker.
#
# Số core mỗi worker cố định ở 2 để biến duy nhất thay đổi là SỐ LƯỢNG WORKER.
# Nếu vừa đổi số worker vừa đổi core mỗi worker thì không quy kết được kết quả
# cho yếu tố nào.
#
# ALS_RANK/ALS_REG_PARAM ép train_als.py (grid_search, Task 6) chỉ fit ĐÚNG
# MỘT tổ hợp (rank=10, regParam=0.1 — tổ hợp thắng khi chạy thử) thay vì cả
# lưới 15 tổ hợp. Không có hai biến này, mỗi phép đo thời gian sẽ gộp 15 lần
# fit khác nhau và số liệu tăng tốc theo worker sẽ vô nghĩa.
set -euo pipefail

# Tắt dịch đường dẫn của MSYS (Git Bash trên Windows) — xem chú thích cùng nội
# dung trong run_pipeline.sh. Không có dòng này, đường dẫn trong container
# "/opt/spark/bin/spark-submit" bị dịch thành đường dẫn Windows và job chết với
# exit 127. Đặt trong script để không phụ thuộc việc người gọi có nhớ export hay
# không. Vô hại trên Linux/macOS.
export MSYS_NO_PATHCONV=1

DATASET="${1:-ml-25m}"
RUNS=3
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# Đường dẫn compose file để TƯƠNG ĐỐI (không dùng $ROOT tuyệt đối), giống
# run_pipeline.sh: trên Git Bash (Windows), MSYS_NO_PATHCONV=1 (cần để các
# đường dẫn container như /opt/spark/... không bị dịch sang đường dẫn Windows)
# lại làm hỏng đường dẫn HOST tuyệt đối kiểu /d/... truyền cho docker.exe.
# Đường dẫn tương đối không bị ảnh hưởng bởi cờ đó.
COMPOSE="docker compose -f $(dirname "$0")/../docker/docker-compose.yml"
OUT="$ROOT/report/results/scaling.csv"
# File tạm: kết quả được gom vào đây trong suốt vòng lặp và chỉ ĐỔI TÊN
# (mv, thao tác gần như nguyên tử) vào OUT sau khi CẢ 9 phép đo chạy xong.
# Nếu ghi thẳng vào OUT như trước đây (echo header > "$OUT" ngay từ đầu),
# script xoá sạch số liệu ml-25m thật (~25 phút cụm để đo) NGAY KHI VỪA BẮT
# ĐẦU CHẠY — trước khi có bất kỳ phép đo nào thành công — nên một lần chạy bị
# lỗi, bị ngắt, hay CHỈ ĐỂ TEST cũng phá luôn kết quả của lần chạy thật trước
# đó. Việc này đã xảy ra thật khi test trap ở dưới trên ml-latest-small.
OUT_TMP=""

# Dọn dẹp khi script kết thúc, DÙ THÀNH CÔNG, LỖI (set -e) HAY BỊ NGẮT
# (Ctrl-C):
#   1. Khôi phục cụm Spark về cấu hình mặc định (2 worker, dùng CORES/MEMORY
#      mặc định của docker-compose.yml vì không đặt biến env cho lệnh này).
#      Không có bước này, một lần chạy bị lỗi giữa chừng (ví dụ ở cấu hình 1
#      worker) sẽ để cụm kẹt ở 1 worker mãi mãi — không có lỗi, không cảnh
#      báo, pipeline lần sau âm thầm chạy trên cụm nhỏ hơn và số liệu sai mà
#      không ai biết.
#   2. Xoá file tạm OUT_TMP nếu vòng lặp chưa chạy xong (mv sang OUT chưa xảy
#      ra) — không để sót file rác, và quan trọng hơn: đảm bảo OUT (kết quả
#      thật) không hề bị đụng tới khi thí nghiệm dang dở.
cleanup() {
  echo "Dọn dẹp: khôi phục cụm Spark về cấu hình mặc định (2 worker)..."
  if ! $COMPOSE up -d --scale spark-worker=2 spark-worker; then
    echo "CẢNH BÁO: không khôi phục được cụm về 2 worker mặc định — kiểm tra thủ công bằng 'docker compose ps'." >&2
  fi
  if [ -n "$OUT_TMP" ] && [ -f "$OUT_TMP" ]; then
    rm -f "$OUT_TMP"
    echo "Dọn dẹp: xoá file tạm $OUT_TMP (thí nghiệm chưa chạy xong nên không đụng tới $OUT)."
  fi
}
trap cleanup EXIT
# Chuyển tín hiệu ngắt thành 'exit' tường minh để trap EXIT ở trên luôn chạy
# (bash vẫn gọi trap EXIT sau khi 'exit' được gọi từ trong một trap khác).
trap 'exit 130' INT
trap 'exit 143' TERM

mkdir -p "$(dirname "$OUT")"
OUT_TMP="$(mktemp "$(dirname "$OUT")/scaling.XXXXXX.tmp")"
echo "n_workers,total_cores,run,seconds" > "$OUT_TMP"

for n in 1 2 4; do
  echo "--- Cấu hình $n worker x 2 core ---"
  SPARK_WORKER_REPLICAS=$n SPARK_WORKER_CORES=2 SPARK_WORKER_MEMORY=2g \
    $COMPOSE up -d --scale spark-worker=$n spark-worker
  sleep 15   # chờ worker đăng ký với master

  for run in $(seq 1 $RUNS); do
    start=$(date +%s.%N)
    $COMPOSE exec -T spark-master \
      env DATASET="$DATASET" SPARK_MASTER_URL=spark://spark-master:7077 \
      ALS_RANK=10 ALS_REG_PARAM=0.1 \
      /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
      /opt/app/src/jobs/train_als.py > /dev/null
    end=$(date +%s.%N)
    # awk thay cho bc: bc không có sẵn trên Git Bash (MSYS) trên host Windows,
    # trong khi awk luôn có cả trên Linux lẫn MSYS. Không có dòng này, script
    # (set -e) sẽ chết ngay sau lần đo đầu tiên với lỗi "bc: command not
    # found" — script dừng giữa chừng, không ghi được dòng CSV nào, không bao
    # giờ chạy tới cấu hình 2 hay 4 worker, và không tự khôi phục cụm về 2
    # worker mặc định.
    seconds=$(awk -v a="$start" -v b="$end" 'BEGIN { printf "%.3f", b - a }')
    echo "$n,$((n * 2)),$run,$seconds" >> "$OUT_TMP"
    echo "  lần $run: ${seconds}s"
  done
done

# Chỉ tới đây, khi cả 9 phép đo đã ghi xong vào file tạm, mới đổi tên đè lên
# kết quả thật. Đặt OUT_TMP="" sau đó để cleanup() không xoá nhầm (đường dẫn
# cũ đã không còn tồn tại sau mv, nhưng đặt rỗng cho tường minh).
mv "$OUT_TMP" "$OUT"
OUT_TMP=""
echo "Ghi kết quả: $OUT"
