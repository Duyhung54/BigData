#!/usr/bin/env python
"""Job: Cập nhật model ALS từ dữ liệu Delta Lake (streaming đã ghi).

- Đọc Delta Lake (hoặc Parquet gốc nếu Delta chưa tồn tại).
- Tính train/val/test bằng hàm `add_split_column`.
- Fit ALS, lưu model vào `config.MODEL_DIR`.
"""

import csv as csv_module
from pathlib import Path

from src import config
from src.common.split import add_split_column, count_excluded_users
from src.session import get_spark
from src.jobs.train_als import build_als, evaluate_rmse, grid_search


def main() -> None:
    spark = get_spark("train_als_stream")

    delta_path = config.DELTA_LAKE_PATH
    
    # Dùng try-except để tương thích với cả HDFS và Local
    try:
        stream_ratings = spark.read.format("delta").load(delta_path)
        stream_count = stream_ratings.count()
        has_delta = stream_count > 0
    except Exception:
        stream_count = 0
        has_delta = False

    # Kiểm tra trạng thái đã train trước đó để tránh train lặp vô nghĩa
    state_file = config.OUTPUT_DIR / ".last_trained_stream_count"
    last_trained_count = -1
    if state_file.exists():
        try:
            last_trained_count = int(state_file.read_text().strip())
        except Exception:
            pass

    if has_delta and stream_count == last_trained_count:
        print(f"Không có dữ liệu stream mới (hiện tại: {stream_count} ratings). Bỏ qua lượt train để tiết kiệm CPU và đĩa!")
        spark.stop()
        return
    elif not has_delta and config.RECS_SQLITE.exists() and last_trained_count == 0:
        print("Chưa có dữ liệu stream và Model lịch sử đã tồn tại. Bỏ qua lượt train!")
        spark.stop()
        return

    # Ưu tiên đọc Delta Lake (dữ liệu streaming đã ghi).
    # Nếu Delta chưa có, fallback về Parquet gốc (batch pipeline).
    if has_delta:
        print(f"Đọc dữ liệu MỚI từ Delta Lake: {config.DELTA_LAKE_PATH} ({stream_count} ratings)")
        
        print(f"Đọc dữ liệu LỊCH SỬ từ Parquet: {config.RATINGS_PARQUET}")
        historical_ratings = spark.read.parquet(config.RATINGS_PARQUET)
        
        print("Gộp (Union) dữ liệu cũ và mới để train...")
        ratings = historical_ratings.unionByName(stream_ratings)
    else:
        try:
            ratings = spark.read.parquet(config.RATINGS_PARQUET)
            print(f"Delta Lake chưa tồn tại, chỉ đọc dữ liệu LỊCH SỬ từ Parquet: {config.RATINGS_PARQUET}")
        except Exception:
            print("Không tìm thấy dữ liệu (Delta Lake hoặc Parquet). Hãy chạy pipeline trước.")
            spark.stop()
            return
    excluded = count_excluded_users(ratings)
    print(f"Excluded users (cold-start): {excluded}")

    # Tối ưu cho Streaming: Không chạy lại Grid Search 15 tổ hợp nữa (tránh spill 4GB rác BlockManager ra /tmp)
    # Lấy tổ hợp siêu tham số tối ưu đã tìm được
    best_rank = 10
    best_reg = 0.1
    best_path = config.RESULTS_DIR / "best_params.csv"
    if best_path.exists():
        try:
            with open(best_path, "r", encoding="utf-8") as fh:
                reader = csv_module.DictReader(fh)
                row = next(reader)
                best_rank = int(row.get("rank", 10))
                best_reg = float(row.get("regParam", 0.1))
        except Exception:
            pass

    print(f"Huấn luyện ALS tối ưu cho Streaming: rank={best_rank}, regParam={best_reg}")
    training_data = ratings.select("userId", "movieId", "rating")
    final_model = build_als(best_rank, best_reg).fit(training_data)
    final_model.write().overwrite().save(str(config.MODEL_DIR))

    print(f"Model saved to {config.MODEL_DIR}")

    # Ghi nhận trạng thái để tránh train lại khi không có data mới
    try:
        state_file.parent.mkdir(parents=True, exist_ok=True)
        state_file.write_text(str(stream_count))
    except Exception:
        pass

    # Dọn dẹp các snapshot/file parquet cũ trong Delta Lake (VACUUM)
    if has_delta:
        try:
            spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
            spark.sql(f"VACUUM delta.`{config.DELTA_LAKE_PATH}` RETAIN 0 HOURS")
            print("Đã chạy VACUUM dọn dẹp các file cũ không dùng trong Delta Lake.")
        except Exception as e:
            print(f"Bỏ qua VACUUM: {e}")

    # Chạy luôn job export để sinh lại file recs.sqlite
    # Nhờ mtime của file này thay đổi, FastAPI sẽ tự động hot-reload Model
    print("Đang cập nhật lại CSDL SQLite (export_recs)...")
    try:
        from src.jobs import export_recs
        export_recs.main()
    except Exception as e:
        print(f"Lỗi khi export SQLite: {e}")

    spark.stop()


if __name__ == "__main__":
    main()