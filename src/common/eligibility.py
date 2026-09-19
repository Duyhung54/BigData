"""Lọc phim đủ điều kiện làm ứng viên gợi ý.

Dùng chung giữa evaluate.py (Task 7) và export_recs.py (Task 8): cả hai đều
cần loại phim quá ít lượt đánh giá ra khỏi tập ứng viên trước khi gọi
recommendForAllUsers/recommendForUserSubset, vì lý do nêu trong docstring
của eligible_items bên dưới.
"""
import shutil
from pathlib import Path

from pyspark.ml.recommendation import ALSModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src import config


def eligible_items(
    train_val: DataFrame, min_ratings: int = config.MIN_RATINGS_FOR_RECOMMENDATION
) -> DataFrame:
    """Phim đủ điều kiện làm ứng viên gợi ý: >= min_ratings lượt đánh giá trong train+val.

    Không lọc bước này, ALS coi phim chỉ có 1 lượt đánh giá là ứng viên ngang hàng
    với phim có hàng trăm lượt — nhưng factor của phim 1-lượt được ước lượng từ
    đúng một quan sát nên điểm dự đoán bị đẩy tới cực trị và chiếm hết top-K
    (xem MIN_RATINGS_FOR_RECOMMENDATION trong config.py).
    """
    return (
        train_val.groupBy("movieId")
        .agg(F.count(F.lit(1)).alias("_n"))
        .filter(F.col("_n") >= min_ratings)
        .select("movieId")
    )


def restrict_model_to_eligible_items(
    spark: SparkSession, model: ALSModel, eligible: DataFrame, tmp_dir
) -> ALSModel:
    """Trả về một ALSModel mà itemFactors chỉ còn các phim trong `eligible`.

    Mục đích: tái dùng recommendForUserSubset (thuật toán top-k khối hoá,
    tối ưu của Spark ALS — nhân ma trận cục bộ theo khối rồi lấy top-k cục
    bộ, không bao giờ vật chất hoá toàn bộ tích user x item) để sinh gợi ý
    CHỈ trong tập ứng viên đủ điều kiện, thay vì crossJoin(test_users, eligible)
    rồi tự xếp hạng bằng window function. Trên ml-latest-small, crossJoin cho
    586 x 1.110 ~ 650k cặp — chấp nhận được. Trên ml-25m, con số này thành
    162.541 user x (~7.000-15.000 phim đủ điều kiện) = 1,1-2,5 TỶ cặp, tức
    35-100 GB dữ liệu trung gian cộng với một lần shuffle-sort toàn phần cho
    window function — vượt xa 7,66 GB RAM của Docker trên máy này và chắc
    chắn OOM chứ không chỉ chậm.

    ALSModel không có constructor công khai trong PySpark (không thể tự dựng
    một model mới từ DataFrame factor), nhưng định dạng lưu của nó chỉ là
    Parquet thuần (metadata/ + itemFactors/ + userFactors/, xem
    org.apache.spark.ml.recommendation.ALSModel.load), nên có thể build một
    bản "bị cắt" bằng API công khai:
      1. Ghi model gốc ra `tmp_dir/full` (ALSModel.write().overwrite()).
      2. Đọc lại itemFactors từ đó, inner-join với `eligible` (đổi tên
         movieId -> id để khớp cột "id" của itemFactors).
      3. Ghi kết quả lọc vào MỘT thư mục MỚI HOÀN TOÀN `tmp_dir/restricted`,
         chưa từng được đọc trong lần gọi này — không bao giờ ghi đè một
         thư mục Parquet đang đồng thời được đọc. Copy nguyên vẹn metadata/
         và userFactors/ từ `full` sang `restricted` bằng shutil (không phụ
         thuộc gì vào itemFactors nên không cần đi qua Spark).
      4. ALSModel.load(tmp_dir/restricted).

    Lựa chọn "ghi ra thư mục mới" thay vì "đọc hết vào bộ nhớ rồi ghi đè tại
    chỗ" (cache()+count() rồi overwrite cùng đường dẫn): ghi-thư-mục-mới
    không phụ thuộc vào việc cache có bị Spark evict giữa chừng hay không,
    nên đúng trong mọi trường hợp, kể cả khi itemFactors lớn hơn bộ nhớ khả
    dụng của driver — đúng tình huống ml-25m mà hàm này được viết ra để né.
    """
    tmp_dir = Path(str(tmp_dir))
    full_dir = tmp_dir / "full"
    restricted_dir = tmp_dir / "restricted"

    model.write().overwrite().save(str(full_dir))

    if restricted_dir.exists():
        shutil.rmtree(str(restricted_dir))

    item_factors = spark.read.parquet(str(full_dir / "itemFactors"))
    eligible_ids = eligible.select(F.col("movieId").alias("id")).distinct()
    restricted_factors = item_factors.join(eligible_ids, on="id", how="inner")
    restricted_factors.write.mode("overwrite").parquet(str(restricted_dir / "itemFactors"))

    shutil.copytree(str(full_dir / "metadata"), str(restricted_dir / "metadata"))
    shutil.copytree(str(full_dir / "userFactors"), str(restricted_dir / "userFactors"))

    return ALSModel.load(str(restricted_dir))
