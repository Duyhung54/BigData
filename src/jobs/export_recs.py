"""Job 4: xuất kết quả batch sang dạng tầng serving đọc được.

Đây là ranh giới bàn giao giữa batch và serving. Sau job này, tầng serving
chỉ cần SQLite và một mảng numpy — không cần Spark, không cần Java.
"""
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
from pyspark.ml.recommendation import ALSModel
from pyspark.sql import Window
from pyspark.sql import functions as F

from src import config
from src.common.eligibility import eligible_items, restrict_model_to_eligible_items
from src.common.split import add_split_column
from src.session import get_spark

HISTORY_PER_USER = 20


def write_sqlite(
    recs_pdf: pd.DataFrame,
    movies_pdf: pd.DataFrame,
    ratings_sample_pdf: pd.DataFrame,
    db_path: Path,
) -> None:
    """Ghi gợi ý, mẫu rating và metadata phim vào SQLite, có index trên userId.

    if_exists="replace" để chạy lại job không nhân đôi dữ liệu.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        recs_pdf.to_sql("recommendations", conn, if_exists="replace", index=False)
        movies_pdf.to_sql("movies", conn, if_exists="replace", index=False)
        ratings_sample_pdf.to_sql("ratings_sample", conn, if_exists="replace", index=False)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_recommendations_userId "
            "ON recommendations(userId)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ratings_sample_userId "
            "ON ratings_sample(userId)"
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_movies_movieId ON movies(movieId)")
        conn.commit()


def top_rated_per_user(ratings, n: int = HISTORY_PER_USER):
    """Giữ n phim user chấm cao nhất, để giao diện đối chiếu với gợi ý.

    Không xuất toàn bộ 25 triệu rating sang SQLite: giao diện chỉ cần đủ để
    người xem thấy sở thích của user, và file SQLite phải đủ nhỏ để nạp nhanh.
    """
    order = Window.partitionBy("userId").orderBy(F.col("rating").desc(), F.col("movieId"))
    return (
        ratings.withColumn("_r", F.row_number().over(order))
        .filter(F.col("_r") <= n)
        .select("userId", "movieId", "rating")
    )


def main() -> None:
    spark = get_spark("export_recs")

    model = ALSModel.load(str(config.MODEL_DIR))
    movies = spark.read.parquet(str(config.MOVIES_PARQUET))
    ratings = spark.read.parquet(str(config.RATINGS_PARQUET))

    # Lọc ứng viên gợi ý xuống các phim có đủ min-support, giống Task 7: không lọc,
    # ALS chọn top-K toàn phim 1-lượt-đánh-giá (factor ước lượng từ đúng một quan sát
    # nên điểm dự đoán bị đẩy tới cực trị) — một demo cho thấy 10 phim chưa ai từng
    # xem thì trông như hỏng. eligible_items/restrict_model_to_eligible_items dùng
    # lại nguyên vẹn từ src/common/eligibility.py (viết cho evaluate.py ở Task 7).
    split = add_split_column(ratings)
    train_val = split.filter("split IN ('train', 'val')").select("userId", "movieId", "rating")
    eligible = eligible_items(train_val, config.MIN_RATINGS_FOR_RECOMMENDATION).cache()
    n_eligible = eligible.count()
    catalog_size = ratings.select("movieId").distinct().count()
    print(
        f"Phim đủ điều kiện gợi ý (>= {config.MIN_RATINGS_FOR_RECOMMENDATION} lượt "
        f"đánh giá trong train+val): {n_eligible}/{catalog_size}"
    )
    restricted_model = restrict_model_to_eligible_items(
        spark, model, eligible, config.OUTPUT_DIR / "model_eligible_export"
    )

    recs = (
        restricted_model.recommendForAllUsers(config.N_RECOMMENDATIONS)
        .select("userId", F.posexplode("recommendations").alias("pos", "rec"))
        .select(
            "userId",
            F.col("rec.movieId").alias("movieId"),
            (F.col("pos") + 1).alias("rank"),
            F.col("rec.rating").cast("double").alias("score"),
        )
    )
    recs.write.mode("overwrite").parquet(str(config.RECS_PARQUET))
    print(f"Ghi Parquet gợi ý: {config.RECS_PARQUET}")

    recs_pdf = spark.read.parquet(str(config.RECS_PARQUET)).toPandas()
    movies_pdf = movies.toPandas()
    ratings_sample_pdf = top_rated_per_user(ratings).toPandas()
    write_sqlite(recs_pdf, movies_pdf, ratings_sample_pdf, config.RECS_SQLITE)
    print(
        f"Ghi SQLite: {config.RECS_SQLITE} "
        f"({len(recs_pdf):,} gợi ý, {len(ratings_sample_pdf):,} rating mẫu)"
    )

    # Item factors cho chức năng "phim tương tự" — chỉ xuất phim đủ điều kiện,
    # nếu không "phim tương tự" sẽ lại trồi lên đúng những phim 1-lượt-đánh-giá mà
    # danh sách gợi ý ở trên đã loại bỏ.
    factors_pdf = restricted_model.itemFactors.toPandas().sort_values("id").reset_index(drop=True)
    matrix = np.array(factors_pdf["features"].tolist(), dtype=np.float32)
    np.save(config.ITEM_FACTORS_NPY, matrix)
    factors_pdf[["id"]].rename(columns={"id": "movieId"}).to_parquet(
        config.ITEM_INDEX_PARQUET, index=False
    )
    print(f"Ghi item factors: {config.ITEM_FACTORS_NPY} shape={matrix.shape}")

    spark.stop()


if __name__ == "__main__":
    main()
