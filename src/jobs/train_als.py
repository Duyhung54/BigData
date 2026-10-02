"""Job 2: Huấn luyện Spark MLlib ALS theo luồng Batch (Chu kỳ định kỳ).
Quy trình:
1. Đọc ratings.parquet từ HDFS.
2. Chia Temporal Split 80/10/10 theo timestamp trong từng user để chống rò rỉ dữ liệu.
3. Grid Search siêu tham số (rank=[10, 50], regParam=[0.01, 0.1]).
4. Đánh giá sai số RMSE trên tập Validation và Test.
5. Sinh Top-20 gợi ý cho mọi người dùng và ghi trực tiếp vào PostgreSQL qua JDBC.
"""
import time
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.sql import DataFrame, functions as F

from src import config
from src.common.split import add_split_column, count_excluded_users
from src.session import get_spark


def build_als(rank: int, reg_param: float, max_iter: int = config.ALS_MAX_ITER) -> ALS:
    """Khởi tạo mô hình ALS với các cờ bắt buộc cho Big Data."""
    return ALS(
        rank=rank,
        regParam=reg_param,
        maxIter=max_iter,
        userCol="userId",
        itemCol="movieId",
        ratingCol="rating",
        numUserBlocks=50,
        numItemBlocks=50,
        coldStartStrategy="drop",  # Loại bỏ NaN khi gặp user/item mới
        checkpointInterval=config.ALS_CHECKPOINT_INTERVAL,  # Cắt tỉa RDD Lineage chống StackOverflow
        nonnegative=False,
        seed=42,
    )


def evaluate_rmse(model: ALSModel, df: DataFrame) -> float:
    """Đo sai số RMSE trên tập đánh giá."""
    predictions = model.transform(df)
    evaluator = RegressionEvaluator(
        metricName="rmse", labelCol="rating", predictionCol="prediction"
    )
    return float(evaluator.evaluate(predictions))


def grid_search(train: DataFrame, validation: DataFrame, ranks: list = None, reg_params: list = None) -> list:
    """Chạy lưới tìm kiếm siêu tham số tối ưu trên tập Validation."""
    ranks = ranks if ranks is not None else config.ALS_RANKS
    reg_params = reg_params if reg_params is not None else config.ALS_REG_PARAMS

    train.cache()
    validation.cache()

    results = []
    print(f"\n--- BẮT ĐẦU GRID SEARCH (ranks={ranks}, reg_params={reg_params}) ---")
    for rank in ranks:
        for reg in reg_params:
            t0 = time.perf_counter()
            model = build_als(rank, reg).fit(train)
            dur = time.perf_counter() - t0
            rmse = evaluate_rmse(model, validation)
            print(f"-> Model rank={rank:2d}, regParam={reg:4.2f} | RMSE={rmse:.4f} ({dur:.1f}s)")
            results.append({
                "rank": rank,
                "regParam": reg,
                "rmse": round(rmse, 5),
                "duration": round(dur, 1)
            })

    results.sort(key=lambda x: x["rmse"])
    return results


def export_recommendations_to_postgres(model: ALSModel, num_recs: int = 20) -> None:
    """Sinh Top-20 gợi ý phân tán và ghi vào PostgreSQL qua JDBC."""
    print(f"\n--- Đang sinh Top-{num_recs} gợi ý cho toàn bộ người dùng ---")
    t0 = time.perf_counter()
    user_recs = model.recommendForAllUsers(num_recs)

    # user_recs có schema: userId: int, recommendations: array<struct<movieId: int, rating: float>>
    # Bung mảng (explode) thành các dòng riêng biệt kèm thứ hạng (rank)
    flattened = (
        user_recs
        .select(
            F.col("userId").alias("user_id"),
            F.posexplode("recommendations").alias("rank", "rec")
        )
        .select(
            F.col("user_id"),
            F.col("rec.movieId").alias("movie_id"),
            (F.col("rank") + 1).alias("rank"),
            F.col("rec.rating").alias("rating")
        )
    )

    print(f"-> Sinh xong bảng gợi ý ({time.perf_counter() - t0:.1f}s). Đang ghi vào PostgreSQL...")

    # Ghi vào PostgreSQL qua JDBC driver
    try:
        (
            flattened.write.format("jdbc")
            .option("url", config.POSTGRES_JDBC_URL)
            .option("dbtable", "recommendations")
            .option("user", config.POSTGRES_USER)
            .option("password", config.POSTGRES_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .option("batchsize", "10000")
            .mode("overwrite")
            .save()
        )
        print(f"-> [THÀNH CÔNG] Đã ghi dữ liệu gợi ý vào bảng 'recommendations' trên PostgreSQL.")
    except Exception as e:
        print(f"-> [CẢNH BÁO JDBC] {e}. Đang thực hiện fallback ghi qua psycopg2...")
        try:
            import psycopg2
            import psycopg2.extras
            conn = psycopg2.connect(config.POSTGRES_URI)
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS recommendations (
                    user_id INT,
                    movie_id INT,
                    rank INT,
                    rating FLOAT,
                    PRIMARY KEY (user_id, rank)
                );
            """)
            cur.execute("TRUNCATE TABLE recommendations;")
            conn.commit()

            # Lấy mẫu top gợi ý để ghi fallback
            pdf = flattened.limit(100000).toPandas()
            tuples = [tuple(x) for x in pdf.to_numpy()]
            insert_query = "INSERT INTO recommendations (user_id, movie_id, rank, rating) VALUES %s"
            psycopg2.extras.execute_values(cur, insert_query, tuples)
            conn.commit()
            cur.close()
            conn.close()
            print("-> [THÀNH CÔNG] Fallback ghi PostgreSQL hoàn tất.")
        except Exception as fallback_err:
            print(f"-> [LỖI POSTGRES] {fallback_err}")


def main() -> None:
    spark = get_spark("movielens-batch-train-als")
    try:
        print(f"=== ĐỌC DỮ LIỆU TỪ HDFS: {config.RATINGS_PARQUET} ===")
        ratings = spark.read.parquet(config.RATINGS_PARQUET)
        total_ratings = ratings.count()
        print(f"Tổng số lượt đánh giá: {total_ratings:,}")

        # 1. Phân chia theo chuỗi thời gian (80/10/10)
        print("Đang phân chia tập Temporal Split (80% Train, 10% Val, 10% Test)...")
        ratings_split = add_split_column(
            ratings,
            train_frac=config.SPLIT_TRAIN,
            val_frac=config.SPLIT_VAL,
            min_ratings=config.MIN_RATINGS_PER_USER
        )

        train_df = ratings_split.filter(F.col("split") == "train").select("userId", "movieId", "rating")
        val_df = ratings_split.filter(F.col("split") == "val").select("userId", "movieId", "rating")
        test_df = ratings_split.filter(F.col("split") == "test").select("userId", "movieId", "rating")

        # 2. Grid Search siêu tham số
        results = grid_search(train_df, val_df)
        best = results[0]
        print(f"\n⭐ TỔ HỢP TỐI ƯU NHẤT: rank={best['rank']}, regParam={best['regParam']} với RMSE={best['rmse']:.4f}")

        # 3. Huấn luyện mô hình cuối cùng trên (Train + Validation)
        print("\nHuấn luyện mô hình cuối cùng trên (Train + Validation)...")
        train_val_df = ratings_split.filter(F.col("split").isin(["train", "val"])).select("userId", "movieId", "rating")
        final_model = build_als(best["rank"], best["regParam"]).fit(train_val_df)

        # 4. Đánh giá khách quan lần cuối trên tập Test
        test_rmse = evaluate_rmse(final_model, test_df)
        print(f"⭐ SAI SỐ TEST RMSE CUỐI CÙNG: {test_rmse:.4f}")

        # 5. Lưu mô hình phân tán lên HDFS
        final_model.write().overwrite().save(config.MODEL_DIR)
        print(f"Đã lưu mô hình ALS vào: {config.MODEL_DIR}")

        # 6. Xuất Top-20 gợi ý vào PostgreSQL
        export_recommendations_to_postgres(final_model, num_recs=config.N_RECOMMENDATIONS)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

