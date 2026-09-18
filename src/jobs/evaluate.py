"""Job 3: chấm mô hình tốt nhất trên tập TEST, một lần duy nhất.

RMSE đo sai lệch của điểm dự đoán, không đo chất lượng của danh sách gợi ý —
một mô hình RMSE đẹp vẫn có thể gợi ý danh sách vô dụng. Vì vậy báo cáo cần
cả metrics xếp hạng lẫn đối chiếu với baseline.
"""
import csv as csv_module

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALSModel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src import config
from src.common import metrics as M
from src.common.baselines import (
    global_mean_predictions,
    item_mean_predictions,
    popularity_top_n,
)
from src.common.split import add_split_column
from src.session import get_spark


def relevant_items(test: DataFrame, threshold: float = config.RELEVANCE_THRESHOLD) -> DataFrame:
    """Ground truth: các phim user chấm >= ngưỡng.

    User không có phim nào đạt ngưỡng sẽ biến mất khỏi kết quả — đúng như
    mong muốn, vì không có gì để so sánh thì không chấm được.
    """
    return (
        test.filter(F.col("rating") >= threshold)
        .groupBy("userId")
        .agg(F.collect_set("movieId").alias("relevant"))
    )


def rating_metrics(predictions: DataFrame) -> dict:
    result = {}
    for name in ("rmse", "mae"):
        evaluator = RegressionEvaluator(
            metricName=name, labelCol="rating", predictionCol="prediction"
        )
        result[name] = round(float(evaluator.evaluate(predictions)), 5)
    return result


def ranking_metrics(recs: DataFrame, actual: DataFrame, k: int = config.TOP_K) -> dict:
    """Trung bình Precision/Recall/NDCG@k trên các user có ground truth."""
    schema = T.StructType([
        T.StructField("precision", T.DoubleType()),
        T.StructField("recall", T.DoubleType()),
        T.StructField("ndcg", T.DoubleType()),
    ])

    @F.udf(returnType=schema)
    def _row_metrics(items, relevant):
        recommended = list(items or [])
        truth = set(relevant or [])
        return (
            M.precision_at_k(recommended, truth, k),
            M.recall_at_k(recommended, truth, k),
            M.ndcg_at_k(recommended, truth, k),
        )

    scored = recs.join(actual, on="userId", how="inner").withColumn(
        "m", _row_metrics(F.col("items"), F.col("relevant"))
    )
    row = scored.agg(
        F.avg("m.precision").alias("precision_at_k"),
        F.avg("m.recall").alias("recall_at_k"),
        F.avg("m.ndcg").alias("ndcg_at_k"),
    ).first()

    return {
        "precision_at_k": round(float(row["precision_at_k"] or 0.0), 5),
        "recall_at_k": round(float(row["recall_at_k"] or 0.0), 5),
        "ndcg_at_k": round(float(row["ndcg_at_k"] or 0.0), 5),
    }


def _coverage_of(recs: DataFrame, catalog_size: int) -> float:
    distinct_items = (
        recs.select(F.explode("items").alias("movieId")).select("movieId").distinct().count()
    )
    return round(distinct_items / catalog_size, 5) if catalog_size else 0.0


def main() -> None:
    spark = get_spark("evaluate")

    ratings = spark.read.parquet(str(config.RATINGS_PARQUET))
    split = add_split_column(ratings)
    train_val = split.filter("split IN ('train', 'val')").select("userId", "movieId", "rating")
    test = split.filter("split = 'test'").select("userId", "movieId", "rating")

    catalog_size = ratings.select("movieId").distinct().count()
    actual = relevant_items(test).cache()
    test_users = actual.select("userId")
    k = config.TOP_K

    model = ALSModel.load(str(config.MODEL_DIR))
    rows = []

    # --- ALS ---
    als_preds = model.transform(test)
    als_recs = (
        model.recommendForUserSubset(test_users, k)
        .select("userId", F.col("recommendations.movieId").alias("items"))
    )
    rows.append({
        "model": "als",
        **rating_metrics(als_preds),
        **ranking_metrics(als_recs, actual, k),
        "coverage": _coverage_of(als_recs, catalog_size),
    })

    # --- Baseline dự đoán điểm ---
    rows.append({
        "model": "global_mean",
        **rating_metrics(global_mean_predictions(train_val, test)),
        "precision_at_k": None, "recall_at_k": None, "ndcg_at_k": None, "coverage": None,
    })
    rows.append({
        "model": "item_mean",
        **rating_metrics(item_mean_predictions(train_val, test)),
        "precision_at_k": None, "recall_at_k": None, "ndcg_at_k": None, "coverage": None,
    })

    # --- Baseline popularity: cùng một danh sách cho mọi user ---
    top_movies = popularity_top_n(train_val, k)
    popularity_recs = test_users.withColumn(
        "items", F.array(*[F.lit(movie_id) for movie_id in top_movies])
    )
    rows.append({
        "model": "popularity",
        "rmse": None, "mae": None,
        **ranking_metrics(popularity_recs, actual, k),
        "coverage": _coverage_of(popularity_recs, catalog_size),
    })

    config.RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out = config.RESULTS_DIR / "metrics.csv"
    fieldnames = ["model", "rmse", "mae", "precision_at_k", "recall_at_k", "ndcg_at_k", "coverage"]
    with open(out, "w", newline="", encoding="utf-8") as fh:
        writer = csv_module.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})

    print(f"Ghi metrics: {out}  (K={k}, ngưỡng liên quan={config.RELEVANCE_THRESHOLD})")
    for row in rows:
        print(f"  {row['model']}: {row}")

    spark.stop()


if __name__ == "__main__":
    main()
