"""Job 3: chấm mô hình tốt nhất trên tập TEST, một lần duy nhất.

RMSE đo sai lệch của điểm dự đoán, không đo chất lượng của danh sách gợi ý —
một mô hình RMSE đẹp vẫn có thể gợi ý danh sách vô dụng. Vì vậy báo cáo cần
cả metrics xếp hạng lẫn đối chiếu với baseline.
"""
import csv as csv_module

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALSModel
from pyspark.sql import DataFrame, Window
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


def top_k_from_scores(scored: DataFrame, k: int = config.TOP_K) -> DataFrame:
    """Từ (userId, movieId, prediction) đã chấm điểm, lấy top-k movieId mỗi user.

    Dùng struct(rank, movieId) rồi sort_array thay vì orderBy trước groupBy:
    Spark không đảm bảo thứ tự hàng sống sót qua shuffle của groupBy, nên
    orderBy-rồi-collect_list có thể ra danh sách sai thứ tự một cách âm thầm.
    """
    window = Window.partitionBy("userId").orderBy(F.col("prediction").desc())
    ranked = (
        scored.withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") <= k)
    )
    return (
        ranked.withColumn("pair", F.struct(F.col("rank"), F.col("movieId")))
        .groupBy("userId")
        .agg(F.sort_array(F.collect_list("pair")).alias("pairs"))
        .withColumn("items", F.expr("transform(pairs, x -> x.movieId)"))
        .select("userId", "items")
    )


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

    # --- ALS: dự đoán điểm (không phụ thuộc cách sinh ứng viên gợi ý) ---
    als_preds = model.transform(test)
    als_rating = rating_metrics(als_preds)

    # --- ALS, ứng viên KHÔNG lọc: recommendForUserSubset xét toàn bộ catalog ---
    # Giữ lại làm bằng chứng: đây là con số cho thấy RMSE đẹp không đồng nghĩa
    # danh sách gợi ý tốt — phim 1-lượt-đánh-giá chiếm hết top-K (xem báo cáo).
    als_recs_unfiltered = (
        model.recommendForUserSubset(test_users, k)
        .select("userId", F.col("recommendations.movieId").alias("items"))
    )
    rows.append({
        "model": "als_unfiltered",
        **als_rating,
        **ranking_metrics(als_recs_unfiltered, actual, k),
        "coverage": _coverage_of(als_recs_unfiltered, catalog_size),
    })

    # --- ALS, ứng viên có lọc min-support: chỉ xét phim >= MIN_RATINGS_FOR_RECOMMENDATION
    # lượt đánh giá trong train+val — cùng sân chơi với baseline popularity, vốn
    # chỉ bao giờ gợi ý phim có hàng trăm lượt đánh giá. recommendForUserSubset
    # không có tham số lọc ứng viên, nên ở đây chấm điểm trực tiếp lưới
    # user x phim-đủ-điều-kiện rồi tự xếp hạng — cách này lấy đúng luôn K phim mỗi
    # user (miễn số phim đủ điều kiện >= K), khỏi phải đoán xin dư bao nhiêu rồi lọc.
    eligible = eligible_items(train_val, config.MIN_RATINGS_FOR_RECOMMENDATION).cache()
    n_eligible = eligible.count()
    scored_candidates = model.transform(test_users.crossJoin(eligible))
    als_recs = top_k_from_scores(scored_candidates, k).cache()
    n_short = als_recs.filter(F.size("items") < k).count()
    if n_short:
        print(
            f"CẢNH BÁO: {n_short} user nhận ít hơn K={k} gợi ý sau khi lọc min-support "
            f"(chỉ có {n_eligible} phim đủ điều kiện)."
        )
    rows.append({
        "model": "als",
        **als_rating,
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
    print(
        f"Phim đủ điều kiện gợi ý (>= {config.MIN_RATINGS_FOR_RECOMMENDATION} lượt "
        f"đánh giá trong train+val): {n_eligible}/{catalog_size}"
    )
    for row in rows:
        print(f"  {row['model']}: {row}")

    spark.stop()


if __name__ == "__main__":
    main()
