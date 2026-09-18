"""Job 2: huấn luyện ALS và chọn siêu tham số trên tập validation.

Siêu tham số được chọn trên tập VALIDATION, không phải tập test. Sau khi chốt,
mô hình được huấn luyện lại trên train + validation rồi mới giao cho Job 3
chấm trên test đúng một lần.
"""
import csv as csv_module
import time

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.sql import DataFrame

from src import config
from src.common.split import add_split_column, count_excluded_users
from src.session import get_spark


def build_als(rank: int, reg_param: float, max_iter: int = config.ALS_MAX_ITER) -> ALS:
    """Tạo ALS với hai tham số bắt buộc mà thiếu là hỏng.

    coldStartStrategy="drop": không có nó, user hoặc phim chưa xuất hiện trong
    train cho dự đoán NaN, và RMSE của toàn bộ tập test trở thành NaN.

    checkpointInterval: ALS lặp nhiều vòng sinh lineage RDD rất dài, dẫn tới
    StackOverflowError. Checkpoint định kỳ cắt lineage.
    """
    return ALS(
        rank=rank,
        regParam=reg_param,
        maxIter=max_iter,
        userCol="userId",
        itemCol="movieId",
        ratingCol="rating",
        coldStartStrategy="drop",
        checkpointInterval=config.ALS_CHECKPOINT_INTERVAL,
        nonnegative=False,
        seed=42,
    )


def evaluate_rmse(model: ALSModel, df: DataFrame) -> float:
    predictions = model.transform(df)
    evaluator = RegressionEvaluator(
        metricName="rmse", labelCol="rating", predictionCol="prediction"
    )
    return float(evaluator.evaluate(predictions))


def grid_search(
    train: DataFrame,
    validation: DataFrame,
    ranks: list = None,
    reg_params: list = None,
) -> list:
    """Thử mọi tổ hợp, chấm trên validation, trả về kết quả đã sắp theo RMSE.

    Khi cả config.ALS_FIXED_RANK và config.ALS_FIXED_REG_PARAM được đặt (qua
    biến môi trường ALS_RANK / ALS_REG_PARAM), bỏ qua toàn bộ lưới ranks/
    reg_params và chỉ fit đúng một tổ hợp cố định đó. Thí nghiệm đo scale ở
    Task 11 cần điều này: chạy cả lưới 9 tổ hợp ba lần trên ba cấu hình cụm sẽ
    mất khoảng sáu giờ, và mỗi phép đo thời gian sẽ gộp chín lần fit khác nhau
    thay vì đo một đơn vị công việc lặp lại được — số liệu tăng tốc sẽ vô nghĩa.
    """
    if config.ALS_FIXED_RANK is not None and config.ALS_FIXED_REG_PARAM is not None:
        print(
            f"ALS_RANK và ALS_REG_PARAM đã được đặt qua biến môi trường: "
            f"bỏ qua grid search, chỉ fit rank={config.ALS_FIXED_RANK} "
            f"regParam={config.ALS_FIXED_REG_PARAM}"
        )
        ranks = [config.ALS_FIXED_RANK]
        reg_params = [config.ALS_FIXED_REG_PARAM]
    else:
        ranks = ranks if ranks is not None else config.ALS_RANKS
        reg_params = reg_params if reg_params is not None else config.ALS_REG_PARAMS

    train.cache()
    validation.cache()

    results = []
    for rank in ranks:
        for reg_param in reg_params:
            started = time.perf_counter()
            model = build_als(rank, reg_param).fit(train)
            fit_seconds = time.perf_counter() - started
            rmse = evaluate_rmse(model, validation)
            print(f"rank={rank} regParam={reg_param} -> RMSE={rmse:.4f} ({fit_seconds:.1f}s)")
            results.append({
                "rank": rank,
                "regParam": reg_param,
                "maxIter": config.ALS_MAX_ITER,
                "rmse": round(rmse, 5),
                "fit_seconds": round(fit_seconds, 1),
            })

    return sorted(results, key=lambda row: row["rmse"])


def main() -> None:
    spark = get_spark("train_als")

    ratings = spark.read.parquet(str(config.RATINGS_PARQUET))
    excluded = count_excluded_users(ratings)
    print(f"User bị loại khỏi đánh giá (dưới {config.MIN_RATINGS_PER_USER} rating): {excluded}")

    split = add_split_column(ratings)
    train = split.filter("split = 'train'").select("userId", "movieId", "rating")
    validation = split.filter("split = 'val'").select("userId", "movieId", "rating")

    results = grid_search(train, validation)

    config.RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    tuning_csv = config.RESULTS_DIR / "tuning.csv"
    with open(tuning_csv, "w", newline="", encoding="utf-8") as fh:
        writer = csv_module.DictWriter(fh, fieldnames=list(results[0]))
        writer.writeheader()
        writer.writerows(results)
    print(f"Ghi kết quả tuning: {tuning_csv}")

    best = results[0]
    print(f"Tổ hợp tốt nhất: rank={best['rank']} regParam={best['regParam']} RMSE={best['rmse']}")

    # Huấn luyện lại trên train + validation trước khi giao cho Job 3
    refit_data = train.union(validation)
    final_model = build_als(best["rank"], best["regParam"]).fit(refit_data)
    final_model.write().overwrite().save(str(config.MODEL_DIR))
    print(f"Lưu mô hình: {config.MODEL_DIR}")

    with open(config.RESULTS_DIR / "best_params.csv", "w", newline="", encoding="utf-8") as fh:
        writer = csv_module.DictWriter(fh, fieldnames=list(best) + ["excluded_users"])
        writer.writeheader()
        writer.writerow({**best, "excluded_users": excluded})

    spark.stop()


if __name__ == "__main__":
    main()
