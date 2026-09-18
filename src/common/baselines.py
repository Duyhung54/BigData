"""Ba baseline để đối chiếu với ALS.

Baseline popularity là cái quan trọng nhất: trong recsys nó mạnh một cách
đáng ngạc nhiên, và một mô hình thua nó là mô hình vô dụng. Nhiều đồ án
hoàn thành rồi mới phát hiện điều này.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def global_mean_predictions(train: DataFrame, target: DataFrame) -> DataFrame:
    """Luôn dự đoán điểm trung bình toàn cục của tập train."""
    mean = train.agg(F.avg("rating")).first()[0]
    return target.withColumn("prediction", F.lit(float(mean)))


def item_mean_predictions(train: DataFrame, target: DataFrame) -> DataFrame:
    """Dự đoán bằng điểm trung bình của chính bộ phim đó.

    Phim chưa từng xuất hiện trong train lấy trung bình toàn cục làm dự phòng
    — nếu để null thì RMSE sẽ ra NaN, đúng cái bẫy mà coldStartStrategy="drop"
    xử lý cho phía ALS.
    """
    global_mean = float(train.agg(F.avg("rating")).first()[0])
    item_means = train.groupBy("movieId").agg(F.avg("rating").alias("_item_mean"))
    return (
        target.join(item_means, on="movieId", how="left")
        .withColumn("prediction", F.coalesce(F.col("_item_mean"), F.lit(global_mean)))
        .drop("_item_mean")
    )


def popularity_top_n(train: DataFrame, n: int) -> list:
    """Top-n phim theo SỐ LƯỢT rating, không theo điểm trung bình.

    Xếp theo điểm trung bình sẽ đẩy lên đầu những phim chỉ có 1-2 lượt đánh
    giá 5 sao — đó không phải là "phổ biến".
    """
    rows = (
        train.groupBy("movieId")
        .agg(F.count(F.lit(1)).alias("_n"))
        .orderBy(F.col("_n").desc(), F.col("movieId").asc())
        .limit(n)
        .collect()
    )
    return [row["movieId"] for row in rows]
