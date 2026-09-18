"""Chia tập train/validation/test theo thời gian trong từng user.

KHÔNG dùng randomSplit: nó cho mô hình nhìn thấy đánh giá tương lai của một
user rồi bắt dự đoán đánh giá quá khứ của chính user đó — rò rỉ dữ liệu, và
làm điểm số đẹp một cách giả tạo.

Phải có tập validation tách khỏi test: nếu chọn siêu tham số trên chính tập
test rồi báo cáo điểm trên tập đó, con số sẽ lạc quan hơn thực tế.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src import config


def add_split_column(
    df: DataFrame,
    train_frac: float = config.SPLIT_TRAIN,
    val_frac: float = config.SPLIT_VAL,
    min_ratings: int = config.MIN_RATINGS_PER_USER,
) -> DataFrame:
    """Thêm cột `split` với giá trị "train" | "val" | "test".

    User có dưới `min_ratings` rating thì không chia được thành ba phần có
    nghĩa — toàn bộ rating của họ vào train và họ bị loại khỏi phần đánh giá.
    """
    # Sắp xếp phụ theo movieId để kết quả tất định khi trùng timestamp
    order = Window.partitionBy("userId").orderBy("timestamp", "movieId")
    per_user = Window.partitionBy("userId")

    ranked = (
        df.withColumn("_pct", F.percent_rank().over(order))
          .withColumn("_n", F.count(F.lit(1)).over(per_user))
    )

    return (
        ranked.withColumn(
            "split",
            F.when(F.col("_n") < min_ratings, F.lit("train"))
             .when(F.col("_pct") < train_frac, F.lit("train"))
             .when(F.col("_pct") < val_frac, F.lit("val"))
             .otherwise(F.lit("test")),
        )
        .drop("_pct", "_n")
    )


def count_excluded_users(df: DataFrame, min_ratings: int = config.MIN_RATINGS_PER_USER) -> int:
    """Số user bị loại khỏi đánh giá vì có quá ít rating.

    Con số này phải nêu trong báo cáo: nó chính là biểu hiện định lượng của
    bài toán cold-start trên bộ dữ liệu.
    """
    return (
        df.groupBy("userId")
          .count()
          .filter(F.col("count") < min_ratings)
          .count()
    )
