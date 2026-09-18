"""Schema tường minh cho các file CSV của MovieLens.

Khai báo tường minh thay vì inferSchema=True: inferSchema buộc Spark quét
toàn bộ file thêm một lượt chỉ để suy ra kiểu dữ liệu — với ratings.csv 650 MB
đó là một lượt đọc thừa hoàn toàn tránh được.
"""
from pyspark.sql import types as T

RATINGS_SCHEMA = T.StructType([
    T.StructField("userId", T.IntegerType(), True),
    T.StructField("movieId", T.IntegerType(), True),
    T.StructField("rating", T.DoubleType(), True),
    T.StructField("timestamp", T.LongType(), True),
])

MOVIES_SCHEMA = T.StructType([
    T.StructField("movieId", T.IntegerType(), True),
    T.StructField("title", T.StringType(), True),
    T.StructField("genres", T.StringType(), True),
])
