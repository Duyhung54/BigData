"""Schema tường minh cho toàn bộ 6 file CSV của MovieLens.
Khai báo tường minh thay vì inferSchema=True: tránh việc Spark phải quét toàn bộ
file thêm một lượt, tối ưu thời gian đọc dữ liệu quy mô hàng chục triệu dòng.
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

TAGS_SCHEMA = T.StructType([
    T.StructField("userId", T.IntegerType(), True),
    T.StructField("movieId", T.IntegerType(), True),
    T.StructField("tag", T.StringType(), True),
    T.StructField("timestamp", T.LongType(), True),
])

LINKS_SCHEMA = T.StructType([
    T.StructField("movieId", T.IntegerType(), True),
    T.StructField("imdbId", T.StringType(), True),
    T.StructField("tmdbId", T.StringType(), True),
])

GENOME_SCORES_SCHEMA = T.StructType([
    T.StructField("movieId", T.IntegerType(), True),
    T.StructField("tagId", T.IntegerType(), True),
    T.StructField("relevance", T.DoubleType(), True),
])

GENOME_TAGS_SCHEMA = T.StructType([
    T.StructField("tagId", T.IntegerType(), True),
    T.StructField("tag", T.StringType(), True),
])
