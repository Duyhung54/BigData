from pyspark.sql import types as T

from src.common.schema import RATINGS_SCHEMA, MOVIES_SCHEMA
from src.jobs.ingest import read_ratings_csv


def test_ratings_schema_has_exact_types():
    assert RATINGS_SCHEMA == T.StructType([
        T.StructField("userId", T.IntegerType(), True),
        T.StructField("movieId", T.IntegerType(), True),
        T.StructField("rating", T.DoubleType(), True),
        T.StructField("timestamp", T.LongType(), True),
    ])


def test_movies_schema_has_exact_types():
    assert MOVIES_SCHEMA == T.StructType([
        T.StructField("movieId", T.IntegerType(), True),
        T.StructField("title", T.StringType(), True),
        T.StructField("genres", T.StringType(), True),
    ])


def test_read_ratings_csv_applies_schema_not_strings(spark, tmp_path):
    csv = tmp_path / "ratings.csv"
    csv.write_text("userId,movieId,rating,timestamp\n1,10,4.0,1000\n2,20,3.5,2000\n")
    df = read_ratings_csv(spark, str(csv))
    assert df.schema == RATINGS_SCHEMA
    assert df.count() == 2
    assert df.filter("userId = 1").first()["rating"] == 4.0
