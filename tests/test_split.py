from src.common.split import add_split_column, count_excluded_users

SCHEMA = "userId int, movieId int, rating double, timestamp long"


def test_newest_ratings_go_to_test(spark):
    # user 1 có 10 rating, timestamp tăng dần theo movieId
    rows = [(1, i, 4.0, 1000 + i) for i in range(1, 11)]
    df = spark.createDataFrame(rows, SCHEMA)

    by_movie = {r["movieId"]: r["split"] for r in add_split_column(df).collect()}

    # percent_rank của 10 dòng là 0, 1/9, 2/9, ..., 9/9
    assert by_movie[1] == "train"    # 0.000
    assert by_movie[7] == "train"    # 6/9 = 0.667 < 0.70
    assert by_movie[8] == "val"      # 7/9 = 0.778, trong [0.70, 0.85)
    assert by_movie[9] == "test"     # 8/9 = 0.889 >= 0.85
    assert by_movie[10] == "test"    # 1.000


def test_users_below_min_ratings_all_go_to_train(spark):
    rows = [(2, i, 5.0, 2000 + i) for i in range(1, 4)]  # chỉ 3 rating
    df = spark.createDataFrame(rows, SCHEMA)

    splits = {r["split"] for r in add_split_column(df).collect()}

    assert splits == {"train"}


def test_split_is_per_user_not_global(spark):
    # user 3 rating rất sớm, user 4 rating rất muộn. Nếu chia theo thời gian
    # toàn cục thì toàn bộ user 3 vào train và toàn bộ user 4 vào test.
    rows = [(3, i, 4.0, 100 + i) for i in range(1, 11)]
    rows += [(4, i, 4.0, 900_000 + i) for i in range(1, 11)]
    df = spark.createDataFrame(rows, SCHEMA)

    result = add_split_column(df)

    for user in (3, 4):
        splits = {r["split"] for r in result.filter(f"userId = {user}").collect()}
        assert splits == {"train", "val", "test"}


def test_input_columns_are_preserved(spark):
    df = spark.createDataFrame([(1, i, 4.0, 1000 + i) for i in range(1, 11)], SCHEMA)
    result = add_split_column(df)
    assert set(df.columns).issubset(set(result.columns))
    assert "split" in result.columns


def test_count_excluded_users(spark):
    rows = [(5, i, 4.0, 1000 + i) for i in range(1, 11)]   # đủ điều kiện
    rows += [(6, i, 4.0, 2000 + i) for i in range(1, 3)]   # chỉ 2 rating
    df = spark.createDataFrame(rows, SCHEMA)

    assert count_excluded_users(df, min_ratings=5) == 1
