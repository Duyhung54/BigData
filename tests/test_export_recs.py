import sqlite3

import pandas as pd

from src.jobs.export_recs import write_sqlite


def _recs_frame():
    return pd.DataFrame({
        "userId": [1, 1, 2],
        "movieId": [10, 11, 10],
        "rank": [1, 2, 1],
        "score": [4.9, 4.2, 3.8],
    })


def _movies_frame():
    return pd.DataFrame({
        "movieId": [10, 11],
        "title": ["Phim A (1999)", "Phim B (2005)"],
        "genres": ["Action", "Comedy"],
    })


def _ratings_frame():
    return pd.DataFrame({
        "userId": [1, 1, 2],
        "movieId": [10, 11, 10],
        "rating": [5.0, 4.0, 3.0],
    })


def test_write_sqlite_creates_all_three_tables(tmp_path):
    db = tmp_path / "recs.sqlite"

    write_sqlite(_recs_frame(), _movies_frame(), _ratings_frame(), db)

    with sqlite3.connect(db) as conn:
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"recommendations", "movies", "ratings_sample"} <= tables


def test_write_sqlite_indexes_both_user_id_columns(tmp_path):
    db = tmp_path / "recs.sqlite"

    write_sqlite(_recs_frame(), _movies_frame(), _ratings_frame(), db)

    with sqlite3.connect(db) as conn:
        indexes = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='index'")}
    assert "idx_recommendations_userId" in indexes
    assert "idx_ratings_sample_userId" in indexes


def test_recommendations_are_queryable_in_rank_order(tmp_path):
    db = tmp_path / "recs.sqlite"

    write_sqlite(_recs_frame(), _movies_frame(), _ratings_frame(), db)

    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            "SELECT movieId FROM recommendations WHERE userId = 1 ORDER BY rank"
        ).fetchall()
    assert [r[0] for r in rows] == [10, 11]


def test_ratings_sample_is_queryable_by_user(tmp_path):
    db = tmp_path / "recs.sqlite"

    write_sqlite(_recs_frame(), _movies_frame(), _ratings_frame(), db)

    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            "SELECT movieId FROM ratings_sample WHERE userId = 1 ORDER BY rating DESC"
        ).fetchall()
    assert [r[0] for r in rows] == [10, 11]


def test_write_sqlite_is_idempotent(tmp_path):
    db = tmp_path / "recs.sqlite"

    write_sqlite(_recs_frame(), _movies_frame(), _ratings_frame(), db)
    write_sqlite(_recs_frame(), _movies_frame(), _ratings_frame(), db)

    with sqlite3.connect(db) as conn:
        count = conn.execute("SELECT COUNT(*) FROM recommendations").fetchone()[0]
    assert count == 3
