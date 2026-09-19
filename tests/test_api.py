import sqlite3

import numpy as np
import pandas as pd
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def store_paths(tmp_path):
    db = tmp_path / "recs.sqlite"
    recs = pd.DataFrame({
        "userId": [1, 1, 1, 2],
        "movieId": [10, 11, 12, 10],
        "rank": [1, 2, 3, 1],
        "score": [4.9, 4.5, 4.1, 3.2],
    })
    movies = pd.DataFrame({
        "movieId": [10, 11, 12],
        "title": ["Ma trận (1999)", "Kẻ huỷ diệt (1984)", "Tình yêu (2001)"],
        "genres": ["Action|Sci-Fi", "Action|Sci-Fi", "Romance"],
    })
    # User 1 đã chấm cả ba phim, rating không theo cùng thứ tự với movieId
    # hay với recommendations, để phân biệt được history có thật sự sắp
    # xếp theo rating hay đang vô tình trùng thứ tự của bảng khác.
    ratings_sample = pd.DataFrame({
        "userId": [1, 1, 1],
        "movieId": [12, 10, 11],
        "rating": [5.0, 4.5, 3.0],
    })
    with sqlite3.connect(db) as conn:
        recs.to_sql("recommendations", conn, if_exists="replace", index=False)
        movies.to_sql("movies", conn, if_exists="replace", index=False)
        ratings_sample.to_sql("ratings_sample", conn, if_exists="replace", index=False)
        conn.execute("CREATE INDEX idx_u ON recommendations(userId)")

    # Phim 10 và 11 gần nhau; phim 12 nằm hướng khác
    factors = np.array([[1.0, 0.0], [0.99, 0.1], [0.0, 1.0]], dtype=np.float32)
    factors_path = tmp_path / "item_factors.npy"
    np.save(factors_path, factors)

    index_path = tmp_path / "item_index.parquet"
    pd.DataFrame({"movieId": [10, 11, 12]}).to_parquet(index_path, index=False)

    return db, factors_path, index_path


@pytest.fixture
def client(store_paths, monkeypatch):
    db, factors_path, index_path = store_paths
    monkeypatch.setenv("RECS_SQLITE", str(db))
    monkeypatch.setenv("ITEM_FACTORS_NPY", str(factors_path))
    monkeypatch.setenv("ITEM_INDEX_PARQUET", str(index_path))
    import importlib

    from serving import api

    importlib.reload(api)
    return TestClient(api.app)


def test_health_returns_ok(client):
    assert client.get("/health").json()["status"] == "ok"


def test_recommendations_are_returned_in_rank_order(client):
    body = client.get("/api/users/1/recommendations?k=3").json()

    assert [item["movieId"] for item in body["items"]] == [10, 11, 12]
    assert body["items"][0]["title"] == "Ma trận (1999)"


def test_recommendations_respect_k(client):
    body = client.get("/api/users/1/recommendations?k=2").json()
    assert len(body["items"]) == 2


def test_unknown_user_returns_404(client):
    assert client.get("/api/users/9999/recommendations").status_code == 404


def test_similar_movies_ranked_by_cosine(client):
    body = client.get("/api/movies/10/similar?k=2").json()

    # Phim 11 gần phim 10 hơn phim 12; phim 10 không tự gợi ý chính nó
    assert body["items"][0]["movieId"] == 11
    assert 10 not in [item["movieId"] for item in body["items"]]


def test_similar_unknown_movie_returns_404(client):
    assert client.get("/api/movies/9999/similar").status_code == 404


def test_similar_excludes_self_even_when_k_covers_whole_catalog(client):
    # k=10 > 3 phim đủ điều kiện trong fixture: nếu lát cắt argsort không
    # lọc chính nó ra, phim 10 sẽ xuất hiện trong kết quả của chính nó.
    body = client.get("/api/movies/10/similar?k=10").json()
    assert 10 not in [item["movieId"] for item in body["items"]]
    assert len(body["items"]) == 2


def test_history_returns_highest_rated_first_with_metadata(client):
    body = client.get("/api/users/1/history?k=3").json()

    assert [item["movieId"] for item in body["items"]] == [12, 10, 11]
    assert body["items"][0]["rating"] == 5.0
    assert body["items"][0]["title"] == "Tình yêu (2001)"
    assert body["items"][0]["genres"] == "Romance"


def test_history_respects_k(client):
    body = client.get("/api/users/1/history?k=2").json()
    assert [item["movieId"] for item in body["items"]] == [12, 10]


def test_history_for_unknown_user_is_200_with_empty_items(client):
    """Ghim quyết định: user không có rating là hợp lệ, không phải 404.

    Khác với recommendations/similar (rỗng = không tìm thấy đối tượng =
    lỗi), history rỗng = "user này chưa chấm điểm phim nào" = trạng thái
    hợp lệ. Nếu sau này ai đổi endpoint sang 404, test này báo lỗi thay vì
    để hành vi API âm thầm đổi.
    """
    resp = client.get("/api/users/9999/history")
    assert resp.status_code == 200
    assert resp.json()["items"] == []


def test_search_is_case_insensitive_substring(client):
    body = client.get("/api/movies/search?q=ma tr").json()
    assert 10 in [item["movieId"] for item in body["items"]]


def test_search_empty_query_is_rejected(client):
    assert client.get("/api/movies/search?q=").status_code == 422


def test_search_missing_query_is_rejected(client):
    assert client.get("/api/movies/search").status_code == 422


def test_search_percent_is_treated_literally_not_as_wildcard(client):
    # Không escape thì q="%" khớp MỌI tựa phim (LIKE '%%%' luôn đúng).
    body = client.get("/api/movies/search?q=%25").json()  # %25 = "%" url-encoded
    assert body["items"] == []


def test_k_out_of_range_is_rejected(client):
    assert client.get("/api/users/1/recommendations?k=0").status_code == 422
    assert client.get("/api/users/1/recommendations?k=999").status_code == 422


def test_stats_reports_catalog_size(client):
    body = client.get("/api/stats").json()
    assert body["movies_in_catalog"] == 3
    assert body["factor_rank"] == 2


def test_serving_does_not_import_pyspark(store_paths):
    """Kiểm tra trong TIẾN TRÌNH CON.

    Không kiểm tra được bằng `"pyspark" not in sys.modules` ở đây: conftest.py
    đã nạp pyspark vào tiến trình test từ trước, nên phép kiểm tra đó luôn sai
    bất kể serving/ có sạch hay không.

    Ràng buộc này quan trọng vì image Dockerfile.app không có Java — một câu
    `import pyspark` lọt vào serving/ sẽ làm container chết lúc khởi động.
    """
    import subprocess
    import sys
    import textwrap

    db, factors_path, index_path = store_paths
    script = textwrap.dedent(f"""
        import os, sys
        os.environ["RECS_SQLITE"] = {str(db)!r}
        os.environ["ITEM_FACTORS_NPY"] = {str(factors_path)!r}
        os.environ["ITEM_INDEX_PARQUET"] = {str(index_path)!r}
        import serving.api
        sys.exit(1 if "pyspark" in sys.modules else 0)
    """)
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True)

    assert result.returncode == 0, f"serving/ đã kéo theo pyspark:\n{result.stderr}"


# --- Khởi động khi chưa có dữ liệu (Store phải được tạo lười) ---


def _reload_api_with_paths(monkeypatch, db, factors_path, index_path):
    monkeypatch.setenv("RECS_SQLITE", str(db))
    monkeypatch.setenv("ITEM_FACTORS_NPY", str(factors_path))
    monkeypatch.setenv("ITEM_INDEX_PARQUET", str(index_path))
    import importlib

    from serving import api

    importlib.reload(api)
    return api


def test_health_ok_even_when_data_files_missing(tmp_path, monkeypatch):
    api = _reload_api_with_paths(
        monkeypatch,
        tmp_path / "missing.sqlite",
        tmp_path / "missing.npy",
        tmp_path / "missing.parquet",
    )
    client = TestClient(api.app)

    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_data_endpoints_return_503_when_files_missing(tmp_path, monkeypatch):
    api = _reload_api_with_paths(
        monkeypatch,
        tmp_path / "missing.sqlite",
        tmp_path / "missing.npy",
        tmp_path / "missing.parquet",
    )
    client = TestClient(api.app)

    assert client.get("/api/users/1/recommendations").status_code == 503
    assert client.get("/api/users/1/history").status_code == 503
    assert client.get("/api/movies/10/similar").status_code == 503
    assert client.get("/api/movies/search?q=abc").status_code == 503
    assert client.get("/api/stats").status_code == 503


def test_server_starts_serving_once_files_appear_without_restart(tmp_path, monkeypatch):
    db = tmp_path / "recs.sqlite"
    factors_path = tmp_path / "item_factors.npy"
    index_path = tmp_path / "item_index.parquet"
    api = _reload_api_with_paths(monkeypatch, db, factors_path, index_path)
    client = TestClient(api.app)

    # Chưa có dữ liệu -> 503
    assert client.get("/api/stats").status_code == 503

    # Mô phỏng pipeline batch chạy xong trong khi tiến trình server vẫn sống
    recs = pd.DataFrame({"userId": [1], "movieId": [10], "rank": [1], "score": [4.5]})
    movies = pd.DataFrame({"movieId": [10], "title": ["Phim A"], "genres": ["Drama"]})
    with sqlite3.connect(db) as conn:
        recs.to_sql("recommendations", conn, if_exists="replace", index=False)
        movies.to_sql("movies", conn, if_exists="replace", index=False)
    np.save(factors_path, np.array([[1.0, 0.0]], dtype=np.float32))
    pd.DataFrame({"movieId": [10]}).to_parquet(index_path, index=False)

    # Không reload module, không tạo TestClient mới -> vẫn phục vụ được
    resp = client.get("/api/stats")
    assert resp.status_code == 200
    assert resp.json()["movies_in_catalog"] == 1
