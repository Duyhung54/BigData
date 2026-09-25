"""Truy cập dữ liệu cho tầng serving.

KHÔNG import pyspark. Tầng này chỉ đọc kết quả mà tầng batch đã ghi ra:
mỗi lần khởi tạo SparkSession tốn 10-20 giây, đặt trong đường xử lý request
sẽ làm demo trông như treo.
"""
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd


class Store:
    def __init__(self, db_path: Path, factors_path: Path, index_path: Path):
        self.db_path = Path(db_path)
        self._conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row

        matrix = np.load(factors_path).astype(np.float32)
        norms = np.linalg.norm(matrix, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        self._factors = matrix / norms          # chuẩn hoá sẵn -> cosine là một phép nhân

        movie_ids = pd.read_parquet(index_path)["movieId"].to_numpy()
        self._movie_ids = movie_ids
        self._row_of = {int(mid): row for row, mid in enumerate(movie_ids)}

    def _movie_rows(self, movie_ids):
        if not movie_ids:
            return {}
        placeholders = ",".join("?" for _ in movie_ids)
        rows = self._conn.execute(
            f"SELECT movieId, title, genres FROM movies WHERE movieId IN ({placeholders})",
            list(movie_ids),
        ).fetchall()
        return {row["movieId"]: dict(row) for row in rows}

    def recommendations(self, user_id: int, k: int = 10) -> list:
        rows = self._conn.execute(
            "SELECT movieId, rank, score FROM recommendations "
            "WHERE userId = ? ORDER BY rank LIMIT ?",
            (user_id, k),
        ).fetchall()
        meta = self._movie_rows([row["movieId"] for row in rows])
        return [
            {
                "movieId": row["movieId"],
                "rank": row["rank"],
                "score": round(row["score"], 4),
                "title": meta.get(row["movieId"], {}).get("title", "?"),
                "genres": meta.get(row["movieId"], {}).get("genres", ""),
            }
            for row in rows
        ]

    def history(self, user_id: int, k: int = 10) -> list:
        """Phim user đã chấm điểm cao nhất.

        Đọc từ bảng ratings_sample mà Task 11 ghi kèm (mẫu rating của user),
        để giao diện đặt lịch sử cạnh gợi ý cho người xem tự đối chiếu.
        """
        rows = self._conn.execute(
            "SELECT movieId, rating FROM ratings_sample "
            "WHERE userId = ? ORDER BY rating DESC, movieId LIMIT ?",
            (user_id, k),
        ).fetchall()
        meta = self._movie_rows([row["movieId"] for row in rows])
        return [
            {
                "movieId": row["movieId"],
                "rating": row["rating"],
                "title": meta.get(row["movieId"], {}).get("title", "?"),
                "genres": meta.get(row["movieId"], {}).get("genres", ""),
            }
            for row in rows
        ]

    def similar(self, movie_id: int, k: int = 10) -> list:
        """Cosine similarity trên item factors đã chuẩn hoá.

        16.358 x rank (rank=10, xem report/results/best_params.csv) chỉ vài
        chục MB, nên nhân một vector với cả ma trận mất vài mili giây —
        không cần tiền tính toán ma trận 16.358 x 16.358.
        """
        row = self._row_of.get(int(movie_id))
        if row is None:
            return []
        scores = self._factors @ self._factors[row]
        scores[row] = -np.inf                     # không tự gợi ý chính nó
        # -inf luôn xếp hạng CUỐI trong argsort(-scores), nên khi k < n-1
        # phim tự thân không lọt vào top-k. Nhưng nếu k >= n-1 (không xảy ra
        # hôm nay vì k tối đa 50 << 16.358 phim đủ điều kiện), nó vẫn nằm
        # trong k phần tử đầu — lấy dư một phần tử rồi lọc bỏ chính nó thay
        # vì dựa vào việc nó luôn "rơi ra ngoài" lát cắt.
        top = np.argsort(-scores)[: k + 1]
        top = top[top != row][:k]
        movie_ids = [int(self._movie_ids[i]) for i in top]
        meta = self._movie_rows(movie_ids)
        return [
            {
                "movieId": mid,
                "similarity": round(float(scores[i]), 4),
                "title": meta.get(mid, {}).get("title", "?"),
                "genres": meta.get(mid, {}).get("genres", ""),
            }
            for i, mid in zip(top, movie_ids)
        ]

    def search(self, query: str, limit: int = 10) -> list:
        # LIKE coi % và _ là ký tự đại diện; không escape thì q="%" khớp MỌI
        # tựa phim. Escape bằng '\' (khai báo qua ESCAPE) để chúng được hiểu
        # là ký tự thường trong chuỗi người dùng nhập, không phải wildcard.
        escaped = query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        rows = self._conn.execute(
            "SELECT movieId, title, genres FROM movies WHERE title LIKE ? ESCAPE '\\' "
            "ORDER BY LENGTH(title) LIMIT ?",
            (f"%{escaped}%", limit),
        ).fetchall()
        return [dict(row) for row in rows]

    def stats(self) -> dict:
        n_users = self._conn.execute(
            "SELECT COUNT(DISTINCT userId) FROM recommendations"
        ).fetchone()[0]
        n_movies = self._conn.execute("SELECT COUNT(*) FROM movies").fetchone()[0]
        return {
            "users_with_recommendations": n_users,
            "movies_in_catalog": n_movies,
            "factor_rank": int(self._factors.shape[1]),
        }
