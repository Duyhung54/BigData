"""Job 4: Content-Based Filtering & Item-Item Similarity từ Tag Genome.
Quy trình:
1. Đọc genome-scores.parquet từ HDFS (15.58M bản ghi).
2. Xây dựng ma trận đặc trưng phim 1.128 chiều (movie_id x 1128_tags).
3. Tính toán độ tương đồng Cosine Similarity giữa các bộ phim.
4. Lưu Top phim tương tự nhất vào bảng movie_similarity(movie_id1, movie_id2, score) trong PostgreSQL.
"""
import time
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src import config
from src.session import get_spark


def compute_content_based_similarity(spark: SparkSession) -> None:
    print(f"=== ĐỌC GENOME SCORES TỪ HDFS: {config.GENOME_SCORES_PARQUET} ===")
    t0 = time.perf_counter()

    try:
        genome_df = spark.read.parquet(config.GENOME_SCORES_PARQUET)
    except Exception as e:
        print(f"[CẢNH BÁO] Không đọc được Parquet từ HDFS ({e}). Fallback đọc từ CSV...")
        genome_df = spark.read.csv(str(config.GENOME_SCORES_CSV), header=True, inferSchema=True)

    print(f"-> Đã nạp genome-scores ({time.perf_counter() - t0:.2f}s). Đang chuyển đổi ma trận...")

    # Chuyển đổi sang Pandas để tính Cosine Similarity hiệu năng cao bằng NumPy/SciPy
    # Ma trận 13.816 phim x 1.128 tags = 15.58M số thực, chỉ chiếm ~62 MB RAM
    t1 = time.perf_counter()
    pdf = genome_df.select("movieId", "tagId", "relevance").toPandas()
    print(f"-> Nạp vào bộ nhớ Pandas ({time.perf_counter() - t1:.2f}s). Đang pivot ma trận...")

    t2 = time.perf_counter()
    feature_matrix = pdf.pivot(index="movieId", columns="tagId", values="relevance").fillna(0.0)
    movie_ids = feature_matrix.index.to_numpy()
    matrix_values = feature_matrix.to_numpy(dtype=np.float32)

    # Chuẩn hóa L2 norm cho từng vector phim để tính Cosine Similarity qua phép nhân ma trận
    norms = np.linalg.norm(matrix_values, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    normalized_matrix = matrix_values / norms

    print(f"-> Ma trận hoàn tất: {len(movie_ids):,} phim x {normalized_matrix.shape[1]:,} đặc trưng ({time.perf_counter() - t2:.2f}s).")
    print(f"Đang tính toán Cosine Similarity và trích xuất Top-20 phim tương đồng...")

    t3 = time.perf_counter()
    batch_size = 1000
    top_k = 20
    similarity_records = []

    # Tính theo từng khối 1000 phim để tối ưu bộ nhớ đệm
    for start_idx in range(0, len(movie_ids), batch_size):
        end_idx = min(start_idx + batch_size, len(movie_ids))
        sub_matrix = normalized_matrix[start_idx:end_idx]

        # Phép nhân ma trận: sub (B x 1128) x all.T (1128 x N) -> (B x N)
        cosine_sim = np.dot(sub_matrix, normalized_matrix.T)

        for i, row in enumerate(cosine_sim):
            current_movie = int(movie_ids[start_idx + i])
            # Lấy top k+1 (loại chính nó ở vị trí đầu)
            top_indices = np.argpartition(row, -(top_k + 1))[-(top_k + 1):]
            top_indices = top_indices[np.argsort(-row[top_indices])]

            for target_idx in top_indices:
                target_movie = int(movie_ids[target_idx])
                score = float(row[target_idx])
                if current_movie != target_movie and score > 0.1:
                    similarity_records.append((current_movie, target_movie, round(score, 4)))
                    if len(similarity_records) >= top_k:
                        break

    print(f"-> Tính xong {len(similarity_records):,} cặp phim tương đồng ({time.perf_counter() - t3:.2f}s).")

    # Ghi vào PostgreSQL
    print(f"Đang ghi kết quả vào bảng 'movie_similarity' trên PostgreSQL ({config.POSTGRES_URI})...")
    import psycopg2
    import psycopg2.extras

    try:
        conn = psycopg2.connect(config.POSTGRES_URI)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS movie_similarity (
                movie_id1 INT,
                movie_id2 INT,
                score FLOAT,
                PRIMARY KEY (movie_id1, movie_id2)
            );
        """)
        cur.execute("TRUNCATE TABLE movie_similarity;")
        conn.commit()

        insert_query = "INSERT INTO movie_similarity (movie_id1, movie_id2, score) VALUES %s ON CONFLICT DO NOTHING"
        psycopg2.extras.execute_values(cur, insert_query, similarity_records, page_size=10000)
        conn.commit()
        cur.close()
        conn.close()
        print(f"-> [THÀNH CÔNG] Đã ghi thành công {len(similarity_records):,} bản ghi vào PostgreSQL.")
    except Exception as db_err:
        print(f"[LỖI GHI POSTGRESQL] {db_err}")


def main() -> None:
    spark = get_spark("movielens-content-based-tags")
    try:
        compute_content_based_similarity(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

