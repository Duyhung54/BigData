"""Tập trung mọi đường dẫn và hằng số cấu hình.

Không hardcode đường dẫn hay ngưỡng ở bất kỳ file nào khác.
"""
import os
from pathlib import Path

DATA_ROOT = Path(os.environ.get("DATA_ROOT", "/opt/data"))
RAW_DIR = DATA_ROOT / "raw"
LAKE_DIR = DATA_ROOT / "lake"
OUTPUT_DIR = DATA_ROOT / "output"
CHECKPOINT_DIR = DATA_ROOT / "checkpoint"
RESULTS_DIR = Path(os.environ.get("RESULTS_DIR", "/opt/app/report/results"))

# Bộ dữ liệu đang dùng: "ml-25m" cho chạy thật, "ml-latest-small" khi phát triển
DATASET = os.environ.get("DATASET", "ml-25m")

RATINGS_CSV = RAW_DIR / DATASET / "ratings.csv"
MOVIES_CSV = RAW_DIR / DATASET / "movies.csv"
RATINGS_PARQUET = LAKE_DIR / "ratings.parquet"
MOVIES_PARQUET = LAKE_DIR / "movies.parquet"

MODEL_DIR = OUTPUT_DIR / "model"

# Khi ALS_FIXED_RANK/ALS_FIXED_REG_PARAM được đặt (thí nghiệm đo scale ở Task
# 11 trong scripts/scaling_experiment.sh), train_als.py không chạy grid search
# thật nên KHÔNG được ghi vào RESULTS_DIR/tuning.csv hay MODEL_DIR — thí
# nghiệm gọi train_als.py 9 lần với cùng một tổ hợp, và ghi đè các đường dẫn
# chính thức sẽ xoá mất lưới tuning 15 dòng đã tốn công chạy trước đó. Xem
# src/jobs/train_als.py:resolve_output_paths().
SCALING_RESULTS_DIR = RESULTS_DIR / "scaling_scratch"
SCALING_MODEL_DIR = OUTPUT_DIR / "model_scaling_scratch"
RECS_PARQUET = OUTPUT_DIR / "recommendations.parquet"
RECS_SQLITE = OUTPUT_DIR / "recs.sqlite"
ITEM_FACTORS_NPY = OUTPUT_DIR / "item_factors.npy"
ITEM_INDEX_PARQUET = OUTPUT_DIR / "item_index.parquet"

# Chia tập theo thời gian trong từng user
SPLIT_TRAIN = 0.70
SPLIT_VAL = 0.85
MIN_RATINGS_PER_USER = 5

# Đánh giá
RELEVANCE_THRESHOLD = 4.0
TOP_K = 10
N_RECOMMENDATIONS = 20

# Chỉ sinh gợi ý từ các phim có ít nhất ngần này lượt đánh giá trong tập huấn luyện.
# Không có ngưỡng này, ALS gợi ý toàn phim có TRUNG VỊ 1 lượt đánh giá: factor của
# chúng ước lượng từ đúng một quan sát nên điểm dự đoán bị đẩy lên cực trị và chiếm
# hết top-10, khiến NDCG@10 rớt xuống 0.0003 so với 0.031 của baseline popularity.
MIN_RATINGS_FOR_RECOMMENDATION = 20

# Lưới siêu tham số
ALS_RANKS = [10, 50, 100]
# regParam kéo tới 0.5 vì lần chạy thử trên ml-latest-small cho 0.2 thắng —
# mà 0.2 là giá trị lớn nhất được thử, tức tối ưu nằm ở BIÊN của lưới và chưa
# kết luận được. Thêm 0.3 và 0.5 để tối ưu nằm hẳn bên trong lưới.
ALS_REG_PARAMS = [0.01, 0.1, 0.2, 0.3, 0.5]
ALS_MAX_ITER = 10
ALS_CHECKPOINT_INTERVAL = 5

# Khi cả hai được đặt, train_als.py bỏ qua grid search và chỉ fit một tổ hợp.
# Thí nghiệm đo scale ở Task 11 cần điều này để phép đo chỉ gồm một lần fit.
ALS_FIXED_RANK = int(os.environ["ALS_RANK"]) if os.environ.get("ALS_RANK") else None
ALS_FIXED_REG_PARAM = float(os.environ["ALS_REG_PARAM"]) if os.environ.get("ALS_REG_PARAM") else None
