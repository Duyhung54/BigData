"""FastAPI phục vụ gợi ý. Không import pyspark.

Store được nạp LƯỜI (lazy): tầng serving phải khởi động được ngay cả khi
tầng batch (Task 1-8) chưa chạy lần nào, vì `docker compose up` khởi động
`app` trước khi ai đó chạy pipeline. Khởi tạo Store ở cấp module sẽ mở
SQLite `mode=ro` ngay lúc import — nếu recs.sqlite chưa tồn tại, import
ném lỗi và container "app" crash-loop mãi mãi, kể cả khi lát nữa dữ liệu
sẽ có. Thay vào đó, mỗi request tự hỏi get_store(): nếu file chưa có thì
trả 503 kèm thông điệp rõ ràng (Task 10 hiển thị thông điệp này); nếu có
thì nạp Store một lần và giữ lại cho các request sau — nên khi pipeline
chạy xong TRONG LÚC server đang sống, request kế tiếp tự phục vụ được mà
không cần khởi động lại.
"""
import os
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from serving.store import Store

DATA_ROOT = Path(os.environ.get("DATA_ROOT", "/opt/data"))
RECS_SQLITE = Path(os.environ.get("RECS_SQLITE", DATA_ROOT / "output" / "recs.sqlite"))
ITEM_FACTORS_NPY = Path(os.environ.get("ITEM_FACTORS_NPY", DATA_ROOT / "output" / "item_factors.npy"))
ITEM_INDEX_PARQUET = Path(os.environ.get("ITEM_INDEX_PARQUET", DATA_ROOT / "output" / "item_index.parquet"))
STATIC_DIR = Path(__file__).parent / "static"

app = FastAPI(title="MovieLens ALS Recommender")

_store: Optional[Store] = None


def get_store() -> Store:
    """Trả về Store đã nạp, nạp lần đầu khi được gọi (không nạp lúc import).

    Raise 503 nếu file batch chưa sinh ra thay vì để lỗi mở file rò rỉ thành
    500 khó hiểu.
    """
    global _store
    if _store is None:
        missing = [
            str(p) for p in (RECS_SQLITE, ITEM_FACTORS_NPY, ITEM_INDEX_PARQUET) if not p.exists()
        ]
        if missing:
            raise HTTPException(
                status_code=503,
                detail=(
                    "Dữ liệu gợi ý chưa sẵn sàng, còn thiếu: "
                    + ", ".join(missing)
                    + ". Hãy chạy pipeline batch (ingest -> train_als -> evaluate -> export_recs) trước."
                ),
            )
        _store = Store(RECS_SQLITE, ITEM_FACTORS_NPY, ITEM_INDEX_PARQUET)
    return _store


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/users/{user_id}/recommendations")
def recommendations(user_id: int, k: int = Query(10, ge=1, le=20), store: Store = Depends(get_store)):
    items = store.recommendations(user_id, k)
    if not items:
        raise HTTPException(status_code=404, detail=f"Không có gợi ý cho user {user_id}")
    return {"userId": user_id, "items": items}


@app.get("/api/users/{user_id}/history")
def history(user_id: int, k: int = Query(10, ge=1, le=50), store: Store = Depends(get_store)):
    """Trả 200 kèm items rỗng khi user không có rating nào, KHÔNG trả 404.

    Cố ý khác `recommendations`/`similar`, nơi rỗng nghĩa là "không tìm
    thấy đối tượng" nên là lỗi. Ở đây user tồn tại nhưng chưa có/không có
    rating đạt ngưỡng liên quan là một trạng thái hợp lệ, không phải lỗi —
    và giao diện T10 đặt history cạnh recommendations, nơi một cột rỗng
    hiển thị bình thường còn 404 sẽ cần xử lý riêng.
    """
    return {"userId": user_id, "items": store.history(user_id, k)}


@app.get("/api/movies/{movie_id}/similar")
def similar(movie_id: int, k: int = Query(10, ge=1, le=50), store: Store = Depends(get_store)):
    items = store.similar(movie_id, k)
    if not items:
        raise HTTPException(status_code=404, detail=f"Không tìm thấy phim {movie_id}")
    return {"movieId": movie_id, "items": items}


@app.get("/api/movies/search")
def search(q: str = Query(..., min_length=1), limit: int = Query(10, ge=1, le=50), store: Store = Depends(get_store)):
    return {"query": q, "items": store.search(q, limit)}


@app.get("/api/stats")
def stats(store: Store = Depends(get_store)):
    return store.stats()


if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    @app.get("/")
    def index():
        return FileResponse(STATIC_DIR / "index.html")
