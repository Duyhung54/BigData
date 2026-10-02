#!/usr/bin/env python
"""
Script: Sinh dữ liệu mẫu mô phỏng đánh giá mới (Streaming Test Data).

Công dụng:
- Tạo ra file CSV chứa các đánh giá mới với timestamp thực tế hiện tại (năm 2026).
- Hỗ trợ 2 chế độ:
  1. 'bulk': Sinh 5,000 đánh giá ngẫu nhiên cho nhiều user để kiểm thử chịu tải stream.
  2. 'user_demo': Sinh đánh giá có chủ đích cho User 1 để thấy rõ thuật toán ALS thay đổi gợi ý trên Web.
"""

import os
import time
import random
import pandas as pd
from pathlib import Path

DATA_DIR = Path(r"E:\BigData\data\raw")
MOVIES_FILE = DATA_DIR / "ml-latest-small" / "movies.csv"
OUTPUT_FILE = DATA_DIR / "stream_test_data.csv"

def generate_bulk_ratings(n_rows: int = 5000) -> pd.DataFrame:
    """Sinh ngẫu nhiên n_rows lượt rating với timestamp mới nhất (2026)."""
    movies_df = pd.read_csv(MOVIES_FILE)
    movie_ids = movies_df["movieId"].tolist()
    
    current_ts = int(time.time()) # Timestamp thời gian thực hiện tại
    ratings_choices = [1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0]
    
    rows = []
    # User ID từ 1 đến 600 (các user quen thuộc trong hệ thống)
    for i in range(n_rows):
        uid = random.randint(1, 600)
        mid = random.choice(movie_ids)
        score = random.choice(ratings_choices)
        # Giả lập thời gian tăng dần từng giây
        ts = current_ts + i
        rows.append({"userId": uid, "movieId": mid, "rating": score, "timestamp": ts})
        
    return pd.DataFrame(rows)

def generate_targeted_demo(target_user: int = 1) -> pd.DataFrame:
    """Tạo kịch bản đặc biệt: User 1 đánh giá 5.0 cho các phim Hoạt hình/Hài hước
    để giảng viên thấy rõ Model cập nhật sở thích theo thời gian thực."""
    movies_df = pd.read_csv(MOVIES_FILE)
    # Lấy các phim thể loại Animation / Children
    anim_movies = movies_df[movies_df["genres"].str.contains("Animation|Children", na=False)]
    sampled_mids = anim_movies["movieId"].head(25).tolist()
    
    current_ts = int(time.time())
    rows = []
    for i, mid in enumerate(sampled_mids):
        rows.append({
            "userId": target_user,
            "movieId": int(mid),
            "rating": 5.0, # Chấm điểm tuyệt đối
            "timestamp": current_ts + i * 2
        })
    return pd.DataFrame(rows)

def main():
    print("=" * 60)
    print("   CÔNG CỤ TẠO DỮ LIỆU KIỂM THỬ STREAMING (TEST DATA GENERATOR)")
    print("=" * 60)
    
    print(f"Đang đọc danh sách phim từ {MOVIES_FILE}...")
    if not MOVIES_FILE.exists():
        print(f"❌ Không tìm thấy {MOVIES_FILE}!")
        return

    # Sinh 5,000 dòng dữ liệu bulk test
    print("Đang tạo 5,000 dòng đánh giá streaming mới (Timestamp 2026)...")
    bulk_df = generate_bulk_ratings(n_rows=5000)
    
    # Kèm thêm 25 đánh giá 5 sao cho User 1 để dễ demo
    demo_df = generate_targeted_demo(target_user=1)
    
    # Gộp lại và sắp xếp theo timestamp
    final_df = pd.concat([bulk_df, demo_df]).sort_values(by="timestamp").reset_index(drop=True)
    
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    final_df.to_csv(OUTPUT_FILE, index=False)
    
    print(f"\n✅ ĐÃ TẠO THÀNH CÔNG: {OUTPUT_FILE}")
    print(f"📊 Tổng số bản ghi: {len(final_df):,} dòng")
    print(f"⏱️ Khoảng timestamp: {final_df['timestamp'].min()} -> {final_df['timestamp'].max()}")
    print("\nMột vài dòng mẫu:")
    print(final_df.head(5))
    print("=" * 60)

if __name__ == "__main__":
    main()
