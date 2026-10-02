#!/usr/bin/env python
"""
CÔNG CỤ MÔ PHỎNG DÒNG DỮ LIỆU STREAMING THỰC TẾ (REAL-TIME STREAM SIMULATOR)

Điểm nâng cấp so với bản cũ:
1. KHÔNG đọc toàn bộ file vào RAM cùng một lúc (Batch IO).
2. Đọc file theo dạng DÒNG CHẢY STREAMING (Python Streaming Generator / Chunk).
3. Hỗ trợ 2 chế độ phát:
   - Chế độ 1 (File Replay): Đọc và phát tuần tự từ file CSV theo nhịp độ thời gian thực (tùy chỉnh tốc độ: 10, 50, 100, 500 events/giây hoặc burst tối đa).
   - Chế độ 2 (Live Generator): Liên tục sinh luồng đánh giá mới ngẫu nhiên 24/7 theo thời gian thực (như người dùng đang lướt web thật).
"""

import os
import sys
import time
import json
import csv
import random
from pathlib import Path
from kafka import KafkaProducer

# Cấu hình Kafka (Gọi từ Windows host cổng 29092)
KAFKA_SERVER = "localhost:29092"
KAFKA_TOPIC = "ratings"

DEFAULT_FILE = r"E:\BigData\data\raw\stream_test_data.csv"
FALLBACK_FILE = r"E:\BigData\data\raw\ml-latest-small\ratings.csv"

def get_producer() -> KafkaProducer:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks=1, # Báo nhận nhanh
            linger_ms=10,
            batch_size=16384
        )
        return producer
    except Exception as e:
        print(f"❌ Không kết nối được Kafka tại {KAFKA_SERVER}: {e}")
        print("💡 Hãy chắc chắn cụm Docker Kafka đang chạy (docker ps).")
        sys.exit(1)

def stream_from_file(csv_path: str, rate_per_sec: int = 50, use_current_timestamp: bool = True):
    """Đọc file CSV dạng STREAMING GENERATOR từng dòng một và đẩy vào Kafka."""
    producer = get_producer()
    delay_str = f"delay {1.0/rate_per_sec:.3f}s" if rate_per_sec > 0 else "Không độ trễ (Tối đa)"
    print(f"\n🌊 ĐANG ĐỌC STREAMING TỪ FILE: {csv_path}")
    print(f"⚡ Tốc độ phát: {rate_per_sec if rate_per_sec > 0 else 'Cực đại'} đánh giá / giây ({delay_str})")
    print(f"⏰ Cập nhật timestamp: {'Thời gian thực (Now)' if use_current_timestamp else 'Giữ nguyên gốc'}")
    print("👉 Nhấn Ctrl+C để dừng luồng stream bất kỳ lúc nào.\n")

    sleep_interval = 1.0 / rate_per_sec if rate_per_sec > 0 else 0
    count = 0
    start_time = time.time()

    with open(csv_path, mode="r", encoding="utf-8") as fh:
        # Sử dụng DictReader để đọc stream từng dòng, không ngốn RAM
        reader = csv.DictReader(fh)
        for row in reader:
            now_ts = int(time.time()) if use_current_timestamp else int(float(row.get("timestamp", time.time())))
            
            payload = {
                "userId": int(row["userId"]),
                "movieId": int(row["movieId"]),
                "rating": float(row["rating"]),
                "timestamp": now_ts
            }

            producer.send(KAFKA_TOPIC, payload)
            count += 1

            if count % 500 == 0 or count == 1:
                elapsed = time.time() - start_time
                actual_rate = count / elapsed if elapsed > 0 else 0
                print(f"  [STREAMING] Đã đẩy {count:,} ratings | Tốc độ thực tế: {actual_rate:.1f} ratings/s", flush=True)

            if sleep_interval > 0:
                time.sleep(sleep_interval)

    producer.flush()
    total_time = time.time() - start_time
    print(f"\n✅ HOÀN TẤT STREAM FILE! Đã đẩy {count:,} tin nhắn vào Kafka trong {total_time:.2f} giây.", flush=True)

def live_infinite_stream(rate_per_sec: int = 10):
    """Chế độ Live Stream: Tự sinh sự kiện thời gian thực liên tục 24/7."""
    producer = get_producer()
    print(f"\n📡 KHỞI ĐỘNG CHẾ ĐỘ LIVE STREAM LIÊN TỤC 24/7")
    print(f"⚡ Tốc độ: ~{rate_per_sec} lượt đánh giá/giây")
    print("👉 Nhấn Ctrl+C để dừng mô phỏng.\n")

    ratings_choices = [1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0]
    # Danh sách các movieId phổ biến
    popular_movies = [1, 2, 3, 6, 10, 32, 47, 50, 110, 260, 296, 318, 356, 480, 527, 589, 593, 2571, 2959]
    
    sleep_interval = 1.0 / rate_per_sec
    count = 0
    start_time = time.time()

    try:
        while True:
            payload = {
                "userId": random.randint(1, 600),
                "movieId": random.choice(popular_movies) if random.random() < 0.7 else random.randint(1, 5000),
                "rating": random.choice(ratings_choices),
                "timestamp": int(time.time())
            }
            producer.send(KAFKA_TOPIC, payload)
            count += 1

            if count % 100 == 0:
                elapsed = time.time() - start_time
                print(f"  [LIVE STREAM] Đã gửi {count:,} lượt đánh giá | Running: {elapsed:.1f}s")

            time.sleep(sleep_interval)
    except KeyboardInterrupt:
        producer.flush()
        print(f"\n🛑 ĐÃ DỪNG LIVE STREAM. Tổng số sự kiện phát sinh: {count:,}")

def main():
    print("=" * 65)
    print("      BỘ PHÁT DÒNG DỮ LIỆU STREAMING THỰC TẾ (KAFKA PRODUCER)")
    print("=" * 65)
    print("Chọn chế độ mô phỏng:")
    print("  1. Stream tuần tự từ File CSV theo nhịp điệu (50 ratings/giây)")
    print("  2. Stream tốc độ cao từ File CSV (500 ratings/giây)")
    print("  3. Stream tốc độ cực đại (Xả tức thì không giới hạn - Bulk Burst)")
    print("  4. Live Stream liên tục 24/7 (Sinh sự kiện trực tiếp 10 ratings/giây)")
    print("=" * 65)

    choice = sys.argv[1] if len(sys.argv) > 1 else None
    
    # Mặc định lấy file test nếu có, ngược lại lấy fallback
    target_csv = DEFAULT_FILE if os.path.exists(DEFAULT_FILE) else FALLBACK_FILE
    if len(sys.argv) > 2 and os.path.exists(sys.argv[2]):
        target_csv = sys.argv[2]

    if not choice:
        try:
            choice = input("Nhập lựa chọn của bạn (1/2/3/4) [Mặc định: 1]: ").strip()
            if not choice:
                choice = "1"
        except (EOFError, KeyboardInterrupt):
            choice = "1"

    if choice == "1":
        stream_from_file(target_csv, rate_per_sec=50, use_current_timestamp=True)
    elif choice == "2":
        stream_from_file(target_csv, rate_per_sec=500, use_current_timestamp=True)
    elif choice == "3":
        stream_from_file(target_csv, rate_per_sec=0, use_current_timestamp=True)
    elif choice == "4":
        live_infinite_stream(rate_per_sec=10)
    else:
        print("Lựa chọn không hợp lệ. Mặc định chạy Chế độ 1.")
        stream_from_file(target_csv, rate_per_sec=50, use_current_timestamp=True)

if __name__ == "__main__":
    main()
