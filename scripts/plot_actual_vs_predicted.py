#!/usr/bin/env python
import os
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel
from src import config

def main():
    print("Khởi tạo SparkSession...")
    spark = SparkSession.builder \
        .appName("plot_actual_vs_predicted") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    delta_path = Path(str(config.DELTA_LAKE_PATH))
    
    # Đọc dữ liệu thực tế
    if delta_path.exists() and any(delta_path.iterdir()):
        print(f"Đọc dữ liệu thực tế từ Delta Lake: {config.DELTA_LAKE_PATH}")
        ratings = spark.read.format("delta").load(str(config.DELTA_LAKE_PATH))
    else:
        print(f"Đọc dữ liệu thực tế từ Parquet: {config.RATINGS_PARQUET}")
        ratings = spark.read.parquet(str(config.RATINGS_PARQUET))
        
    print(f"Load mô hình ALS từ: {config.MODEL_DIR}")
    try:
        model = ALSModel.load(str(config.MODEL_DIR))
    except Exception as e:
        print(f"Lỗi: Không tìm thấy model tại {config.MODEL_DIR}. Bạn cần chạy train_als trước! ({e})")
        return

    print("Đang dự đoán điểm số (Transform)...")
    predictions = model.transform(ratings).dropna()
    
    print("Lấy mẫu dữ liệu để vẽ biểu đồ...")
    # Lấy mẫu tối đa 100,000 dòng để vẽ cho nhanh và nhẹ
    total_count = predictions.count()
    fraction = min(1.0, 100000 / max(total_count, 1))
    
    pdf = predictions.select("rating", "prediction").sample(fraction=fraction).toPandas()
    
    print("Vẽ biểu đồ...")
    plt.figure(figsize=(10, 6))
    
    # Vẽ Boxplot: Phân bố điểm dự đoán theo từng mốc điểm thực tế
    sns.boxplot(x='rating', y='prediction', data=pdf, color='lightblue', fliersize=1)
    
    # Vẽ đường chéo lý tưởng y = x (hoặc tương đương vì X là category trong boxplot)
    # Vì trục X của boxplot đánh index từ 0 đến N (0.5, 1.0, 1.5...), ta cần map cho đúng.
    # Các nhãn X: 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0
    plt.plot([-0.5, 9.5], [0.5, 5.0], color='red', linestyle='--', label='Dự đoán hoàn hảo (Lý tưởng)')
    
    plt.title('Rating Thực tế (Delta Lake) vs Rating Dự đoán (ALS Model)')
    plt.xlabel('Điểm số Thực tế (Người dùng chấm)')
    plt.ylabel('Điểm số Dự đoán (ALS)')
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    out_dir = Path(config.APP_ROOT) / "serving" / "static"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "actual_vs_predicted.png"
    
    plt.savefig(str(out_path), dpi=120)
    print(f"✅ Đã lưu biểu đồ thành công tại: {out_path}")
    
    spark.stop()

if __name__ == "__main__":
    main()
