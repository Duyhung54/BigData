import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# Đọc dữ liệu
print("Reading data...")
csv_path = 'E:/BigData/data/raw/ml-latest-small/ratings.csv'
df = pd.read_csv(csv_path)

# Chuyển đổi timestamp sang datetime
df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
df['year'] = df['datetime'].dt.year
df['hour'] = df['datetime'].dt.hour

# Nhóm user thành 2 nhóm: Hardcore (>200 ratings) và Casual (<=200 ratings)
user_counts = df.groupby('userId').size()
hardcore_users = user_counts[user_counts > 200].index
casual_users = user_counts[user_counts <= 200].index

df['user_group'] = df['userId'].apply(lambda x: 'Hardcore (>200 ratings)' if x in hardcore_users else 'Casual (<=200 ratings)')

# Tạo 2 biểu đồ (1 theo Năm, 1 theo Giờ trong ngày)
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

# Biểu đồ 1: Số lượng đánh giá theo Năm
yearly_counts = df.groupby(['year', 'user_group']).size().reset_index(name='count')
sns.lineplot(data=yearly_counts, x='year', y='count', hue='user_group', ax=ax1, marker='o')
ax1.set_title('Xu hướng đánh giá theo Năm (1996 - 2018)')
ax1.set_xlabel('Năm')
ax1.set_ylabel('Tổng số Rating')
ax1.grid(True, alpha=0.3)

# Biểu đồ 2: Hoạt động theo Khung giờ trong ngày
hourly_counts = df.groupby(['hour', 'user_group']).size().reset_index(name='count')
sns.lineplot(data=hourly_counts, x='hour', y='count', hue='user_group', ax=ax2, marker='s')
ax2.set_title('Thói quen đánh giá theo Khung giờ trong ngày (0-23h)')
ax2.set_xlabel('Giờ trong ngày')
ax2.set_ylabel('Tổng số Rating')
ax2.set_xticks(range(0, 24, 2))
ax2.grid(True, alpha=0.3)

plt.tight_layout()

# Lưu biểu đồ vào thư mục static của Web App
out_dir = r"E:\BigData\serving\static"
out_path = Path(out_dir) / "user_trend.png"
plt.savefig(out_path, dpi=120)
print(f"Saved chart to {out_path}")
