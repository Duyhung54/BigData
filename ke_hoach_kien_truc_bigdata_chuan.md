# KẾ HOẠCH THIẾT KẾ VÀ TRIỂN KHAI HỆ THỐNG BIG DATA
## Đề tài: Hệ Thống Lưu Trữ, Phân Tích Dữ Liệu Lớn và Gợi Ý Phim Phân Tán
### Học phần: BDA501 - Big Data Analytics (HDFS 3 DataNodes & Apache Spark SQL)
# KẾ HOẠCH THIẾT KẾ VÀ XÂY DỰNG HỆ THỐNG BIG DATA
## Đề tài: Hệ Thống Phân Tích Dữ Liệu Lớn & Gợi Ý Phim Phân Tán
### Học phần: BDA501 - Big Data Analytics

---

## 1. MỤC TIÊU VÀ PHẠM VI HỆ THỐNG
## 1. MỤC ĐÍCH & MỤC TIÊU MONG MUỐN CỦA HỆ THỐNG

### 1.1 Trọng tâm học phần
Trọng tâm cốt lõi của đề tài bám sát các chuẩn mực kiến trúc đã được học trong học phần BDA501:
* **Lưu trữ phân tán chịu lỗi**: Triển khai cụm **Apache HDFS đa node (1 NameNode + 3 DataNodes)** với cơ chế nhân bản `Replication Factor = 3`, quản lý khối `Block Size = 128 MB`, tối ưu hóa vị trí dữ liệu (Data Locality) và giám sát qua Web UI cổng `9870`.
* **Tổ chức dữ liệu Data Lake**: Chuyển đổi dữ liệu thô sang định dạng tệp cột **Apache Parquet nén Snappy**, áp dụng kỹ thuật đóng gói tệp nhỏ (Small Files Packing) và phân vùng dữ liệu (Partitioning) tương tự các bài tập thực hành `exe1` và `exe2`.
* **Phân tích dữ liệu phân tán (Distributed Data Analytics / EDA)**: Đặt trọng tâm hàng đầu vào việc dùng **Spark SQL và DataFrame API** để xử lý song song, tính toán các chỉ số thống kê trên hơn **41.7 triệu bản ghi**, phân tích hiện tượng đuôi dài, độ thưa ma trận và xu hướng người dùng.
* **Mô-đun ứng dụng gợi ý (Downstream Application)**: Ứng dụng thuật toán phân tán Spark MLlib ALS và Content-Based Filtering để chứng minh năng lực phục vụ của dữ liệu sau khi được phân tích.
### 1.1 Chúng ta muốn xây dựng hệ thống gì?
Chúng ta muốn xây dựng một **Hệ Thống Phân Tích Dữ Liệu Lớn & Gợi Ý Phim Cá Nhân Hóa (Big Data Analytics & Movie Recommendation System)** hoạt động trên nền tảng tính toán và lưu trữ phân tán đa máy chủ (Multi-Node Cluster).

Hệ thống được thiết kế để giải quyết bài toán: Tiếp nhận một khối lượng dữ liệu khổng lồ về lịch sử tương tác, đánh giá, danh mục phim và các thuộc tính ngữ nghĩa điện ảnh của hàng trăm nghìn người dùng, sau đó xử lý phân tán để biến dữ liệu thô thành thông tin hữu ích và dịch vụ gợi ý thông minh cho người xem.

---

## 2. QUY TRÌNH BIG DATA: NỀN TẢNG HIỆN ĐẠI KẾT HỢP HẠ TẦNG CHUẨN MỰC
### 1.2 Dữ liệu sau khi qua hệ thống Big Data xử lý sẽ làm được gì?
Dữ liệu thô ban đầu chỉ là các tệp văn bản CSV rời rạc chứa các con số và chuỗi ký tự. Sau khi được đưa qua hệ thống Big Data xử lý phân tán, dữ liệu sẽ tạo ra **4 giá trị đầu ra cụ thể**:

Trong hệ thống này, quy trình xử lý tuân thủ mô hình **ELT (Extract - Load - Transform)** của hệ sinh thái Big Data hiện đại:
#### 1. Báo cáo phân tích chuyên sâu hành vi điện ảnh (Big Data Analytics & BI)
* **Khám phá xu hướng cộng đồng**: Phân tích sự chuyển dịch thị hiếu xem phim qua các thập kỷ (từ năm 1995 đến 2019), xác định các thể loại phim bùng nổ và các giai đoạn hoàng kim của từng dòng phim.
* **Thấu hiểu hành vi người dùng**: Nhận diện và phân nhóm người xem thành các phân khúc: người dùng tích cực (Power Users đánh giá hàng nghìn phim), người dùng mới (Cold-Start Users ít tương tác), xu hướng chấm điểm (người xem dễ tính thích cho 4-5 sao vs người xem khắt khe).
* **Nhận diện hiện tượng Đuôi dài (Long-Tail Distribution)**: Định lượng chính xác tỷ lệ tập trung tương tác (ví dụ: 20% phim bom tấn thu hút trên 80% lượt xem), chỉ ra các bộ phim ngách ít người biết nhưng chất lượng cao.

```
┌────────────────────────────────────────────────────────────────────────┐
│ 1. INGESTION THÔ: Tải toàn bộ 6 tệp MovieLens 25M vào HDFS Raw Zone   │
│    (hdfs://namenode:9000/movielens/raw/, Replication = 3)              │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│ 2. DATA LAKE CHUẨN HÓA: Chuyển đổi sang Parquet Snappy (128 MB blocks) │
│    (Column Pruning, Predicate Pushdown, Tránh lỗi Small Files)         │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│ 3. DISTRIBUTED EDA: Phân tích phân tán quy mô lớn bằng Spark SQL       │
│    (Chạy song song trên các Worker, tối ưu hóa qua Catalyst Engine)    │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│ 4. MÔ-ĐUN ỨNG DỤNG & PHỤC VỤ: Spark MLlib ALS & Báo cáo trực quan      │
│    (Serving qua SQLite/FastAPI và trích xuất biểu đồ phân tích)        │
└────────────────────────────────────────────────────────────────────────┘
```
#### 2. Dữ liệu sạch làm đầu vào huấn luyện mô hình trí tuệ nhân tạo (Clean Data for AI/ML)
* Toàn bộ 25 triệu dòng đánh giá sau khi được làm sạch, kiểm tra tính hợp lệ và chuẩn hóa dạng cột Parquet sẽ được dùng để huấn luyện mô hình **Phân rã ma trận xấp xỉ hạng thấp (Spark MLlib ALS)**.
* Kết quả học máy sẽ phân giải ma trận thưa 99.74% thành hai ma trận nhân tử ẩn:
  * **Hồ sơ sở thích người dùng (User Factors)**: Vector số hóa gu điện ảnh của từng người trong không gian ẩn (ví dụ: thích phim hành động pha hài hước, viễn tưởng logic cao).
  * **Đặc trưng bản chất của phim (Item Factors)**: Vector số hóa các đặc tính ẩn của từng bộ phim trong danh mục.

* **Về mặt công nghệ**: Kết hợp giữa **hạ tầng lưu trữ phân tán chuẩn mực của Hadoop (HDFS đa node)** (Session 1, 2) và **công nghệ tính toán bộ nhớ phân tán hiện đại của Apache Spark (Spark SQL)** (Session 7, 8, 9, 10).
* **Lý do nạp dữ liệu lên HDFS trước khi phân tích**: Toàn bộ tập dữ liệu thô gồm hơn 41.7 triệu dòng không thể nạp vừa RAM của một máy đơn để phân tích bằng các công cụ thông thường (như Pandas/Excel). Dữ liệu bắt buộc phải được lưu trữ trên hệ thống tệp phân tán HDFS để các Spark Worker quét đọc song song, tính toán các phép tổng hợp lớn và chỉ xuất bảng kết quả tổng hợp nhỏ về vẽ biểu đồ.
#### 3. Giải quyết bài toán gợi ý phim mới chưa có đánh giá (Item Cold-Start Solution)
* Khai thác ma trận **Tag Genome (15.58 triệu điểm số liên quan)**: Số hóa 13.816 bộ phim thành các vector đặc trưng nội dung 1.128 chiều.
* Khi một bộ phim mới được nhập vào hệ thống và chưa có bất kỳ ai xem hay chấm điểm, hệ thống dùng dữ liệu này để tính toán độ tương đồng nội dung (Content-Based Similarity), từ đó gợi ý phim mới đến đúng những khán giả thích phong cách tương tự mà không cần chờ thu thập phản hồi cộng đồng.

---
#### 4. Cung cấp dịch vụ gợi ý phim cá nhân hóa theo thời gian thực (Serving API & Dashboard)
* **Đối với người dùng xem phim**: Khi đăng nhập vào ứng dụng, hệ thống lập tức hiển thị danh sách **Top-N phim gợi ý riêng biệt** phù hợp nhất với gu của họ, cùng danh sách các phim tương tự với bộ phim họ vừa xem.
* **Đối với nhà quản trị**: Cung cấp giao diện Dashboard trực quan hiển thị toàn bộ biểu đồ phân tích thị trường, số lượng tương tác, độ phủ danh mục và tốc độ phản hồi hệ thống.

## 3. THIẾT KẾ KIẾN TRÚC HẠ TẦNG PHÂN TÁN (HDFS 3 DATANODES & SPARK)

Hạ tầng hệ thống được kế thừa trực tiếp từ kiến trúc bài tập `exe1/hdfs-cluster` và `exe2`:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       CỤM HỆ THỐNG LƯU TRỮ PHÂN TÁN HDFS                    │
│                                                                             │
│  [NameNode (Master)] ── Cổng Web UI: 9870 ── Cổng IPC: 9000                 │
│         │                                                                   │
│         ├── [DataNode 1] (Cổng 9864) ── Khối 128 MB, Bản sao 1             │
│         ├── [DataNode 2] (Cổng 9865) ── Khối 128 MB, Bản sao 2             │
│         └── [DataNode 3] (Cổng 9866) ── Khối 128 MB, Bản sao 3             │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │ hdfs://namenode:9000/
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      CỤM TÍNH TOÁN PHÂN TÁN APACHE SPARK                    │
│                                                                             │
│  [Spark Master] ── Cổng Web UI: 8080 ── Cổng RPC: 7077                      │
│         │                                                                   │
│         ├── [Spark Worker 1] ── 3 Cores CPU, 2.5 GB RAM                     │
│         └── [Spark Worker 2] ── 3 Cores CPU, 2.5 GB RAM                     │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Thông số kỹ thuật của cụm:
* **HDFS Block Size**: `134217728` bytes (128 MB).
* **Replication Factor**: `3` (Đảm bảo an toàn dữ liệu khi có tối đa 2 DataNode gặp sự cố).
* **Kiểm tra trạng thái cụm**: Dùng lệnh `hdfs dfsadmin -report` và `hdfs fsck / -files -blocks -locations`.

---

## 4. LỘ TRÌNH THỰC HIỆN CHI TIẾT (5 BƯỚC)
### 1.3 Tiêu chí đánh giá thành công của hệ thống (Success Criteria)
1. **Tiêu chí Lưu trữ phân tán**: Dữ liệu được lưu trữ phân tán an toàn trên cụm **HDFS đa node** với cơ chế nhân bản `Replication Factor = 3`, không bị mất dữ liệu khi 1 DataNode gặp sự cố.
2. **Tiêu chí Xử lý phân tán**: Các tác vụ phân tích (EDA) và huấn luyện mô hình (ALS) phải chạy song song trên cụm **Apache Spark (nhiều Worker)**, chứng minh được thời gian xử lý nhanh hơn rõ rệt so với chạy trên 1 máy đơn.
3. **Tiêu chí Chất lượng gợi ý**: Mô hình gợi ý vượt trội hơn hẳn các mô hình cơ sở (Popularity, Mean Baselines) về các chỉ số xếp hạng Top-N: NDCG@10, Precision@10 và Độ bao phủ danh mục (Coverage).
4. **Tiêu chí Phục vụ (Serving Latency)**: Tầng API phản hồi danh sách gợi ý cho người dùng cuối trong thời gian **dưới 20 mili-giây**.

---

### BƯỚC 1: KHỞI TẠO CỤM HDFS VÀ INGESTION DỮ LIỆU THÔ (RAW ZONE)
## 2. ĐẶC TẢ DỮ LIỆU ĐẦU VÀO VÀ TẠI SAO CẦN HỆ THỐNG BIG DATA?

1. **Khởi động cụm HDFS đa node**:
   * Dựng các container `namenode`, `datanode1`, `datanode2`, `datanode3` bằng Docker Compose.
   * Tạo cấu trúc không gian tên trên HDFS:
     ```bash
     hdfs dfs -mkdir -p /movielens/raw
     hdfs dfs -mkdir -p /movielens/lake
     hdfs dfs -mkdir -p /movielens/results
     ```
2. **Nạp dữ liệu thô vào HDFS**:
   * Nạp toàn bộ 6 tệp CSV từ máy local lên `hdfs://namenode:9000/movielens/raw/`.
   * Kiểm tra tính toàn vẹn và sự phân bổ các block trên 3 DataNode:
     ```bash
     hdfs dfsadmin -report
     hdfs fsck /movielens/raw -files -blocks -locations
     ```
Hệ thống tiếp nhận bộ dữ liệu chuẩn **MovieLens 25M** gồm 6 tệp dữ liệu với tổng quy mô hơn **41.7 triệu bản ghi**:

---
| Tên tệp dữ liệu | Số lượng bản ghi | Dung lượng thô | Vai trò trong hệ thống |
| :--- | :--- | :--- | :--- |
| **`ratings.csv`** | 25.000.095 dòng | 678.26 MB | Lịch sử tương tác người dùng - phim, đầu vào cho Collaborative Filtering |
| **`genome-scores.csv`** | 15.584.448 dòng | 435.16 MB | Ma trận liên quan ngữ nghĩa (13.816 phim × 1.128 thẻ đặc trưng) cho Content-Based |
| **`tags.csv`** | 1.093.360 dòng | 38.81 MB | Thẻ nhãn tự do của người dùng, phục vụ phân tích ngữ cảnh |
| **`movies.csv`** | 62.423 dòng | 3.04 MB | Danh mục phim và danh sách 20 thể loại chuẩn hóa |
| **`links.csv`** | 62.423 dòng | 1.37 MB | Bảng ánh xạ mã định danh IMDb và TMDb (lấy poster/trailer) |
| **`genome-tags.csv`** | 1.128 dòng | 18.10 KB | Từ điển mô tả 1.128 đặc trưng điện ảnh |

### BƯỚC 2: CHUẨN HÓA DỮ LIỆU VÀ TỐI ƯU HÓA DATA LAKE (LAKE ZONE)
* **Tại sao đây là bài toán bắt buộc phải dùng Big Data?**
  * Dữ liệu thô vượt quá 1.15 GB và chứa hơn 41.7 triệu dòng văn bản.
  * Không gian tương tác Người dùng - Phim lên tới **9.6 tỷ ô** ($162.541 \times 59.047$).
  * Việc nạp, lọc, join và tính toán ma trận này trên một máy tính cá nhân bằng các thư viện thông thường (như Pandas/Excel) sẽ ngay lập tức gây cạn kiệt bộ nhớ RAM (OOM) hoặc mất hàng giờ đồng hồ để quét. Do đó, việc xây dựng một hệ thống lưu trữ phân tán (HDFS) và tính toán bộ nhớ phân tán (Apache Spark) là yêu cầu kỹ thuật bắt buộc.

1. **Khai báo Schema tĩnh tường minh (Explicit Schema)**:
   * Định nghĩa toàn bộ cấu trúc bảng bằng `StructType` trong Python để loại bỏ hoàn toàn chi phí quét thừa của `inferSchema=True`.
2. **Chuyển đổi sang Parquet nén Snappy**:
   * Chuyển đổi 6 tệp từ CSV sang Parquet dạng cột:
     * `ratings.csv` (678 MB) $\rightarrow$ Nén thành ~268 MB Parquet, chia đúng 2 khối 128 MB.
     * `genome-scores.csv` (435 MB) $\rightarrow$ Nén thành ~140 MB Parquet, chia 1-2 khối 128 MB.
     * `movies.csv`, `tags.csv`, `links.csv`, `genome-tags.csv`.
3. **Giải quyết bài toán Small Files**:
   * Áp dụng `.coalesce(1)` cho các tệp nhỏ (`movies`, `links`) để gom thành 1 tệp duy nhất, tránh phân mảnh tệp trên NameNode.
4. **Lưu trữ vào HDFS Lake Zone**:
   * Toàn bộ tệp Parquet được ghi trực tiếp vào `hdfs://namenode:9000/movielens/lake/`.

---

### BƯỚC 3: PHÂN TÍCH DỮ LIỆU PHÂN TÁN BẰNG SPARK SQL (DISTRIBUTED EDA) - TRỌNG TÂM
## 3. LỘ TRÌNH KỸ THUẬT HIỆN THỰC HÓA HỆ THỐNG (5 BƯỚC)

Tạo job `src/jobs/eda_sparksql.py` thực thi các câu truy vấn phân tán bằng Spark SQL để trả lời các câu hỏi lớn về dữ liệu:
Sau khi đã xác định rõ mục đích và giá trị đầu ra của dữ liệu, quy trình kỹ thuật sẽ lần lượt triển khai qua 5 bước:

1. **Phân tích độ thưa ma trận tương tác (Matrix Sparsity)**:
   * Truy vấn đếm số lượng người dùng duy nhất ($|U|$), số lượng phim duy nhất ($|I|$) và số lượng đánh giá ($|R|$).
   * Tính toán độ thưa ma trận: $\text{Sparsity} = 1 - \frac{|R|}{|U| \times |I|} \approx 99.74\%$.
2. **Phân tích hiện tượng Đuôi dài (Long-Tail Distribution)**:
   * Nhóm dữ liệu theo `movieId`, đếm số lượt đánh giá và sắp xếp giảm dần.
   * Tính toán tỷ lệ phần trăm tích lũy của các phim phổ biến nhất.
   * Rút ra căn cứ kỹ thuật giải thích việc cần áp dụng bộ lọc `MIN_RATINGS_PER_MOVIE = 20`.
3. **Phân tích phân bố điểm số đánh giá (Rating Distribution)**:
   * Thống kê tần suất và tỷ lệ của 10 mức điểm (từ 0.5 đến 5.0 sao).
   * Tính điểm trung bình ($\mu \approx 3.53$), độ lệch chuẩn và phân tích xu hướng người dùng thiên về đánh giá tích cực.
4. **Phân tích đa chiều người dùng (User Activity Analysis)**:
   * Dùng hàm phân tích cửa sổ `Window.partitionBy("userId")` để khảo sát hành vi người dùng (số lượng phim đã xem, điểm trung bình cá nhân).
   * Phân tích các trường hợp người dùng cực đoan (Cold-start users vs Power users).
5. **Phân tích đặc trưng thể loại và Thẻ ngữ nghĩa (Tag Genome)**:
   * Tách nhãn thể loại trong cột `genres` bằng `split` và `explode` để thống kê độ phổ biến của 20 thể loại phim.
   * Truy vấn ma trận `genome-scores` để tìm ra các đặc trưng có điểm liên quan cao nhất giữa các nhóm phim.
6. **Xuất bảng chỉ số và vẽ biểu đồ báo cáo**:
   * Xuất các bảng aggregate nhỏ ra `report/results/eda_*.csv`.
   * Sinh các biểu đồ chất lượng cao (PNG 300 DPI) lưu vào `report/figures/` tương tự bài `exe2`.
```
┌─────────────────────────────────────────────────────────────────────────────┐
│ BƯỚC 1: DỰNG HẠ TẦNG PHÂN TÁN ĐA NODE & NẠP DỮ LIỆU THÔ VÀO HDFS            │
│ - Dựng cụm HDFS (1 NameNode + 3 DataNodes, Rep=3, Block 128 MB)             │
│ - Dựng cụm Apache Spark (1 Master + N Workers)                              │
│ - Nạp toàn bộ 6 tệp CSV thô lên hdfs://namenode:9000/movielens/raw/        │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ BƯỚC 2: CHUẨN HÓA DỮ LIỆU SANG TẦNG DATA LAKE (PARQUET SNAPPY)              │
│ - Khai báo Schema tĩnh bằng StructType (loại bỏ chi phí inferSchema)        │
│ - Chuyển đổi CSV sang Parquet nén Snappy phân khối chuẩn 128 MB             │
│ - Gom tệp nhỏ (coalesce) để giải quyết bài toán Small Files trên NameNode   │
│ - Lưu trữ tại hdfs://namenode:9000/movielens/lake/                          │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ BƯỚC 3: PHÂN TÍCH DỮ LIỆU PHÂN TÁN BẰNG SPARK SQL (DISTRIBUTED EDA)        │
│ - Phân tích độ thưa ma trận tương tác (Sparsity 99.74%)                     │
│ - Phân tích hiện tượng đuôi dài (Long-Tail) trên 25 triệu đánh giá          │
│ - Phân tích phân bố điểm số và xu hướng đánh giá của người dùng             │
│ - Phân tích 1.128 chiều đặc trưng của Tag Genome và 20 thể loại phim        │
│ - Xuất bảng tổng hợp số liệu và vẽ biểu đồ báo cáo khoa học (PNG 300 DPI)  │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ BƯỚC 4: HUẤN LUYỆN MÔ HÌNH HỌC MÁY GỢI Ý PHÂN TÁN (SPARK MLLIB)             │
│ - Phân tách tập dữ liệu theo chuỗi thời gian (Temporal Split 70/15/15)      │
│ - Huấn luyện mô hình ALS Collaborative Filtering và Grid Search siêu tham số│
│ - Xây dựng mô hình Content-Based Filtering qua Tag Genome giải quyết Cold-Start│
│ - Đánh giá Top-N Ranking (NDCG@10, Precision@10) đối chiếu 3 Baselines     │
│ - Đo đạc khả năng mở rộng quy mô tính toán (Scaling: 1, 2, 4 Workers)       │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ BƯỚC 5: XUẤT BẢN KẾT QUẢ & PHỤC VỤ TRỰC TUYẾN (SERVING & DASHBOARD)         │
│ - Xuất danh sách gợi ý vào SQLite có đánh chỉ mục B-Tree                    │
│ - Xuất vector đặc trưng phim ra mảng NumPy (In-Memory Similarity Search)    │
│ - Triển khai RESTful API bằng FastAPI (< 20ms độ trễ)                       │
│ - Xây dựng giao diện Web Dashboard demo trực quan cho người dùng            │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

### BƯỚC 4: HUẤN LUYỆN MÔ HÌNH VÀ THẨM ĐỊNH HIỆU NĂNG PHÂN TÁN
## 4. BỐ TRÍ HẠ TẦNG VẬT LÝ VÀ PHÂN BỔ TÀI NGUYÊN (16 GB RAM / 16 CORES CPU)

1. **Phân tách tập dữ liệu theo chuỗi thời gian (Temporal Split)**:
   * Áp dụng `Window.partitionBy("userId").orderBy("timestamp")` chia 70% Train, 15% Validation, 15% Test trên từng người dùng.
2. **Huấn luyện Spark MLlib ALS**:
   * Huấn luyện mô hình phân rã ma trận xấp xỉ hạng thấp.
   * Đo đạc hiệu năng với Grid Search siêu tham số ($rank, regParam$).
3. **Đánh giá và so sánh Baseline**:
   * Đánh giá RMSE và các chỉ số Top-N ranking (NDCG@10, Precision@10, Coverage) so với Global Mean, Item Mean và Popularity Baseline.
4. **Đo lường khả năng mở rộng (Scaling Experiment)**:
   * Đo thời gian thực thi của cụm khi thay đổi số lượng Worker (1 Worker, 2 Workers, 4 Workers) để đánh giá khả năng mở rộng quy mô tính toán.
Hệ thống được cấu hình chạy trên Docker Compose bảo đảm tính cô lập và phân bổ tài nguyên tối ưu:

---
| Thành phần Container | Vai trò kiến trúc | CPU giới hạn | RAM giới hạn | Cổng dịch vụ |
| :--- | :--- | :--- | :--- | :--- |
| **`namenode`** | HDFS Master: Quản lý Metadata, Namespace | 1 Core | 1.0 GB (`-Xmx768m`) | `9870` (Web UI), `9000` (IPC) |
| **`datanode1`** | HDFS Worker 1: Lưu trữ khối dữ liệu 128 MB | 1 Core | 1.0 GB (`-Xmx768m`) | `9864` |
| **`datanode2`** | HDFS Worker 2: Lưu trữ bản sao dữ liệu (Rep=3) | 1 Core | 1.0 GB (`-Xmx768m`) | `9865` |
| **`datanode3`** | HDFS Worker 3: Lưu trữ bản sao dữ liệu (Rep=3) | 1 Core | 1.0 GB (`-Xmx768m`) | `9866` |
| **`spark-master`**| Spark Master: Điều phối Executor và DAG | 1 Core | 1.0 GB | `8080` (Web UI), `7077` (RPC) |
| **`spark-worker-1`**| Spark Worker 1: Thực thi tính toán phân tán | 3 Cores | 2.5 GB | - |
| **`spark-worker-2`**| Spark Worker 2: Thực thi tính toán phân tán | 3 Cores | 2.5 GB | - |
| **`movielens-api`** | Serving API Gateway (FastAPI) | 1 Core | 0.5 GB | `8000` (Web Demo) |
| **Hệ điều hành Host** | Dành cho Windows Host / WSL2 | 4 Cores | ~5.5 GB | Bảo đảm hệ thống mượt mà |
| **TỔNG CỘNG** | **8 containers phân tán** | **16 Cores** | **15.5 GB / 16.0 GB** | **Vận hành an toàn** |

### BƯỚC 5: TẦNG PHỤC VỤ VÀ BÁO CÁO NGHIỆM THU

1. **Bàn giao kết quả sang Serving Storage**:
   * Xuất danh sách gợi ý vào `recs.sqlite` có đánh chỉ mục B-Tree và ma trận vector phim `item_factors.npy`.
2. **FastAPI Gateway & Demo**:
   * Phục vụ truy vấn gợi ý phim và tìm phim tương tự với độ trễ $< 16$ mili-giây.
3. **Biên dịch báo cáo kỹ thuật hoàn chỉnh**:
   * Biên dịch báo cáo dạng Markdown và HTML trình diễn đầy đủ các sơ đồ kiến trúc HDFS, kết quả Spark SQL và bảng đo lường.

---

## 5. PHÂN BỔ TÀI NGUYÊN TRÊN MÁY CHỦ (16 GB RAM / 16 CORES CPU)

| Thành phần Container | Số lượng | CPU cấp phát | Giới hạn RAM | Ghi chú cấu hình |
| :--- | :--- | :--- | :--- | :--- |
| **`namenode`** | 1 | 1 Core | 1.0 GB | Image Hadoop 3.2.1, cổng Web UI 9870 |
| **`datanode1`** | 1 | 1 Core | 1.0 GB | Cổng 9864, lưu trữ khối HDFS |
| **`datanode2`** | 1 | 1 Core | 1.0 GB | Cổng 9865, lưu trữ khối HDFS |
| **`datanode3`** | 1 | 1 Core | 1.0 GB | Cổng 9866, lưu trữ khối HDFS |
| **`spark-master`** | 1 | 1 Core | 1.0 GB | Cổng Web UI 8080, RPC 7077 |
| **`spark-worker`** | 2 | 3 Cores / node | 2.5 GB / node | Thực thi tính toán Spark SQL & ALS |
| **`movielens-api`** | 1 | 1 Core | 0.5 GB | Tầng phục vụ độc lập |
| **Windows Host / WSL2** | - | 4 Cores | Dư địa an toàn (~5.5 GB) | Đảm bảo hệ thống vận hành ổn định |
| **TỔNG CỘNG** | **8 containers** | **16 Cores** | **15.5 GB / 16.0 GB** | **Vận hành an toàn trên 1 máy** |
