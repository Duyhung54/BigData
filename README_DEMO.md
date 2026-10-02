# KỊCH BẢN THUYẾT TRÌNH & DEMO HỆ THỐNG TRONG 3 PHÚT
## Đề tài: Hệ Thống Gợi Ý Phim Phân Tán (MovieLens 32M + HDFS + Spark + Kafka + PostgreSQL + Redis)
### Học phần: Big Data Analytics (BDA501)

---

## 1. CHUẨN BỊ TRƯỚC KHI BẮT ĐẦU (0:00 - 0:15)

1. Mở trình duyệt tại địa chỉ: **`http://localhost:8000`** (Giao diện 4 Tab chuẩn Netflix).
2. Chuẩn bị sẵn 2 tab phụ:
   * **HDFS NameNode UI**: `http://localhost:9870` (chứng minh 3 DataNodes).
   * **Spark Master UI**: `http://localhost:8080` (chứng minh 3 Workers).

---

## 2. KỊCH BẢN DEMO TỪNG PHÚT CHI TIẾT

### PHÚT 1: GIỚI THIỆU KIẾN TRÚC LAMBDA TRÊN TAB "DASHBOARD"
* **Hành động**: Đang ở Tab 1 (**Dashboard**).
* **Lời trình bày**:
  > *"Kính thưa Thầy/Cô, hệ thống của nhóm em được thiết kế theo đúng mô hình **Lambda Architecture** xử lý bộ dữ liệu lớn **MovieLens 32M** (> 49,6 triệu dòng) với 2 luồng độc lập hiển thị trực tiếp trên giao diện:*
  > 
  > *1. **Cột bên trái là Luồng Khối (Batch Layer)**: Sử dụng **Apache Spark MLlib ALS** chạy định kỳ trên cụm **HDFS 3 DataNodes** với khối 128 MB. Kết quả gợi ý Top-20 được lưu trữ kiên cố trong **PostgreSQL** để phục vụ người dùng với độ trễ dưới 10ms.*
  > 
  > *2. **Cột bên phải là Luồng Tốc độ (Speed Layer)**: Đón các sự kiện chấm điểm thời gian thực qua **Apache Kafka KRaft**, sau đó **Spark Structured Streaming** tính toán trượt cửa sổ 1 giờ (Sliding Window 1h) để liên tục cập nhật Top 20 Phim Thịnh Hành vào **Redis In-Memory** với huy hiệu Top 3 Vàng, Bạc, Đồng và tự động làm mới mỗi 5 giây mà không cần F5 trang."*

---

### PHÚT 2: CHỨNG MINH XỬ LÝ BÀI TOÁN COLD-START TRÊN TAB "COLDSTART"
* **Hành động**: Nhấp chuyển sang Tab 2 (**ColdStart**).
* **Lời trình bày**:
  > *"Để hệ thống không bị phụ thuộc đơn điệu vào một thuật toán, nhóm đã giải quyết triệt để 2 bài toán Khởi đầu lạnh (Cold-Start):*
  > 
  > *1. **Item Cold-Start (Phim mới ra rạp chưa có đánh giá)**: Nhóm khai thác tệp **`genome-scores.csv` gồm 15,58 triệu bản ghi** để vector hóa 13.816 bộ phim thành các vector đặc trưng 1.128 chiều. Khi em nhập `Movie ID = 1` (Toy Story) và bấm **'Tìm Phim Tương Tự'**, hệ thống lập tức truy vấn bảng **`movie_similarity`** trong PostgreSQL, trả về các phim hoạt hình phiêu lưu tương đồng ngữ nghĩa trên 90% (như Toy Story 2, A Bug's Life).*
  > 
  > *2. **User Cold-Start (Người dùng mới toanh)**: Khi em bấm nút **'Test với UserID = 99999'**, hệ thống phát hiện người dùng này chưa từng có lịch sử trong tập Train ALS. Thay vì báo lỗi 404, tầng Serving lập tức kích hoạt cơ chế **Fallback sang Top Trending thời gian thực từ Redis** kèm tag cảnh báo màu vàng rõ ràng trên màn hình."*

---

### PHÚT 3: MÔ PHỎNG TƯƠNG TÁC THỰC NGHIỆM TRÊN TAB "VẬN HÀNH & DEMO"
* **Hành động**: Nhấp chuyển sang Tab 3 (**Vận Hành & Demo**).
* **Lời trình bày**:
  > *"Bây giờ em sẽ mô phỏng luồng tương tác phát sinh trực tiếp:*
  > 
  > *1. **Bắn 100 sự kiện Stream**: Em bấm nút **'Bắn 100 Sự Kiện Kafka'**. Thanh tiến trình chạy từ 1 đến 100, gửi các sự kiện đánh giá vào topic `user_events`. Terminal bên dưới lập tức ghi nhận log. Khi em chuyển lại Tab 1 Dashboard, bảng xếp hạng Top 20 Trending lập tức tự động nhảy số lượt tương tác.*
  > 
  > *2. **Mô phỏng Click & Boost**: Khi em nhập `UserID = 1, MovieID = 296` và bấm **'Click & Boost'**, hệ thống ghi nhận ngay lượt quan tâm vào Redis Hash `user:1:online_pref` và cộng +2 điểm Trending cho bộ phim đó.*
  > 
  > *3. **Chứng minh phân tán**: Trên terminal máy chủ, lệnh `hdfs fsck /processed/ratings.parquet -files -blocks -locations` chứng minh toàn bộ 14 khối dữ liệu đã được nhân bản 3 lần và phân tán đều trên 3 DataNodes độc lập."*

---

## 3. CÁC CÂU HỎI THẦY CÔ CÓ THỂ HỎI & CÁCH TRẢ LỜI NGẮN GỌN

| Câu hỏi | Câu trả lời mấu chốt |
| :--- | :--- |
| **Tại sao luồng Stream không train lại ALS?** | *"Dạ thưa Thầy/Cô, với dữ liệu hàng chục triệu dòng, huấn luyện lại ma trận ALS toàn cục liên tục sẽ làm nghẽn CPU và tràn đĩa đệm. Chuẩn Lambda phân định: Stream chỉ tính Sliding Window Top Trending lưu vào Redis, còn việc Retrain ALS là của Batch chạy định kỳ ban đêm."* |
| **Tại sao chọn BlockSize 128 MB mà không phải 512 MB?** | *"Dạ thưa Thầy/Cô, file MovieLens 32M nặng ~1.7 GB. Chia 128 MB sẽ tạo ra đúng 14 blocks, nhân 3 bản sao = 42 blocks rải đều 3 DataNodes (mỗi node 14 blocks), giúp Spark chạy song song tối đa CPU. Nếu để 512 MB thì chỉ có 3 block, Spark chỉ chạy được 3 task, làm mất tính song song phân tán."* |
| **Tại sao không dùng repartition khi ghi Parquet?** | *"Dạ thưa Thầy/Cô, Spark đã tự động chia partition theo đúng block HDFS đầu vào. Nếu repartition sẽ gây Network Shuffle vô ích và sinh ra hàng trăm file nhỏ vài MB gây lỗi Small Files Problem cho NameNode."* |
| **Kafka đang chạy chế độ gì?** | *"Dạ thưa Thầy/Cô, hệ thống sử dụng Kafka 3.6 chạy chế độ KRaft (Raft Metadata Mode) tự quản lý Controller và Metadata nội bộ mà không cần phụ thuộc vào ZooKeeper, giúp tiết kiệm 1 GB RAM và tăng tốc độ khởi động cụm."* |

