# Báo Cáo: Kiến trúc Hệ thống Phân tích & Gợi ý Phim Thời gian thực (Real-time Streaming Pipeline)

Báo cáo này mô tả quy trình dữ liệu (Data Pipeline) từ lúc một tương tác của người dùng được sinh ra cho đến khi hệ thống AI học lại và phản hồi gợi ý mới lên giao diện web. Hệ thống được xây dựng theo chuẩn **Kappa Architecture**, loại bỏ giới hạn giữa xử lý Batch và Streaming bằng cách sử dụng chung một luồng lõi.

## 1. Sơ đồ luồng Dữ liệu (Architecture Diagram)

Dưới đây là mô hình luồng chảy của dữ liệu trong hệ thống.

```mermaid
flowchart TD
    %% Định nghĩa Style
    classDef source fill:#f9f871,stroke:#333,stroke-width:2px,color:#000
    classDef broker fill:#ffc75f,stroke:#333,stroke-width:2px,color:#000
    classDef compute fill:#ff9671,stroke:#333,stroke-width:2px,color:#000
    classDef storage fill:#ff6f91,stroke:#333,stroke-width:2px,color:#fff
    classDef serving fill:#845ec2,stroke:#333,stroke-width:2px,color:#fff

    subgraph "Nguồn phát sinh (Event Sources)"
        UI["💻 Web UI<br>(Đánh giá trực tiếp)"]:::source
        SIM["⚙️ Python Simulator<br>(Bơm bulk data hàng loạt)"]:::source
    end

    subgraph "Tầng Đệm & Hàng đợi (Message Broker)"
        API["🌐 FastAPI<br>(Endpoint /api/rate)"]:::serving
        KAFKA[("📨 Apache Kafka<br>Topic: 'ratings'")]:::broker
    end

    subgraph "Tầng Xử lý Luồng (Stream Processing)"
        INGEST["⚡ Spark Structured Streaming<br>(stream_ingest.py)"]:::compute
    end

    subgraph "Tầng Lưu trữ (Data Storage)"
        DELTA[("💧 Delta Lake<br>(Dữ liệu mới)") ]:::storage
        PARQUET[("📦 Parquet<br>(Dữ liệu lịch sử)") ]:::storage
    end

    subgraph "Tầng Máy học (Machine Learning)"
        TRAIN["🧠 Spark MLlib ALS<br>(train_als_stream.py)"]:::compute
    end

    subgraph "Tầng Phục vụ (Serving & UI)"
        SQLITE[("🗄️ SQLite DB<br>(recs.sqlite)")]:::storage
        FRONTEND["📱 Giao diện Gợi ý Phim"]:::serving
    end

    %% Luồng đi
    UI -- HTTP POST --> API
    API -- Push JSON --> KAFKA
    SIM -- Push JSON --> KAFKA
    
    KAFKA -- Consume (10s micro-batch) --> INGEST
    INGEST -- Append & Checkpoint --> DELTA
    
    DELTA -- Load Stream Data --> TRAIN
    PARQUET -- Load Base Data --> TRAIN
    
    TRAIN -- Re-fit Model & Export --> SQLITE
    SQLITE -. "Hot-reload (Phát hiện file thay đổi)" .-> API
    API -- Trả kết quả JSON --> FRONTEND

```

---

## 2. Diễn giải chi tiết các Tầng (Layers) trong Pipeline

### Tầng 1: Thu thập Dữ liệu (Ingestion Layer)
Hệ thống cho phép thu thập dữ liệu từ 2 nguồn:
1. **Nguồn thực (Single Event):** Người dùng bấm chấm điểm trên Web UI. Giao diện gọi API `POST /api/rate`, FastAPI đứng ra đóng gói dữ liệu thành JSON và làm Producer đẩy thẳng vào Kafka.
2. **Nguồn giả lập (Bulk Events):** Script `simulate_bulk_stream.py` đọc từ file CSV khổng lồ, "bắn liên thanh" hàng nghìn tin nhắn mỗi giây vào Kafka để stress-test hệ thống.

### Tầng 2: Hệ thống Đệm (Message Broker) - Apache Kafka
Kafka đóng vai trò là "Giảm xóc" (Buffer) cho toàn bộ hệ thống Big Data. Nhờ có Kafka, dù hàng chục nghìn người dùng cùng chấm điểm một lúc, hệ thống cũng không bị sập. Dữ liệu được xếp hàng gọn gàng trong topic `ratings` chờ Spark tới lấy.

### Tầng 3: Xử lý Luồng thời gian thực (Stream Processing) - Spark & Delta Lake
Job `stream_ingest.py` hoạt động 24/7 dưới dạng một con bot túc trực. 
* Cứ mỗi 10 giây (Micro-batch), nó lại vớt toàn bộ dữ liệu mới tinh từ Kafka.
* Chuyển đổi (Transform) dữ liệu JSON thành dạng bảng (DataFrame).
* Ghi nối (Append) vào **Delta Lake**. Việc sử dụng Delta Lake ở đây cực kỳ quan trọng vì nó hỗ trợ ACID (đảm bảo dữ liệu không bị hỏng khi hệ thống vừa ghi vừa đọc cùng lúc).

### Tầng 4: Máy học Liên tục (Continuous Machine Learning)
Job `train_als_stream.py` cũng là một vòng lặp chạy ngầm (mỗi 60s một lần).
* Nó thực hiện lệnh **Union (Gộp)** để nối dữ liệu lịch sử gốc (file Parquet) và dữ liệu mới sinh (Delta Lake) lại với nhau thành một khối.
* Nạp khối dữ liệu mới này vào thuật toán Matrix Factorization (ALS) để huấn luyện lại toàn bộ trọng số (Embeddings) của người dùng và bộ phim. 

### Tầng 5: Xuất bản và Phục vụ (Serving Layer)
* Ngay khi train xong, kết quả gợi ý mới nhất (Top 10 phim cho mỗi người) được lưu đè vào file `recs.sqlite`.
* Ứng dụng **FastAPI** được cấu hình chế độ **Hot-reload**. Ngay khi nó nhận thấy file `sqlite` bị thay đổi dung lượng/thời gian, nó lập tức cập nhật bộ nhớ đệm (RAM) mà không cần sập server.
* Nhờ vậy, người dùng trên Web chỉ việc nhấn F5 (Refresh) là ngay lập tức thấy danh sách gợi ý phim của mình đã bị thay đổi phù hợp với thị hiếu mới nhất.

## 3. Điểm nhấn Công nghệ để Thuyết trình (Key Takeaways)
1. **Khả năng chịu lỗi (Fault-Tolerance):** Nếu Spark chết, nhờ cơ chế *Checkpoint* của Delta Lake và *Offset* của Kafka, khi khởi động lại, Spark sẽ đọc tiếp đúng chỗ bị đứt đoạn, không mất 1 dòng dữ liệu nào.
2. **Thích ứng siêu tốc (Adaptability):** Khắc phục nhược điểm "Model bị thiu (Concept Drift)" của các hệ thống AI cũ bằng cách liên tục mớm dữ liệu mới để tự điều chỉnh độ nhạy cảm của thuật toán theo thời gian thực.
3. **Phân tách Rủi ro (Decoupling):** Frontend (Web) hoàn toàn tách biệt với Backend (Spark). Spark có chạy nặng hay crash thì Web vẫn trả về gợi ý cũ bình thường, đảm bảo trải nghiệm người dùng không bao giờ gián đoạn.

