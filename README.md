## 🗺️ Lộ trình Phát triển Dự án (Project Roadmap)

Lộ trình này mô tả cách tiếp cận chuẩn Data Engineering để xây dựng một hệ thống pipeline xử lý dữ liệu y tế MIMIC-IV v3.1 một cách tin cậy, tối ưu và sẵn sàng cho phân tích.

> [Tuần 1: Ingestion & Định dạng] ──> [Tuần 2: Làm sạch & Silver] ──> [Tuần 3: Tự động hóa Pipeline] ──> [Tuần 4: Gold Layer & Chất lượng]

### 📅 Tuần 1: Khởi tạo Môi trường & Nạp Dữ liệu Thô (Bronze Layer)
*   **Mục tiêu:** Thiết lập cấu trúc dự án và chuyển đổi các file CSV thô khổng lồ sang định dạng lưu trữ dạng cột (columnar format) tối ưu.
*   **Kết quả đạt được:**
    *   [ ] Thiết lập môi trường Python và cài đặt các thư viện cốt lõi (**DuckDB**, **Pandas**, **PyYAML**).
    *   [ ] Xây dựng cấu trúc thư mục chuẩn production (`config/`, `src/`, `tests/`).
    *   [ ] Triển khai pipeline tầng **Bronze**: Đọc dữ liệu CSV từ MIMIC-IV và ghi lại dưới dạng file **Parquet** (áp dụng nén Snappy) để giảm dung lượng lưu trữ lên tới 80%.
    *   [ ] Thử nghiệm tốc độ truy vấn giữa file CSV thô và file Parquet bằng DuckDB để tối ưu hóa hiệu năng phần cứng.

### 📅 Tuần 2: Làm sạch Dữ liệu & Chuẩn hóa Quan hệ (Silver Layer)
*   **Mục tiêu:** Xử lý các bất thường logic, ép kiểu dữ liệu và liên kết các module dữ liệu (`core`, `hosp`, `icu`) lại với nhau.
*   **Kết quả đạt được:**
    *   [ ] Đồng bộ hóa định dạng thời gian về chuẩn ISO (`YYYY-MM-DD HH:MM:SS`) và xử lý các lỗi logic (ví dụ: ngày xuất viện trước ngày nhập viện).
    *   [ ] Chuẩn hóa các giá trị khuyết (`NULL`), ép kiểu dữ liệu tối ưu cho các bảng có mật độ dòng cao như `chartevents` và `labevents`.
    *   [ ] Đảm bảo tính toàn vẹn dữ liệu (Referential Integrity) bằng cách mapping chính xác các cặp ID: `subject_id` $\rightarrow$ `hadm_id` $\rightarrow$ `stay_id`.
    *   [ ] Thiết lập các hàm xử lý dữ liệu có tính **Idempotent** (chạy đi chạy lại nhiều lần với cùng một đầu vào không làm sai lệch hay trùng lặp dữ liệu).

### 📅 Tuần 3: Tự động hóa & Điều phối Pipeline (Orchestration Engine)
*   **Mục tiêu:** Liên kết các script xử lý riêng lẻ thành một chuỗi pipeline tự động, có kiểm soát thứ tự và log rõ ràng.
*   **Kết quả đạt được:**
    *   [ ] Tổ chức các bước ETL (Extract - Transform - Load) thành một luồng công việc có thứ tự phụ thuộc nghiêm ngặt (Ví dụ: module `hosp` phải sạch trước khi nạp module `icu`).
    *   [ ] Tích hợp hệ thống quản lý cấu hình tập trung qua file YAML (`config/base_config.yaml`) để dễ dàng thay đổi tham số pipeline mà không cần sửa code.
    *   [ ] Xây dựng hệ thống Log (Zentralized Logging) để theo dõi số lượng dòng được xử lý qua từng tầng và phát hiện lỗi (Error Catching).
    *   [ ] Viết file `Makefile` để tự động hóa việc kích hoạt toàn bộ pipeline chỉ bằng một câu lệnh duy nhất (ví dụ: `make run-pipeline`).

### 📅 Tuần 4: Tổng hợp Dữ liệu Phân tích (Gold Layer) & Kiểm soát Chất lượng
*   **Mục tiêu:** Gom tụ các dữ liệu lâm sàng tần suất cao thành các tập đặc trưng (Feature Store) sẵn sàng cho Data Scientist hoặc BI tools.
*   **Kết quả đạt được:**
    *   [ ] Xây dựng bảng tổng hợp **ICU Cohort Matrix**: Chứa thông tin tổng quan của mỗi ca nằm viện bao gồm thông tin nhân khẩu học, chỉ số sinh tồn ban đầu, thời gian nằm viện (LOS), và điểm số nhiễm trùng (SOFA/SAPS II).
    *   [ ] Tính toán các chỉ số tổng hợp theo thời gian (Time-series aggregations) như: Nhịp tim/Huyết áp trung bình, cao nhất, thấp nhất trong mỗi block 4 giờ hoặc 12 giờ của bệnh nhân.
    *   [ ] Tích hợp kiểm tra chất lượng tự động (**Data Quality Gate**): Đảm bảo các chỉ số sinh tồn nằm trong khoảng sinh lý hợp lý, các cột khóa chính không bị `NULL`.
    *   [ ] Đo lường hiệu năng (Benchmark): Ghi lại thời gian chạy và tài nguyên tiêu hao của toàn bộ hệ thống pipeline.

---

### 🚀 Định hướng Mở rộng (Future Enhancements)
*   [ ] Tích hợp **dbt (Data Build Tool)** kết hợp DuckDB để quản lý Lineage (sơ đồ phụ thuộc của bảng) một cách trực quan.
*   [ ] Cấu hình **CI/CD via GitHub Actions** để tự động kiểm tra code (linting) và chạy unit test mỗi khi push code mới.
