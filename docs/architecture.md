# System Architecture Specification
> File này đóng vai trò sơ đồ tư duy toàn bộ hệ thống. Giúp người đọc hình dung được dữ liệu đi tư đâu, qua các bước biến đổi nào và phục vụ cho bài toán gì

---

## 1. Overview

Dự án tập trung xây dựng một pipeline xử lý dữ liệu quy mô lớn theo kiến trúc **Medallion** nhằm chuẩn hóa dữ liệu lâm sàng từ bộ cơ sở dữ liệu hồi sức cấp cứu chuyên sâu **MIMIC-IV (v3.1)** thành một **Clinical Feature Store** phẳng, 
có khả năng tái sử dụng linh hoạt cho nhiều bài toán Machine Learning khác nhau trong y tế.

Hệ thống này hướng tới việc tạo ra một lớp dữ liệu trung gian chuẩn hóa **(Unified Feature Store Matrix)**. Lớp dữ liệu này sẵn sàng cung cấp trực tiếp các Vector đặc trưng cho các mô hình dự báo lâm sàng cốt lõi:

*   **Mortality Prediction:** Dự báo nguy cơ tử vong trong viện hoặc trong vòng 30 ngày.
*   **Sepsis Prediction:** Dự báo sớm nhiễm khuẩn huyết theo tiêu chuẩn Sepsis-3.
*   **Acute Kidney Injury (AKI) Prediction:** Dự báo suy thận cấp dựa trên động học Creatinine và nước tiểu theo tiêu chuẩn KDIGO.
*   **Shock Prediction:** Dự báo sốc nhiễm khuẩn hoặc sốc tim dựa trên biến động huyết áp và hạ áp.
*   **Length of Stay (LoS) Prediction:** Dự báo số ngày điều trị số hóa tại ICU.
*   **ICU Readmission Prediction:** Dự báo nguy cơ tái nhập khoa hồi sức cấp cứu trong vòng 30 

---

## 2. High-Level Data Flow (Sơ đồ luồng dữ liệu)

### 2.1. Vai trò của các công nghệ trong hệ thống:
- **Prefect (Orchestration):** Đóng vai trò "nhạc trưởng" điều phối toàn bộ vòng đời của dữ liệu. Prefect quản lý luồng phụ thuộc giữa các tác vụ (Tasks DAG), tự động hóa việc lập lịch, quản lý trạng thái lỗi, cơ chế thử lại (Retry) và giám sát tập trung qua Dashboard.
- **Hydra (Configuration):** Quản lý tập trung cấu hình hệ thống một cách linh hoạt (đường dẫn thư mục, giới hạn tài nguyên phần cứng, danh sách bảng cần lấy, tham số các khung thời gian Multi-window $1\text{h}, 6\text{h}, 24\text{h}$).
- **dbt-duckdb + Parquet (Storage & Transformation):** Trái tim của công cụ xử lý. DuckDB đóng vai trò là Query Engine siêu tốc để thực thi các câu lệnh SQL ngay trên các tệp Parquet cục bộ mà không cần hệ quản trị CSDL cồng kềnh. dbt chịu trách nhiệm quản lý mã nguồn SQL (tính kế thừa qua hàm ref()), quản lý tài liệu (Documentation) và thực thi chuyển đổi dữ liệu theo các tầng Medallion.
- **Great Expectations (Data Quality):** Cung cấp các chốt chặn chất lượng (Checkpoints) để kiểm thử dữ liệu tự động giữa các tầng chuyển tiếp, đảm bảo tính đúng đắn của schema sinh học và logic lâm sàng.
### 2.2. Luồng dữ liệu chi tiết

Processing & Feature Engineering Layer (Tầng xử lý & Trích xuất đặc trưng):

Các bước làm sạch dữ liệu, xử lý giá trị khuyết (imbalanced/missing data).

Nơi các Feature Groups trong feature_catalog.md được tính toán.

## 3. Component & Workflow Module (Chi tiết các Module mã nguồn)
Ánh xạ kiến trúc hệ thống vào chính cấu trúc thư mục code thực tế của bạn. Phần này giúp người đọc biết file nào làm nhiệm vụ gì. Ví dụ:

configs/: Quản lý các file cấu hình.

src/data/: Chứa mã nguồn tải, phân tách (train/val/test split) và tiền xử lý thô.

src/features/: Các script tính toán feature, kết nối trực tiếp với định nghĩa trong data_dictionary.md.

src/models/: Chứa pipeline định nghĩa kiến trúc mô hình, huấn luyện, và tuning.

## 4. Configuration & Experiment Tracking Strategy

Đây là phần thể hiện rõ nhất kỹ năng MLOps chuyên nghiệp của bạn. Hãy ghi rõ cách bạn kiểm soát sự thay đổi của dự án:

Cấu hình hệ thống (Hydra): Giải thích cách bạn module hóa cấu hình (ví dụ: tách biệt config cho data, config cho model, config cho hyperparameters). Nhờ đó khi cần thay đổi mô hình, bạn chỉ cần đổi file YAML thay vì sửa code.

Theo dõi thực nghiệm (MLflow): Định nghĩa rõ các thông số sẽ được lưu vết tự động:

Parameters: Tên mô hình, danh sách features được chọn, bộ siêu tham số (learning rate, depth...).

Metrics: Các chỉ số đánh giá tùy theo bài toán (như RMSE, MAE cho hồi quy; hoặc Precision, Recall, F1-Score, PR-AUC cho bài toán phân loại tập dữ liệu mất cân bằng).

Artifacts: Lưu lại file trọng số mô hình (.pkl, .onnx), biểu đồ Feature Importance, biểu đồ Precision-Recall curve.

## 4. Pipeline Modules Description
*   `src/data/`: Tải và tiền xử lý dữ liệu thô.
*   `src/features/`: Trích xuất các Feature Groups và ánh xạ qua `data_dictionary.md`.
*   `src/models/`: Huấn luyện, tối ưu ngưỡng (threshold tuning) và đánh giá mô hình.

## 5. System Constraints & Design Decisions
Validation Strategy: Sử dụng chiến lược phân tách dữ liệu phù hợp để đảm bảo tính khách quan và ngăn ngừa hiện tượng rò rỉ dữ liệu (Data Leakage).

Data Handling Decision: [Ghi chú các quyết định xử lý dữ liệu đặc biệt như chuẩn hóa, mã hóa biến phân loại, hoặc xử lý mất cân bằng dữ liệu].
