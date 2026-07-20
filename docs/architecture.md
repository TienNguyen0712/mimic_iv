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
Hệ thống xử lý dữ liệu qua 4 tầng chính:

[Raw Data Sources] ──> [Ingestion & Processing] ──> [Feature Store / Catalog] ──> [Model Training / Inference]


*   **Data Ingestion:** Dữ liệu thô từ các nguồn (chọn: Log hệ thống, DB bản ghi, Event Streaming...).
*   **Processing Layer:** Sử dụng [chọn: Spark/Pandas/SQL] để làm sạch và biến đổi.
*   **Storage Layer:** Quản lý cấu hình bằng Hydra, theo dõi thực nghiệm bằng MLflow. Dữ liệu đặc trưng được lưu trữ tại Feature Catalog dưới dạng [chọn: Parquet/Delta Lake].
*   **Application Layer:** Đầu vào cho các thuật toán Machine Learning (Regression, Classification).

## 3. Configuration & Experiment Tracking Strategy
*   **Configuration Management:** Toàn bộ tham số cấu hình hệ thống (Hyperparameters, Data Paths, Feature Selection) được module hóa thông qua **Hydra**.
*   **Experiment Tracking:** Sử dụng **MLflow** để log:
    *   Parameters (learning rate, max depth, list of features).
    *   Metrics (RMSE, F1-Score, Precision, Recall).
    *   Artifacts (Model weight, Feature Importance plot).

## 4. Pipeline Modules Description
*   `src/data/`: Tải và tiền xử lý dữ liệu thô.
*   `src/features/`: Trích xuất các Feature Groups và ánh xạ qua `data_dictionary.md`.
*   `src/models/`: Huấn luyện, tối ưu ngưỡng (threshold tuning) và đánh giá mô hình.
