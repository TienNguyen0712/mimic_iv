# 🩺 MIMIC-IV Medallion Pipeline & Multi-Task Clinical Feature Store

[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/)
[![DuckDB](https://img.shields.io/badge/Database-DuckDB-orange.svg)](https://duckdb.org/)
[![Data Transformation](https://img.shields.io/badge/Transformation-dbt--duckdb-red.svg)](https://docs.getdbt.com/)
[![Orchestration](https://img.shields.io/badge/Orchestration-Prefect-24c2a1.svg)](https://www.prefect.io/)
[![Data Quality](https://img.shields.io/badge/Quality-Great%20Expectations-green.svg)](https://greatexpectations.io/)
[![Architecture](https://img.shields.io/badge/Architecture-Medallion%20(Bronze%E2%86%92Silver%E2%86%92Gold)-gold.svg)]()

## 🎯 1. Mục tiêu & Sứ mệnh Dự án (Project Mission)
Dự án tập trung xây dựng một pipeline xử lý dữ liệu quy mô lớn theo kiến trúc **Medallion (Bronze → Silver → Gold)** nhằm chuẩn hóa dữ liệu lâm sàng từ bộ cơ sở dữ liệu hồi sức cấp cứu chuyên sâu **MIMIC-IV (v3.1)** thành một **Clinical Feature Store** phẳng, có khả năng tái sử dụng linh hoạt cho nhiều bài toán Machine Learning khác nhau trong y tế.

Khác với một Data Warehouse truyền thống chỉ phục vụ cho mục đích truy vấn báo cáo hay tạo các Data Mart đơn lẻ, hệ thống này hướng tới việc tạo ra một **lớp dữ liệu trung gian chuẩn hóa (Unified Feature Store Matrix)** tại tầng Gold. Lớp dữ liệu này sẵn sàng cung cấp trực tiếp các Vector đặc trưng cho các mô hình dự báo lâm sàng cốt lõi:
*   **Mortality Prediction:** Dự báo nguy cơ tử vong trong viện hoặc trong vòng 30 ngày.
*   **Sepsis Prediction:** Dự báo sớm nhiễm khuẩn huyết theo tiêu chuẩn Sepsis-3.
*   **Acute Kidney Injury (AKI) Prediction:** Dự báo suy thận cấp dựa trên động học Creatinine và nước tiểu theo tiêu chuẩn KDIGO.
*   **Shock Prediction:** Dự báo sốc nhiễm khuẩn hoặc sốc tim dựa trên biến động huyết áp và hạ áp.
*   **Length of Stay (LoS) Prediction:** Dự báo số ngày điều trị số hóa tại ICU.
*   **ICU Readmission Prediction:** Dự báo nguy cơ tái nhập khoa hồi sức cấp cứu trong vòng 30 ngày.

---

## 🏥 2. Tại sao chọn MIMIC-IV v3.1?
MIMIC-IV là một trong những bộ dữ liệu mở lớn nhất trong lĩnh vực chăm sóc tích cực (ICU) và được sử dụng rộng rãi như một chuẩn benchmark trong nghiên cứu AI y tế.
*   **Mô hình dữ liệu quan hệ phức tạp:** Chia thành nhiều module (`hosp` và `icu`) với các bảng liên kết qua hệ thống khóa định danh `subject_id`, `hadm_id` và `stay_id`, mô phỏng chính xác hệ thống Hồ sơ bệnh án điện tử (EHR) thực tế.
*   **Dữ liệu chuỗi thời gian quy mô lớn:** Riêng bảng `chartevents` chứa hàng trăm triệu bản ghi về dấu hiệu sinh tồn và các quan sát lâm sàng, đặt ra bài toán tối ưu lưu trữ và tính toán trên thiết bị cá nhân.
*   **Tính thực tiễn lâm sàng cao:** Đòi hỏi xử lý nghiêm ngặt các nghiệp vụ y tế: chuẩn hóa đơn vị đo hỗn hợp, quản lý mốc thời gian ICU, ánh xạ thực thể lâm sàng từ hệ thống `itemid` đa dạng và xử lý cửa sổ tích lũy thời gian.

 Pipeline được xây dựng xoay quanh **08 bảng dữ liệu cốt lõi**: `admissions`, `icustays`, `patients`, `chartevents`, `labevents`, `inputevents`, `outputevents`, và `procedureevents`.

---

## 🏗️ 3. Kiến trúc Pipeline Dữ liệu (Medallion Architecture)

Hệ thống ứng dụng mô hình **ELT (Extract - Load - Transform)** kết hợp sức mạnh xử lý Inside-Memory của **DuckDB** để vận hành lưu trữ dạng cột tối ưu **Apache Parquet**.

```
                     MIMIC-IV Raw (.csv.gz)
                               │
                               ▼ (Bucketing by subject_id Hash & Parquet Conversion)
┌─────────────────────────────────────────────────────────────────────────────────────┐
│ 🥉 BRONZE LAYER: Raw Clinical Data Partitions                                       |
│    - No joins, no normalization. Schemas preserved. Balanced via 8-bucket hashing.  │
└─────────────────────────────────────────────────────────────────────────────────────┘
                                │
                                ▼  (Clinical Outliers Filtering, Metric Conversion & Pivot EAV)
┌─────────────────────────────────────────────────────────────────────────────────────┐
│ 🥈 SILVER LAYER: Clinical Entity Standardization                                    │
│    - Core Tables: patient_master (First ICU stays only, LoS >= 24h), patients_clean │
│    - Standardized Entities: vital_signs, laboratory, medication, outputs, procedures│
│    - Data Quality Guard: Verified via Great Expectations Data Docs                  │
└─────────────────────────────────────────────────────────────────────────────────────┘
                                │
                                ▼ (Multi-Window Dynamic Aggregation via dbt-duckdb)
┌─────────────────────────────────────────────────────────────────────────────────────┐
│ 🥇 GOLD LAYER: Multi-Task Clinical Feature Store                                    │
│    - Feature Groups (1h, 6h, 12h, 24h): Static, Vitals, Labs, Meds, Fluids, Procs   │
│    - Feature Store Matrix: Structured long/wide format ready for downstream ML      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### 🥉 Bronze Layer – Raw Clinical Storage
*   **Mục tiêu:** Lưu trữ nguyên bản, gán ép kiểu dữ liệu tường minh và chuyển đổi định dạng từ `.csv.gz` sang `.parquet`.
*   **Kỹ thuật Bucketing:** Áp dụng thuật toán băm khóa định danh bệnh nhân `abs(hash(subject_id)) % n` để phân rã các bảng sự kiện khổng lồ (`chartevents`, `labevents`) thành n (n tùy ý) phân vùng nhị phân cân bằng, ngăn chặn triệt để lỗi sập nguồn bộ nhớ (Out-of-Memory) khi xử lý tuần tự (Out-of-Core Processing).

### 🥈 Silver Layer – Clinical Entity Standardization
Tập trung làm sạch, lọc nhiễu lâm sàng và chuyển đổi dữ liệu dạng dọc EAV (Entity-Attribute-Value) thành các thực thể phẳng vững chắc:
*   **Xây dựng Patient Master (`patient_master`):** Kết hợp logic `admissions` và `icustays`. Áp dụng hàm phân hạng cửa sổ (`ROW_NUMBER()`) để **chỉ giữ lại đợt nhập ICU đầu tiên** của mỗi đợt nhập viện và áp dụng ràng buộc y tế: thời gian nằm ICU **tối thiểu 24 giờ** (`los >= 1.0`) nhằm bảo toàn cấu trúc cửa sổ quan sát.
*   **Chuẩn hóa hệ đo lường (Metric System Integration):** Đồng bộ toàn bộ các chỉ số sinh học về hệ quy chuẩn quốc tế: Nhiệt độ về độ C ($^{\circ}C$), Chiều cao về `cm` (quy đổi từ inch qua hệ số $2.54$), Cân nặng về `kg` (quy đổi từ lbs qua hệ số $0.45359237$).
*   **Lọc nhiễu sinh học (Clinical Outliers):** Triển khai hàm chặn trên/dưới loại bỏ sai số thiết bị đo (Ví dụ: $30 \le \text{Heart Rate} \le 250$, $50 \le \text{SpO}_2 \le 100$).
*   **Kiểm định chất lượng (Data Quality Guard):** Tích hợp **Great Expectations** để thiết lập các bộ quy tắc (Expectation Suites) kiểm tra tính toàn vẹn của khóa chính, tính hợp lý của mốc thời gian (`intime <= outtime`), sinh tự động báo cáo trực quan (Data Docs) và điều phối Prefect ngắt pipeline ngay khi phát hiện lỗi nghiêm trọng.

### 🥇 Gold Layer – Clinical Feature Store
Sử dụng **dbt-duckdb** để đóng gói chuyển đổi, tạo ra các Feature Group độc lập có tính tái sử dụng cao:
*   **Feature Group 1 (Patient Static Features):** Tuổi, giới tính, chiều cao, cân nặng nền và chỉ số khối cơ thể (BMI) tính toán sẵn từ tầng Silver.
*   **Kiến trúc Đa cửa sổ (Multi-Window Dynamic Aggregation):** Tại các Feature Group động (Vitals, Labs, Meds, Outputs, Procs), áp dụng hàm tính khoảng cách thời gian lũy tiến `date_diff('hour', icu_intime, charttime)` để tích lũy đặc trưng (`MIN`, `MAX`, `MEAN`) theo các lát cắt quan trọng: **1h, 6h, 12h, và 24h** kể từ thời điểm bệnh nhân vào ICU.
*   **Chống rò rỉ dữ liệu (Data Leakage Guard):** Cấu trúc ma trận phẳng `patient_feature_store` phân tách rõ ràng vector đặc trưng theo mốc thời gian snapshot và module sinh Nhãn ($y$) tự động cho 6 bài toán lâm sàng (ví dụ: bốc tách nhãn tử vong `deathtime` từ `admissions` hoặc `dod` từ `patients`), đảm bảo mô hình không nhìn thấy trước dữ liệu tương lai.

---

## 🛠️ 4. Bản đồ Công nghệ (Modern Data Stack)
Dự án tuân thủ triết lý **Local-first Modern Data Stack**, tối ưu hóa tuyệt đối hiệu năng trên máy cá nhân nhưng sẵn sàng mở rộng sang hạ tầng Cloud lớn hơn.

| Thành phần | Công nghệ | Vai trò trong Pipeline |
|------------|-----------|------------------------|
| **Storage Engine** | DuckDB + Apache Parquet | Đọc/Ghi dữ liệu dạng cột hiệu năng cao, thực thi Vectorized on Local Memory. |
| **Data Quality** | Great Expectations | Kiểm định dữ liệu tầng Silver, sinh Data Docs, ngăn chặn lỗi logic lâm sàng. |
| **Transformation** | dbt-duckdb | Quản lý mã nguồn SQL biến đổi tầng Gold, xây dựng Lineage DAG và kiểm soát phiên bản. |
| **Orchestration** | Prefect | Điều phối toàn bộ luồng chạy (Workflow Flow/Tasks), quản lý log và lập lịch tự động. |
| **Configuration** | Hydra | Quản lý tập trung Metadata, danh mục mã hóa `itemid`, đường dẫn hệ thống và cấu hình mô hình. |

---

## ⚡ 5. Những thách thức & Giải pháp Triển khai
*   **Khối lượng dữ liệu cực đại:** Bảng `chartevents` chứa hàng trăm triệu dòng không thể nạp trực tiếp lên RAM. -> *Giải pháp:* Áp dụng Streaming Ingestion, phân mảnh bằng kỹ thuật Hash Bucketing tại tầng Bronze, sử dụng cơ chế thực thi tính toán theo khối (Vectorized Processing) của DuckDB.
*   **Bất đồng bộ thực thể (`itemid`):** Một chỉ số sinh tồn (như nhiệt độ, huyết áp) được ghi nhận bằng nhiều mã `itemid` khác nhau tùy thiết bị. -> *Giải pháp:* Sử dụng **Hydra** để cấu trúc file cấu hình cấu trúc YAML chứa bảng ánh xạ (Mapping Dict), tách biệt hoàn toàn mã nguồn SQL khỏi cấu hình Metadata.
*   **Đồng bộ chuỗi thời gian không đều:** Tần suất lấy mẫu dấu hiệu sinh tồn (vài phút), xét nghiệm (vài giờ/ngày) và dịch truyền (liên tục) hoàn toàn khác nhau. -> *Giải pháp:* Đưa toàn bộ về các cửa sổ thời gian cố định ($1\text{h}, 6\text{h}, 12\text{h}, 24\text{h}$) tính từ mốc `icu_intime`.

---

## 🚀 6. Hướng phát triển Tiếp theo
- [ ] Tích hợp **MLflow** Feature Store để quản lý phiên bản (Versioning) các tập tính năng động.
- [ ] Trích xuất tự động các thang điểm suy đa tạng lâm sàng nâng cao như **SOFA Score**, **NEWS Score**, và chỉ số bệnh nền **Charlson Comorbidity Index** trực tiếp tại tầng Gold.
- [ ] Chuyển đổi linh hoạt môi trường thực thi dbt từ DuckDB sang Snowflake hoặc Google BigQuery thông qua cấu hình Hydra khi mở rộng quy mô dữ liệu sang các hệ thống EHR quốc gia.
