# 🩺 Xây dựng Medallion Feature Store Pipeline từ Bộ Dữ Liệu Lâm Sàng MIMIC-IV

# 🎯 Mục tiêu & Lý do chọn đề tài

**Sứ mệnh dự án** là xây dựng một pipeline dữ liệu theo kiến trúc **Medallion (Bronze → Silver → Gold)** nhằm chuẩn hóa dữ liệu lâm sàng từ MIMIC-IV thành một **Feature Store** có khả năng tái sử dụng cho nhiều bài toán Machine Learning trong y tế.

Khác với một Data Warehouse chỉ phục vụ truy vấn và phân tích, dự án hướng tới việc tạo ra một lớp dữ liệu trung gian có thể được sử dụng trực tiếp cho các mô hình dự báo như:

- Mortality Prediction
- Sepsis Prediction
- Acute Kidney Injury (AKI)
- Shock Prediction
- Length of Stay Prediction
- ICU Readmission Prediction

Pipeline được xây dựng xoay quanh **07 bảng dữ liệu cốt lõi** của MIMIC-IV:

```text
admissions
icustays
chartevents
labevents
inputevents
outputevents
procedureevents
patients
```

Các bảng này chứa gần như toàn bộ thông tin về quá trình điều trị của bệnh nhân trong ICU, từ thông tin nhập viện, dấu hiệu sinh tồn, xét nghiệm, thuốc, dịch truyền cho đến các thủ thuật can thiệp. Thay vì chỉ xây dựng các bảng phân tích (Data Mart), dự án hướng tới việc chuẩn hóa dữ liệu thành các **Feature Group** có thể tái sử dụng cho nhiều mô hình học máy khác nhau.

---

# 🏥 Tại sao chọn MIMIC-IV v3.1?

MIMIC-IV là một trong những bộ dữ liệu mở lớn nhất trong lĩnh vực chăm sóc tích cực (ICU) và được sử dụng rộng rãi như một chuẩn benchmark trong nghiên cứu AI y tế.

Lý do lựa chọn:

- **Mô hình dữ liệu quan hệ phức tạp:** dữ liệu được chia thành nhiều module (Hosp và ICU) với các bảng liên kết thông qua `subject_id`, `hadm_id` và `stay_id`, mô phỏng gần giống hệ thống Hồ sơ bệnh án điện tử (Electronic Health Record - EHR) trong thực tế.
- **Dữ liệu chuỗi thời gian quy mô lớn:** bảng `chartevents` chứa hàng trăm triệu bản ghi về dấu hiệu sinh tồn và các quan sát lâm sàng, đặt ra nhiều thách thức về lưu trữ và xử lý.
- **Tính thực tiễn cao:** việc xây dựng pipeline yêu cầu hiểu biết về nghiệp vụ y tế như chuẩn hóa đơn vị đo, xử lý thời gian ICU, chuẩn hóa ItemID và xây dựng đặc trưng theo cửa sổ thời gian.

Những đặc điểm trên khiến MIMIC-IV trở thành bộ dữ liệu phù hợp để xây dựng một Feature Store phục vụ nhiều bài toán học máy.

---

# 🛠️ Kiến trúc tổng thể

```text
                    MIMIC-IV
                        │
        ┌───────────────┼────────────────┐
        │               │                │
 admissions      icustays       chartevents
 labevents       inputevents    outputevents
 procedureevents
                        │
                        ▼
               🥉 Bronze Layer
            Raw Clinical Tables
                        │
                        ▼
               🥈 Silver Layer
      Clinical Entity Standardization
                        │
                        ▼
                🥇 Gold Layer
            Clinical Feature Store
```

Pipeline được xây dựng theo mô hình **ELT (Extract - Load - Transform)**. Dữ liệu sẽ được nạp và lưu trữ trước tại Bronze, sau đó chuẩn hóa tại Silver và cuối cùng xây dựng Feature Store tại Gold.

---

## 🥉 Bronze Layer – Raw Clinical Storage

### Mục tiêu

Bronze là tầng lưu trữ dữ liệu nguyên bản từ MIMIC-IV.

Tầng này **không thực hiện**:

- Join giữa các bảng
- Chuẩn hóa dữ liệu
- Feature Engineering

Nhiệm vụ chính gồm:

- Đọc dữ liệu từ các file `csv.gz`
- Chuẩn hóa kiểu dữ liệu
- Chuyển đổi sang định dạng Parquet
- Partition các bảng có kích thước lớn

Các bảng được lưu giữ nguyên cấu trúc:

```text
bronze/

├── admissions
├── icustays
├── chartevents
├── labevents
├── inputevents
├── outputevents
└── procedureevents
```

#### Lựa chọn DuckDB + Parquet

Dự án sử dụng **DuckDB** kết hợp với **Parquet** để tối ưu hiệu năng trên môi trường Local hoặc Google Colab.

DuckDB được lựa chọn vì:

- Không yêu cầu cụm máy (Cluster).
- Thực thi theo cơ chế Vectorized Execution.
- Truy vấn trực tiếp trên Parquet.
- Hiệu quả hơn Spark đối với tập dữ liệu từ vài chục GB đến vài trăm GB.

Luồng chuyển đổi dữ liệu:

```text
CSV.GZ -> DuckDB -> Partitioned Parquet
```

Bronze đóng vai trò là vùng lưu trữ dữ liệu gốc, đảm bảo khả năng truy xuất và tái xử lý khi cần thiết.

---

# 🥈 Silver Layer – Clinical Entity Standardization

Silver là tầng quan trọng nhất của toàn bộ pipeline.

Thay vì xây dựng Feature ngay, tầng này tập trung **chuẩn hóa dữ liệu thành các thực thể lâm sàng (Clinical Entities)**.

Sau Silver, toàn bộ dữ liệu sẽ được:

- Chuẩn hóa đơn vị đo.
- Mapping từ `itemid` sang tên đặc trưng.
- Chuẩn hóa timestamp.
- Loại bỏ dữ liệu bất thường.
- Kiểm định chất lượng dữ liệu.

## 1. Patient Master

Nguồn dữ liệu:

```text
admissions
+
icustays
```

Sinh bảng:

```text
patient_master
```

Bao gồm:

- stay_id
- hadm_id
- subject_id
- admission_time
- discharge_time
- icu_intime
- icu_outtime
- admission_type
- careunit

Đây là bảng trung tâm để liên kết toàn bộ các bảng còn lại.

---

## 2. Vital Signs

Nguồn:

```text
chartevents
```

Chuẩn hóa các chỉ số:

- Heart Rate
- Systolic Blood Pressure
- Diastolic Blood Pressure
- Mean Arterial Pressure
- Respiratory Rate
- Temperature
- SpO₂
- FiO₂
- Height
- Weight

Sinh bảng:

```text
vital_signs
```

---

## 3. Laboratory

Nguồn:

```text
labevents
```

Chuẩn hóa:

- Creatinine
- Lactate
- White Blood Cell
- Hemoglobin
- Platelet
- AST
- ALT
- Sodium
- Potassium

Sinh bảng:

```text
laboratory
```

---

## 4. Medication

Nguồn:

```text
inputevents
```

Chuẩn hóa:

- Vasopressor
- Intravenous Fluid
- Insulin
- Heparin

Sinh bảng:

```text
medication
```

---

## 5. Outputs

Nguồn:

```text
outputevents
```

Chuẩn hóa:

- Urine Output
- Drain Output
- Dialysis Output

Sinh bảng:

```text
outputs
```

---

## 6. Procedures

Nguồn:

```text
procedureevents
```

Chuẩn hóa:

- Mechanical Ventilation
- Intubation
- Dialysis
- ECMO

Sinh bảng:

```text
procedures
```

---

## Kiểm định chất lượng dữ liệu

Silver cũng là nơi thực hiện kiểm định dữ liệu trước khi chuyển sang Gold.

Một số quy tắc kiểm định gồm:

- Khóa chính (`stay_id`, `hadm_id`) không được thiếu.
- Thời gian ICU phải hợp lệ.
- Chuẩn hóa đơn vị đo của các chỉ số.
- Loại bỏ các giá trị ngoài ngưỡng lâm sàng.
- Mapping chính xác giữa `itemid` và tên đặc trưng.

Công cụ được sử dụng là **Great Expectations**, cho phép sinh báo cáo chất lượng dữ liệu (Data Docs) và tích hợp với Prefect để dừng pipeline khi phát hiện lỗi.

---

# 🥇 Gold Layer – Clinical Feature Store

Khác với Data Mart truyền thống, Gold được thiết kế như một **Feature Store** phục vụ trực tiếp cho Machine Learning.

Mỗi Feature Group được xây dựng từ các Clinical Entity ở tầng Silver.

## Feature Group 1 — Patient Static Features

```text
patient_static_features
```

Bao gồm:

- Age
- Gender
- Height
- Weight
- BMI
- Admission Type
- ICU Type

---

## Feature Group 2 — Vital Features

```text
vital_features
```

Được tổng hợp theo nhiều cửa sổ thời gian:

```text
1 Hour
6 Hours
12 Hours
24 Hours
```

Ví dụ:

- HR_mean
- HR_max
- MAP_mean
- RR_mean
- Temp_max
- SpO₂_min

---

## Feature Group 3 — Laboratory Features

```text
lab_features
```

Ví dụ:

- Creatinine_max
- Lactate_max
- WBC_mean
- Platelet_min

---

## Feature Group 4 — Medication Features

```text
medication_features
```

Ví dụ:

- Total Fluid
- Total Vasopressor Dose
- Vasopressor Duration
- Total Insulin

---

## Feature Group 5 — Output Features

```text
output_features
```

Ví dụ:

- Urine Output (6h)
- Urine Output (24h)
- Drain Output

---

## Feature Group 6 — Procedure Features

```text
procedure_features
```

Ví dụ:

- Mechanical Ventilation
- Dialysis
- ECMO
- Intubation

---

## Unified Patient Feature Store

Toàn bộ các Feature Group sẽ được hợp nhất thành bảng cuối cùng:

```text
patient_feature_store
```

Mỗi bản ghi đại diện cho **một ICU Stay**.

```text
stay_id

↓

Static Features

↓

Vital Features

↓

Laboratory Features

↓

Medication Features

↓

Output Features

↓

Procedure Features
```

Bảng này có thể kết hợp với các nhãn (Labels) như:

- Mortality
- Sepsis
- AKI
- Shock
- Length of Stay
- Readmission

để tạo tập dữ liệu đầu vào cho nhiều mô hình Machine Learning khác nhau mà không cần xây dựng lại pipeline.

---

# 🛠️ Bản đồ Công nghệ

Dự án sử dụng triết lý **Local-first Modern Data Stack**, tối ưu hiệu năng trên máy cá nhân nhưng vẫn tuân theo tư duy của các hệ thống Data Platform hiện đại.

| Thành phần | Công nghệ | Vai trò |
|------------|-----------|---------|
| Storage | DuckDB + Parquet | Bronze Layer |
| Data Quality | Great Expectations | Silver Layer |
| Data Transformation | dbt-duckdb | Gold Layer |
| Workflow Orchestration | Prefect | Điều phối Pipeline |
| Configuration | Hydra | Quản lý ItemID, Metadata và Paths |

---

# ⚡ Những thách thức trong triển khai

## Khối lượng dữ liệu lớn

Bảng `chartevents` chứa hàng trăm triệu bản ghi nên không thể xử lý toàn bộ trên RAM.

Pipeline cần áp dụng:

- Streaming Ingestion
- Partition Parquet
- Truy vấn trực tiếp trên DuckDB

để giảm chi phí bộ nhớ và tăng tốc độ xử lý.

---

## Chuẩn hóa dữ liệu lâm sàng

Một chỉ số lâm sàng có thể xuất hiện dưới nhiều `itemid` khác nhau.

Ví dụ:

- Heart Rate
- Temperature
- Blood Pressure

Do đó cần xây dựng bảng Mapping để chuẩn hóa toàn bộ về cùng một Feature.

---

## Chuẩn hóa chuỗi thời gian

Các sự kiện lâm sàng xảy ra với tần suất khác nhau:

- Heart Rate: vài phút/lần
- Laboratory: vài giờ hoặc vài ngày/lần
- Medication: theo khoảng thời gian truyền

Pipeline cần chuẩn hóa các sự kiện này về các cửa sổ thời gian cố định (1h, 6h, 12h, 24h) nhằm phục vụ Feature Engineering và tránh hiện tượng Data Leakage.

---

# 🚀 Hướng phát triển

- Mở rộng Feature Store theo chuẩn MLOps.
- Tích hợp MLflow để quản lý phiên bản Feature.
- Chuyển đổi linh hoạt giữa DuckDB, Snowflake hoặc BigQuery thông qua Hydra.
- Tích hợp Data Observability bằng Great Expectations và Prefect.
- Xây dựng thêm các Feature Group chuyên biệt như SOFA Score, NEWS Score hoặc Charlson Comorbidity Index để phục vụ các mô hình dự báo lâm sàng.
