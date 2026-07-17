# 📑 TÀI LIỆU ĐẶC TẢ KỸ THUẬT: PIPELINE TIỀN XỬ LÝ & TỔNG HỢP DỮ LIỆU (MEDALLION FEATURE STORE)

---

## 🥉 TẦNG 1: BRONZE LAYER – RAW CLINICAL STORAGE & INGESTION

### 1.1. Giới thiệu cấu trúc bảng và ánh xạ mã danh mục
Tầng Bronze thực hiện nạp dữ liệu nguyên bản từ các file nén `csv.gz` của bộ dữ liệu MIMIC-IV v3.1, thực thi ép kiểu dữ liệu tường minh (Explicit Schema Enforcement Matrix) và xuất bản sang định dạng lưu trữ dạng cột tối ưu **Apache Parquet**. 

Để đọc hiểu các mã định danh sự kiện lâm sàng, tầng này nạp song song hai bảng từ điển tra cứu (Lookup Tables):
*   `d_items`: Định danh nhãn cho các bảng sự kiện ICU (`chartevents`, `inputevents`, `outputevents`, `procedureevents`).
*   `d_labitems`: Định danh nhãn cho bảng xét nghiệm tập trung (`labevents`).

### 1.2. Kỹ thuật Phân mảnh tối ưu RAM (Hash Bucketing Engine)
Đối với các bảng chuỗi thời gian khổng lồ chứa hàng trăm triệu bản ghi như `chartevents` và `labevents`, pipeline áp dụng bộ lọc phân mảnh chủ động

Dữ liệu sẽ được DuckDB ghi trực tiếp ra ổ đĩa theo cấu trúc thư mục phân vùng (`/bronze/chartevents/bucket_id=X/`). Cơ chế này cho phép các tác vụ tính toán ở tầng Silver và Gold có thể streaming dữ liệu tuần tự (Out-of-Core Processing) mà không gây tràn bộ nhớ RAM của thiết bị cá nhân.

---

## 🥈 TẦNG 2: SILVER LAYER – CLINICAL ENTITY STANDARDIZATION

Tầng Silver thực hiện nhiệm vụ làm sạch, chuẩn hóa đơn vị đo lường quốc tế, xử lý bất thường lâm sàng và chuyển đổi dữ liệu cấu trúc EAV (Entity-Attribute-Value) thành các bảng thực thể phẳng vững chắc.

```
                              [bronze_icustays] + [bronze_admissions]
                                                 │
                                                 ▼ (Lọc Lần đầu + ICU Stay >= 24h)
                                      [silver_patient_master]
                                                 │
        ┌───────────────────┬────────────────────┼───────────────────┬───────────────────┐
        ▼                   ▼                    ▼                   ▼                   ▼
 [silver_patients_clean] [silver_height_weight] [silver_vital_signs] [silver_laboratory] [silver_medication] ...
```

### 2.1. Nhóm bảng Hạt nhân (Core Master Tables)

#### A. Bảng Neo chính: `silver_patient_master`
Đóng vai trò là "Xương sống" (Master Anchor) điều hướng toàn bộ pipeline. Bảng này áp dụng hai bộ lọc lâm sàng nghiêm ngặt:
1.  **Chỉ giữ lại đợt nhập ICU đầu tiên** trong mỗi đợt nhập viện của bệnh nhân (Sử dụng hàm cửa sổ `ROW_NUMBER() OVER (PARTITION BY hadm_id ORDER BY intime)`).
2.  **Thời gian nằm ICU tối thiểu 24 giờ** ($\text{Length of Stay} \ge 24\text{h}$) để bảo toàn cửa sổ quan sát đặc trưng.

Các trường thông tin được chuẩn hóa:
*   `stay_id` (VARCHAR - Khóa chính): Mã lượt nằm ICU duy nhất.
*   `hadm_id` (VARCHAR): Mã đợt nhập viện hành chính.
*   `subject_id` (VARCHAR): Mã định danh bệnh nhân.
*   `admission_time` (TIMESTAMP): Thời điểm hoàn tất thủ tục nhập viện (Ép kiểu từ `admittime`).
*   `discharge_time` (TIMESTAMP): Thời điểm hoàn tất thủ tục xuất viện (Ép kiểu từ `dischtime`).
*   `icu_intime` (TIMESTAMP): Thời điểm chính thức vào khoa ICU (Ép kiểu từ `intime`).
*   `icu_outtime` (TIMESTAMP): Thời điểm chính thức rời khoa ICU (Ép kiểu từ `outtime`).
*   `admission_type` (VARCHAR): Mức độ khẩn cấp khi nhập viện (Emergency, Urgent, Elective,...).
*   `careunit` (VARCHAR): Đơn vị chăm sóc tích cực đầu tiên (`first_careunit`).
*   `deathtime` (TIMESTAMP, Nullable): Thời điểm bệnh nhân tử vong trong viện (Phục vụ làm Nhãn mục tiêu).

#### B. Bảng Nhân khẩu học: `silver_patients_clean`
*   `subject_id` (VARCHAR - Khóa chính): Mã định danh bệnh nhân.
*   `gender` (VARCHAR): Giới tính chuẩn hóa sinh học (M/F).
*   `anchor_year` (INTEGER): Năm gốc giả định để đồng bộ hóa dòng thời gian bảo mật.
*   `anchor_age` (INTEGER): Tuổi thực chính xác của bệnh nhân tại năm gốc.

#### C. Bảng Chỉ số nền: `silver_height_weight`
*   `stay_id` (VARCHAR - Khóa chính): Mã lượt nằm ICU.
*   `height_cm` (DOUBLE): Chiều cao chuẩn hóa về đơn vị centimet.
    *   *Logic chuyển đổi:* Nếu `itemid = 226730` giữ nguyên; nếu `itemid = 226707` (inch) thì nhân với $2.54$.
*   `weight_kg` (DOUBLE): Cân nặng chuẩn hóa về đơn vị kilogram.
    *   *Logic chuyển đổi:* Nếu `itemid = 226512` giữ nguyên; nếu `itemid = 226531` (lbs) thì nhân với $0.45359237$.
*   *Màng lọc nhiễu sinh học (Clinical Outliers):* Loại bỏ các sai số do thiết bị ghi nhận, giới hạn nghiêm ngặt $100 \le \text{height\_cm} \le 250$ và $30 \le \text{weight\_kg} \le 300$.

---

### 2.2. Nhóm bảng Thực thể Lâm sàng biến động (Clinical Entities)
*Tất cả các bảng trong nhóm này bắt buộc phải `INNER JOIN` với `silver_patient_master` theo `stay_id` và áp dụng bộ lọc chặn thời gian: `charttime` (hoặc `starttime`) phải nằm trong khoảng từ `icu_intime` đến `icu_outtime`.*

#### A. Thực thể Dấu hiệu Sinh tồn: `silver_vital_signs`
Trích xuất từ bảng `bronze_chartevents`, thực hiện kỹ thuật xoay trục dữ liệu (Pivot) từ dạng dọc EAV sang dạng bảng phẳng theo từng mốc thời gian ghi nhận thực tế.
*   `stay_id` (VARCHAR), `charttime` (TIMESTAMP).
*   Các chỉ số sinh tồn được mapping mã hóa tự động thông qua cấu hình **Hydra Dictionary**:
    *   *Heart Rate* (`itemid`: 220045) | *Respiratory Rate* (`itemid`: 220210)
    *   *Systolic BP* (`itemid`: 220179, 220050) | *Diastolic BP* (`itemid`: 220180, 220051)
    *   *Mean Arterial Pressure (MAP)* (`itemid`: 220052, 220181, 224322)
    *   *SpO₂* (`itemid`: 220277) | *Temperature* (`itemid`: 223761 cho $^{\circ}\text{F}$ -> Quy đổi: $(^{\circ}\text{F} - 32) \times \frac{5}{9}$, `223762` cho $^{\circ}\text{C}$).

#### B. Thực thể Xét nghiệm: `silver_laboratory`
Trích xuất từ bảng `bronze_labevents`. Do bảng gốc lưu theo dòng, hệ thống tiến hành lọc và đồng bộ các chỉ số hóa sinh máu quan trọng sang dòng thời gian ICU:
*   `stay_id` (VARCHAR), `charttime` (TIMESTAMP).
*   Các chỉ số xét nghiệm hạt nhân: *Creatinine* (Mã: 50912), *Lactate* (Mã: 50813), *White Blood Cell - WBC* (Mã: 51301), *Hemoglobin* (Mã: 51222), *Platelets* (Mã: 51265), *Sodium* (Mã: 50983), *Potassium* (Mã: 50971).

#### C. Thực thể Thuốc và Dịch truyền: `silver_medication`
Trích xuất từ bảng `bronze_inputevents` nhằm theo dõi sát lượng dịch và liều lượng vận mạch nạp vào cơ thể.
*   `stay_id` (VARCHAR), `starttime` (TIMESTAMP), `endtime` (TIMESTAMP).
*   `itemid` (INTEGER): Mã loại thuốc/dịch truyền (Vasopressor: Norepinephrine, Epinephrine; Fluids: Normal Saline, Dextrose).
*   `amount` (DOUBLE): Tổng lượng dịch/thuốc đã truyền vào cơ thể.
*   `rate` (DOUBLE): Tốc độ truyền (ví dụ: mcg/kg/phút đối với vận mạch).
*   `statusdescription` (VARCHAR): Trạng thái truyền (FinishedRunning, Stopped, Rewired).

#### D. Thực thể Dịch tiết ra: `silver_outputs`
Trích xuất từ bảng `bronze_outputevents` để theo dõi chức năng bài tiết của thận và các cơ quan.
*   `stay_id` (VARCHAR), `charttime` (TIMESTAMP).
*   `itemid` (INTEGER): Mã loại dịch tiết (Urine Output: 226559, Drain: 226633).
*   `value` (DOUBLE): Thể tích lượng dịch bài tiết thoát ra (đơn vị mL).

#### E. Thực thể Thủ thuật Lâm sàng: `silver_procedures`
Trích xuất từ bảng `bronze_procedureevents` nhằm ghi nhận các can thiệp ngoại vi xâm lấn.
*   `stay_id` (VARCHAR), `starttime` (TIMESTAMP), `endtime` (TIMESTAMP).
*   `itemid` (INTEGER): Mã thủ thuật (Mechanical Ventilation: 225792, Dialysis/CRRT: 225802, Intubation: 224385).
*   `statusdescription` (VARCHAR): Trạng thái tiến trình thủ thuật (FinishedProcess, Canceled).

### 2.3. Chốt chặn Chất lượng Dữ liệu (Great Expectations Assertion)
Trước khi dữ liệu được chuyển giao lên tầng Gold, Prefect kích hoạt cấu phần **Great Expectations** để chạy bộ kiểm tra tự động:
*   `expect_column_values_to_not_be_null` áp dụng cho cặp khóa hạt nhân (`stay_id`, `subject_id`).
*   `expect_column_values_to_be_between` ép các chỉ số sinh tồn và xét nghiệm nằm trong ngưỡng logic lâm sàng sống.
*   Nếu tỷ lệ lỗi bản ghi vượt quá $1\%$, pipeline sẽ tự động phát tín hiệu cảnh báo (Slack/Discord Webhook) và cô lập phân vùng dữ liệu lỗi.

---

## 🥇 TẦNG 3: GOLD LAYER – CLINICAL FEATURE STORE MATRIX

Tại tầng Gold, **dbt-duckdb** chịu trách nhiệm biên dịch mã nguồn SQL thành một mạng lưới tính toán Graph (DAG). Tầng này chuyển đổi toàn bộ dữ liệu chuỗi thời gian động ở tầng Silver thành một **Ma trận Đặc trưng Phẳng (Wide Flat Matrix)**, tích lũy theo các cửa sổ thời gian cố định tính từ thời điểm bệnh nhân bắt đầu nhập khoa ICU (`icu_intime`)

```
            [silver_vital_signs]    [silver_laboratory]    [silver_medication] ...
                      │                      │                      │
                      ▼                      ▼                      ▼
              (Cửa sổ tích lũy thời gian lũy tiến: 1h, 6h, 12h, 24h)
                      │                      │                      │
                      └──────────────┬───────┴──────────────────────┘
                                     ▼
                         [patient_feature_store] ◄── [Target Labels Module]
```
### 3.1. Cấu trúc các Feature Groups Đa cửa sổ (Multi-Window Feature Groups)
Mỗi chỉ số động sẽ được dbt tạo ra các biến phái sinh bằng cách tính toán khoảng cách thời gian:

$$\Delta t = \text{date\_diff}('\text{hour}', \text{icu\_intime}, \text{charttime})$$

Hệ thống tiến hành gộp dữ liệu (Aggregation) theo 4 cửa sổ lũy tiến: **$\Delta t \le 1\text{h}$**, **$\Delta t \le 6\text{h}$**, **$\Delta t \le 12\text{h}$**, và **$\Delta t \le 24\text{h}$**[cite: 1].

*   **Vital Features Group (`vital_features`):** Tính toán `MIN`, `MAX`, `MEAN` cho từng cửa sổ[cite: 1].
    *   *Ví dụ cấu trúc cột:* `HR_mean_6h`, `HR_max_6h`, `MAP_min_1h`, `SpO2_min_24h`[cite: 1].
*   **Laboratory Features Group (`lab_features`):** Do tần suất xét nghiệm thưa hơn, hệ thống sẽ lấy giá trị gần nhất (`LAST_VALUE`) hoặc giá trị cực đại trong cửa sổ thời gian[cite: 1].
    *   *Ví dụ cấu trúc cột:* `creatinine_max_12h`, `lactate_max_24h`[cite: 1].
*   **Medication & Fluid Features Group (`medication_features`):** Tính toán tổng liều lượng tích lũy (Sum Amount) và thời gian duy trì thuốc truyền[cite: 1].
    *   *Ví dụ cấu trúc cột:* `total_vasopressor_dose_24h`, `total_fluid_input_6h`[cite: 1].
*   **Output Features Group (`output_features`):** Tổng thể tích dịch tiết lũy kế theo giờ để hỗ trợ chấm điểm suy thận KDIGO[cite: 1].
    *   *Ví dụ cấu trúc cột:* `total_urine_output_6h`, `total_urine_output_24h`[cite: 1].
*   **Procedure Features Group (`procedure_features`):** Biến phân loại nhị phân (0/1) thể hiện bệnh nhân có đang phải chịu can thiệp xâm lấn trong cửa sổ đó hay không[cite: 1].
    *   *Ví dụ cấu trúc cột:* `is_ventilated_12h`, `is_dialyzed_24h`[cite: 1].

### 3.2. Module Khởi tạo Nhãn Tự động (Multi-Task Automated Labels Module)
Để biến bảng phẳng thành một Feature Store phục vụ trực tiếp cho Machine Learning mà **không bị rò rỉ dữ liệu (Data Leakage)**, hệ thống thiết lập cấu phần tạo nhãn mục tiêu ($y$) độc lập song song[cite: 1]:
*   `label_mortality` (Nhị phân): 1 nếu bệnh nhân có ngày mất `deathtime` nằm trong khoảng thời gian đợt nằm viện này, 0 nếu xuất viện an toàn[cite: 1].
*   `label_sepsis3` (Nhị phân): 1 nếu xuất hiện mốc thời gian nghi ngờ nhiễm khuẩn huyết thỏa mãn đồng thời điều kiện tăng điểm SOFA $\ge 2$ và có kháng sinh kèm cấy máu.
*   `label_aki_stage` (Đa lớp: 0, 1, 2, 3): Chấm điểm suy thận cấp tự động dựa trên tốc độ sụt giảm nước tiểu (`urine_output_6h`) và tỷ lệ tăng vọt của Creatinine so với mốc nền Silver.
*   `label_long_los` (Nhị phân): 1 nếu tổng số ngày nằm ICU `los_days > 7` (điều trị kéo dài), 0 nếu $\le 7$ ngày[cite: 1].

### 3.3. Hợp nhất Ma trận phẳng cuối cùng: `patient_feature_store`
Bước cuối cùng của dbt thực hiện lệnh `LEFT JOIN` tuyệt đối từ bảng nền tĩnh, quét qua toàn bộ các Feature Groups động và tích hợp tập Nhãn mục tiêu[cite: 1]. 

Mỗi dòng trong bảng này đại diện cho **duy nhất một `stay_id`** hoàn chỉnh, sẵn sàng

--- 

Tại tầng Gold, **dbt-duckdb** chịu trách nhiệm biên dịch mã nguồn SQL thành một mạng lưới tính toán Graph (DAG). Tầng này chuyển đổi toàn bộ dữ liệu chuỗi thời gian động ở tầng Silver thành một **Ma trận Đặc trưng Phẳng (Wide Flat Matrix)**, tích lũy theo các cửa sổ thời gian cố định tính từ thời điểm bệnh nhân bắt đầu nhập khoa ICU (`icu_intime`)[cite: 1].
