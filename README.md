# Roadmap triển khai: ICU Clinical Data Platform (MIMIC-IV)

**Mục tiêu:** Xây dựng 1 pipeline/platform end-to-end từ raw MIMIC-IV events đến dashboard phục vụ theo dõi bệnh nhân ICU, đủ để đưa vào portfolio apply vị trí Data Engineer.

**Thời lượng:** 8 tuần (làm part-time, ~8-10h/tuần). Có thể co giãn tuỳ thời gian bạn có.

**Nguyên tắc:** Ưu tiên MVP chạy được end-to-end càng sớm càng tốt (cuối tuần 4), sau đó mở rộng và làm đẹp. Đừng cố hoàn thiện từng layer trước khi qua layer tiếp theo.

---

## Tuần 1 — Setup môi trường & Xác định scope dữ liệu

**Việc cần làm:**
- Cài đặt môi trường: Docker, Docker Compose, Postgres (hoặc DuckDB nếu muốn nhẹ hơn), Python (venv/poetry).
- Tải subset dữ liệu MIMIC-IV (demo version công khai nếu chưa có credential đầy đủ — MIMIC-IV Clinical Database Demo trên PhysioNet).
- Từ file mapping bạn đã có, **chọn 1 nhóm bệnh lý làm trọng tâm** (gợi ý: Sepsis/Sốc nhiễm khuẩn — vì file của bạn đã có sẵn gần đủ biến: vitals, lactate, vận mạch, WBC, CRP...).
- Giới hạn scope: chỉ lấy các bảng chartevents, labevents, inputevents, outputevents, procedureevents, patients, admissions, icustays.

**Kết quả cần đạt:** Môi trường chạy được, load raw MIMIC-IV demo vào Postgres thành công (`docker-compose up` chạy 1 lệnh).

---

## Tuần 2 — Raw/Staging Layer (Ingestion)

**Việc cần làm:**
- Viết script Python (dùng pandas hoặc polars) đọc raw CSV/table MIMIC-IV và load vào schema `raw` trong Postgres.
- Không transform gì ở bước này — giữ nguyên dữ liệu gốc, chỉ chuẩn hóa kiểu dữ liệu (datetime, numeric).
- Viết 1 file `mapping.yaml` hoặc `mapping.json` dựa trên chính file Excel/CSV bạn đã làm (item_id → tên biến → nhóm STATIC/DENSE/SPARSE/MEDICATION/PROCEDURE). File này sẽ là "nguồn sự thật" cho toàn bộ pipeline transform sau này.

**Kết quả cần đạt:** Có schema `raw` trong Postgres với dữ liệu thô, có file mapping tái sử dụng được.

---

## Tuần 3 — Transformation Layer, phần 1: STATIC + MEDICATION + PROCEDURE

**Việc cần làm:**
- Build bảng `patient_profile` (STATIC): chiều cao, cân nặng, tiền sử bệnh — xử lý case lbs vs kg như bạn đã note.
- Build bảng `medication_events`: gộp inputevents theo category (Vasopressors, Antibiotics, Fluids, Nutrition), tính tổng liều/amount theo ngày.
- Build bảng `procedure_events`: thở máy, lọc máu (CRRT), thủ thuật chẩn đoán hình ảnh.
- Viết unit test đơn giản cho các hàm transform (dùng pytest).

**Kết quả cần đạt:** 3 bảng transform đầu tiên chạy được bằng script Python thuần (chưa cần Airflow).

---

## Tuần 4 — Transformation Layer, phần 2: DENSE_TIME_SERIES + SPARSE_LAB (MVP hoàn chỉnh)

**Việc cần làm:**
- Build bảng `vitals_hourly`: resample HR, huyết áp (NIBP/IBP), SpO2, nhịp thở, nhiệt độ, glucose theo khung giờ, xử lý forward-fill có giới hạn (VD tối đa 2h).
- Build bảng `lab_results`: join labevents + chartevents, chuẩn hóa đơn vị (VD Creatinine mg/dL → micromol/L nếu cần), tách rõ case Calcium ion hóa vs toàn phần (không gộp).
- Xử lý case đặc biệt: Glasgow Coma Scale (3 thành phần lệch charttime) — quyết định rule ghép (VD: lấy giá trị gần nhất trong cùng 1 giờ).

**Kết quả cần đạt: 🎯 MVP end-to-end chạy được** — từ raw → 5 bảng transform hoàn chỉnh, có thể query ra full profile 1 bệnh nhân theo thời gian. Đây là mốc quan trọng nhất, nên demo được cho người khác xem ở đây.

---

## Tuần 5 — Feature/Serving Layer: Derived Clinical Metrics

**Việc cần làm:**
- Tính **SOFA score** theo giờ/ngày (dùng PaO2/FiO2, Platelet, Bilirubin, GCS, MAP + vận mạch, Creatinine) — đây là điểm nhấn thể hiện domain understanding.
- Tính **Fluid Balance** = tổng inputevents - tổng outputevents theo khung giờ.
- Tính **tổng liều kháng sinh/ngày** thay vì chỉ biến 0/1.
- Lưu các bảng derived này vào schema `mart` (theo tư duy data mart phục vụ use-case cụ thể).

**Kết quả cần đạt:** Bảng `sofa_score_hourly`, `fluid_balance_hourly` sẵn sàng cho dashboard.

---

## Tuần 6 — Orchestration & Data Quality

**Việc cần làm:**
- Chuyển toàn bộ script tuần 2-5 thành DAG trong Airflow, chia rõ task theo layer (ingest → transform → feature), có retry/logging.
- Thêm Great Expectations (hoặc tự viết rule) kiểm tra: outlier sinh lý (VD HR > 300), đơn vị sai, duplicate timestamp, giá trị null bất thường ở cột quan trọng.
- (Tùy chọn) Chuyển phần transform sang dbt để có lineage graph đẹp và dễ maintain hơn — nếu muốn thêm điểm cộng dbt vào CV.

**Kết quả cần đạt:** Pipeline chạy tự động qua Airflow, có báo cáo data quality xuất ra (log hoặc file report).

---

## Tuần 7 — Presentation Layer: Dashboard

**Việc cần làm:**
- Build dashboard đơn giản bằng Streamlit (nhanh, dễ) hoặc Metabase/Superset (chuyên nghiệp hơn, gần công cụ BI thật).
- Nội dung dashboard: chọn bệnh nhân → xem biểu đồ vitals theo thời gian, SOFA score trend, danh sách thuốc vận mạch/kháng sinh đang dùng, fluid balance.
- Đóng gói toàn bộ hệ thống (Postgres + Airflow + Dashboard) bằng Docker Compose để chạy 1 lệnh duy nhất.

**Kết quả cần đạt:** Platform chạy được end-to-end chỉ với `docker-compose up`, có dashboard xem trực quan.

---

## Tuần 8 — Documentation, Polish & Chuẩn bị Portfolio

**Việc cần làm:**
- Viết README chi tiết: kiến trúc hệ thống (kèm diagram), cách chạy, các quyết định kỹ thuật quan trọng (VD tại sao xử lý Calcium như vậy, tại sao chọn resample theo giờ...).
- Viết 1 bài blog/case-study ngắn kể lại quá trình làm (rất hữu ích khi phỏng vấn, thể hiện tư duy không chỉ code).
- Dọn code: tách module rõ ràng, thêm comment, đảm bảo test chạy được, xóa credential/data nhạy cảm nếu public lên GitHub.
- Chuẩn bị sẵn 3-5 câu trả lời cho câu hỏi phỏng vấn kiểu "kể về 1 project bạn tự hào" dựa trên các case xử lý dữ liệu tricky trong project này.

**Kết quả cần đạt:** Repository sẵn sàng để đưa vào CV/portfolio, có thể demo trực tiếp trong buổi phỏng vấn.

---

## Ghi chú thêm

- Nếu thời gian hạn hẹp, có thể rút gọn còn **4 tuần**: gộp Tuần 1+2, Tuần 3+4, Tuần 5+6, Tuần 7+8 — miễn là vẫn giữ đúng thứ tự ưu tiên (MVP trước, làm đẹp sau).
- Không cần MIMIC-IV full (cần xin credential riêng, khá lâu) — bản Demo trên PhysioNet là đủ để chứng minh toàn bộ pipeline.
- Ưu tiên chất lượng documentation và khả năng kể chuyện về các quyết định xử lý dữ liệu hơn là số lượng công nghệ dùng — nhà tuyển dụng DE quan tâm cách bạn tư duy hơn là bạn dùng bao nhiêu tool.
