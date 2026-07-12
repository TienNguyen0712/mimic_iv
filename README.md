# 🩺 Xây dựng Medallion Data Pipeline từ Bộ Dữ Liệu Lâm Sàng MIMIC-IV

## 🎯 Mục tiêu & Lý do chọn đề tài

**Sứ mệnh dự án:** Thiết lập một pipeline tự động hóa toàn diện theo kiến trúc Medallion: 
**Nạp dữ liệu (Bronze) ──► Làm sạch & Kiểm định (Silver) ──► Tổng hợp Cohort & Feature Store (Gold)** 
từ nguồn dữ liệu y tế thứ cấp.

Dữ liệu lâm sàng từ các khoa Hồi sức tích cực (ICU) là một trong những loại dữ liệu thách thức nhất đối với kỹ sư dữ liệu (Data Engineer) do các đặc thù:
- **Bất đồng bộ nghiêm trọng:** Các chỉ số sinh tồn (vitals) được đo với tần suất hoàn toàn khác nhau (nhịp tim theo từng phút, xét nghiệm máu theo ngày).
- **Đa dạng thực thể:** Sự kết hợp phức tạp giữa dữ liệu cấu trúc (dạng số, danh mục), chuỗi thời gian (time-series) và văn bản tự do (ghi chú lâm sàng).
- **Nhiễu lâm sàng & Dữ liệu khuyết:** Tỷ lệ dữ liệu trống rất cao do bác sĩ chỉ chỉ định đo đạc khi có nhu cầu lâm sàng, kèm theo nhiều sai số hệ thống khi nhập liệu thủ công.

Do đó, việc xây dựng một pipeline chuẩn hóa, đảm bảo tính toàn vẹn và độ tin cậy dữ liệu là nền tảng bắt buộc trước khi đưa vào các mô hình Machine Learning y tế.

---

## 🏥 Tại sao chọn MIMIC-IV v3.1 làm Benchmark?
- **Cấu trúc quan hệ phức tạp:** Gồm nhiều module tách biệt `(hosp, icu)` với hàng chục bảng liên kết, mô phỏng chính xác hệ thống Bệnh án điện tử (EHR) thực tế.
- **Dữ liệu chuỗi thời gian khổng lồ**: Bảng `chartevents chứa` hàng trăm triệu dòng dữ liệu sự kiện lâm sàng, là bài toán tối ưu lưu trữ đĩa và bộ nhớ.
- **Tính thực tế cao:** Đòi hỏi kỹ sư phải có tư duy hiểu biết về miền nghiệp vụ như chuẩn hóa đơn vị đo, xử lý logic tuổi, và phân tách ca bệnh (ICU Stays).

---

## 🛠️ Bản đồ Công nghệ

Dự án lựa chọn triết lý **Local-first Modern Data Stack** – tối ưu hóa hiệu năng vượt trội trên tài nguyên đơn nút (Single-node/Local/Colab) nhưng thừa hưởng 100% tư duy quản trị của các hệ thống Big Data trên Cloud. 

```
      [ ⚙️ Hydra ] ──► Quản lý tập trung tham số cấu hình (Paths, Metadata, ItemIDs)
            │
            ▼
   [ 🛸 Prefect Engine ] ──► Điều phối & Giám sát luồng tác vụ (UI, Retry Logic, Logging)
            │
  ┌─────────┴─────────┬───────────────────────┐
  ▼                   ▼                       ▼
[ 🥉 Bronze Layer ] ──► [ 🥈 Silver Layer ] ──► [ 🥇 Gold Layer ]
  ├── DuckDB          ├── Great Expectations      ├── dbt-duckdb
  └── Parquet Format  └── Data Docs Reports       └── Analytical Marts
```

### 🥉 **Tầng 1 (Brozen) - Nạp và lưu trữ dữ liệu**

Nhiệm vụ của tầng này là load các file dữ liệu thô vào kiến trúc lưu trữ. Làm vậy là do, các file dữ liệu đều là file `csv.gz` lớn hầu hết khi giải nén sẽ tạo thành các file `csv`
với kích thước rất lớn. **Phương pháp lưu trữ đề xuất** có thể dử dụng _**Apache Spark**_ hoặc _**DuckDB**_ rồi sau đó chuyển thành các file này thành về dạng **parquet**. Với các bảng 
nặng phải phân vùng thành các bảng nhỏ dựa theo một bảng nào đó, còn các bảng nhỏ thì giữ nguyên 

Với yêu cầu **Hiệu năng cao trên tài nguyên giới hạn**:

- Spark là vua khi có hàng trăm Terabyte dữ liệu trải dài trên hàng chục máy chủ (Cluster).
Nhưng với dữ liệu dưới 1-2 Terabyte (như MIMIC-IV), Spark chạy rất chậm vì mất thời gian khởi động máy ảo Java (JVM) và phân phối dữ liệu qua mạng.
DuckDB chạy trực tiếp trong tiến trình Python, xử lý dạng vector (Vectorized Engine) nên nhanh hơn Spark từ 10-100 lần trên cùng một máy tính local, cài đặt chỉ mất 5 giây.

- Việc lựa chọn định dạng **Parquet** do khi giải nén thành `csv` có sẽ rất tốn thời gian để có thể xử lý hết thay vào đổ Parquet sẽ nén chỉ còn gáp 5 đến 10GB lần so với kích thước ban đầu.

**Kết luận:** Lựa chọn `DuckDB -> Parquet`

### 🥈 **Tầng 2 (Silver) - Tiền xử lý & Chuẩn hóa quan hệ**

Nhiệm vụ của tầng này là đảm bảo chất lượng cho dữ liệu khi chuyển sang tầng tiếp theo. Tầng này ta sẽ phải trả lời cho các bài toán như:
- Xử lý dữ liệu bị thiếu
- Chuẩn hóa dữ liệu chuỗi thời gian
- Chuẩn hóa đơn vị của mỗi đặc trưng
- ...

Nếu gặp 1 trong số những lỗi kinh điển như ở trên pipeline sẽ dừng và cảnh báo ngay. **Phương pháo kiểm tra dữ liệu được đề xuát** **Great Expectations** hoặc `WHERE` trong SQL

- Nằm giữa tầng Bronze và Silver. **Great Expectations** đảm bảo dữ liệu khi đã bước vào kho Silver phải hoàn toàn sạch, không có bệnh nhân nào có tuổi âm, không có dòng nào bị trống ID.
- Lệnh `WHERE` hay `assert` chỉ báo lỗi chung chung và làm sập nguồn code.
Great Expectations xuất ra một báo cáo chất lượng (Data Docs) dạng giao diện web rất chuyên nghiệp, giúp Data Scientist nhìn vào là biết ngay dữ liệu lỗi ở dòng nào, cột nào để sửa.

**Kết luận:** Lựa chọn `Great Expectations`

### 🥇 **Tầng 3 (Gold) - Chuẩn bị dữ liệu**

Tầng này sẽ thực hiện biến đổi, chạy SQL chuẩn hóa. Tạo Data Mart để phục vụ cho các công việc phân tích hay xây dựng hệ thống học máy, chuẩn đoán, v..v

**Phương pháp đề xuất** có thể sử dụng _**Code SQL**_ thuần hoặc _**dbt-duckdb**_ để thực hiện việc biến đổi dữ liệu 

- Viết SQL bằng câu lệnh `conn.execute("CREATE TABLE...")` trong Python rất dễ lỗi và không thể quản lý khi dự án lớn lên.
dbt tách biệt hoàn toàn phần logic (SQL) ra khỏi phần hạ tầng, tự động quản lý thứ tự chạy của các bảng (bảng nào chạy trước, bảng nào chạy sau) mà không cần hard-code.

- Biến các câu lệnh SQL dài ngoằng ở tầng Gold của Tiến thành các mô hình có cấu trúc. Chỉ tập trung viết logic, dbt sẽ tự lo việc sinh bảng, quản lý dependencies và vẽ sơ đồ luồng đi của dữ liệu

**Kết luận:** Lựa chọn `dbt-duckdb`


### ➕ **Các nhiệm vụ khác**

- Quản lý cấu hình: `Hydra` - không cần mở code ra sửa mỗi khi muốn thêm một `itemid` (như thêm chỉ số nhịp thở, nhiệt độ). Chỉ cần khai báo trong file cấu hình của Hydra hoặc gỡ lệnh đè qua Terminal.
- Điều phối luồng: `Prefect` - Sẽ bọc các hàm Python lại, giao diện Web đẹp mắt để bấm nút "Run", tự động chạy lại nếu mất mạng/lỗi đĩa, và hiển thị biểu đồ thời gian chạy của từng tác vụ.

---

## ⚡ Thách thức lớn trong triển khai

- **Thách thức xử lý Tải trọng lớn:** Bảng `chartevents` chứa hàng trăm triệu bản ghi y tế.
Việc nạp và truy vấn trực tiếp trên bộ nhớ RAM sẽ gây ra lỗi Out-Of-Memory (OOM).
Hệ thống buộc phải áp dụng kỹ thuật Streaming Ingestion và Hive Partitioning (phân vùng dựa trên dải ID hoặc nhóm chỉ số) của DuckDB để chia nhỏ tệp Parquet thành các phân đoạn vật lý trên đĩa.

- **Khủng hoảng Logic Lâm sàng**: Trong MIMIC-IV, để bảo mật danh tính (HIPAA), thời gian sinh của bệnh nhân đã bị xáo trộn.
Việc tính toán chính xác thuộc tính Tuổi lúc nhập viện yêu cầu phải liên kết phức tạp giữa bảng `patients` (anchor_age, anchor_year) với năm của ngày nhập viện `(admittime) `trong bảng `admissions`.
Sai sót trong bước này sẽ làm sai lệch hoàn toàn biến mục tiêu của các mô hình dự báo tử vong hay suy tạng.

- **Sự phân rã của chuỗi thời gian:** Việc gom cụm dữ liệu nhịp tim/huyết áp dày đặc về các Block 4 giờ đòi hỏi áp dụng các hàm toán học làm tròn thời gian (DATE_TRUNC)
kết hợp các hàm Window Function để điền khuyết dữ liệu (Forward Fill/Backward Fill) một cách hợp lý, tránh hiện tượng rò rỉ dữ liệu (Data Leakage) từ tương lai vào quá khứ của ca bệnh.

---

## 🚀 Hướng phát triển mở rộng

- 🌐 **Mở rộng kiến trúc Hybrid-Cloud:** Phát triển khả năng chuyển đổi linh hoạt (Switchable Infrastructure) nhờ cấu hình Hydra.
Cho phép pipeline chạy local bằng `dbt-duckdb` trong giai đoạn thử nghiệm và chuyển đổi cấu hình sang `dbt-snowflake` hoặc `dbt-bigquery` để chạy trên Cloud Enterprise chỉ bằng một dòng lệnh.
- 🛡️ **Triển khai Data Observability theo thời gian thực:** Tích hợp sâu `Great Expectations` với hệ thống cảnh báo tự động của `Prefect` thông qua
Webhook để đẩy thông báo chất lượng dữ liệu tức thời về các kênh Telegram/Slack của đội ngũ vận hành.
- 🤖 **Đóng gói Feature Store tự động phục vụ MLOps:** Chuẩn hóa đầu ra của tầng Gold thành một Feature Store cục bộ.
Kết hợp với `MLflow` để tự động hóa việc gắn nhãn (Versioning) dữ liệu, giúp các mô hình học máy (như XGBoost, LSTM) có thể truy xuất chính xác tập tính năng theo từng phiên bản thời gian của dữ liệu lâm sàng.










