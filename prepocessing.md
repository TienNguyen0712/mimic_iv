# Các đặc trưng thuộc các bảng cần lựa chọn 

## Giới thiệu các bảng được lựa chọn 

Dự án bao gồm 8 bảng và mỗi bảng sẽ lấy các đặc trưng quan trọng như: 

- `admissions`: Lưu trữ thông tin nhập viện nội trú của bệnh nhân thuộc module **hosp** 
- `icustays`: Lưu trữ thông tin các ca bệnh trong ICU thuộc module **icu** 
- `chartevents`: Lưu trữ thông tin các chỉ số lưu tại giường bệnh của bệnh nhân trong khoa ICU thuộc module **icu** iên kết với `d_items` để đọc nhãn 
- `labevents`: Kết quả xét nghiệm thuộc trong module **hosp** gồm cả nội và ngoại trú trong bệnh viện liên kết với `d_labitems` để đọc nhãn 
- `inputevents`: Theo dõi cân bằng dịch và thuốc truyền tính mạch vào bệnh nhân thuộc module **icu**
- `outputevents`: Theo dõi cân bằng dịch và thuốc truyền tĩnh mạch ra từ bệnh nhân thuộc module **icu**
- `procedureevents`: Thông tin về các thủ thuật lâm sàng trong quá trình hồi sức thuộc module **icu** liên kết với `d_items` dọc nhãn
- `patients`: Thuộc module **hosp** lưu trữ nhân khẩu học của bệnh nhân

Ngoài ra còn có các bảng phụ như `d_items` và `d_labitems` nhằm định danh các nhãn theo các id khác nhau có trong những bảng trên

## Các đặc trưng thuộc mỗi bảng cần lựa chọn 

Để lựa chọn các đặc trưng của mỗi bảng ta sẽ dựa vào mục tiêu của **Tầng Silver** trong pipeline và thực hiện 

### Xây dựng bảng chính `core` liên kết với các bảng còn lại 

- `patients_master`

| Đặc trưng     | Mô tả     | Vị trí bảng     |
|-----------|-----------|-----------|
| stay_id | Mã lượt nằm ICU | icustay |
| hadm_id | Mã đợt nhập viện |  |
| subject_id | Mã bệnh nhân |  |
| intime | Thời điểm vào ICU |  |
| outtime | Thời điểm rời ICU  |  |
| first_careunit | Đơn vị chăm sóc đầu tiên |  |
| admission_type | Mức độ khẩn cấp khi nhập viện | admissions |
| dischtime | Thời điểm hoàn tất thủ tục xuất viện |  |
| admittime | Thời điểm hoàn tất thủ tục nhập viện |  |

- `patients_clean` 

| Đặc trưng     | Mô tả     | Vị trí bảng     |
|-----------|-----------|-----------|
| subject_id | Mã bệnh nhân | patients |
| gender | Giới tính |  |
| anchor_year | Năm gốc (ảo) gán làm mốc của bệnh nhân |  |
| anchor_age | tuổi thực của bệnh nhân (tại anchor_year) |  |

- `weight_height`  

| Đặc trưng     | Mô tả     | Vị trí bảng     |
|-----------|-----------|-----------|
| stay_id | Mã lưu trữ lần ở và đi của bệnh nhân  | chartevents |
| itemid | Mã định danh thông số  |  |


### Xây dựng các bảng còn lại nhằm liên kết với bảng **core**

- `vital_signs`

| Đặc trưng     | Mô tả     | Vị trí bảng  |
|-----------|-----------|-----------|
| stay_id | Mã lưu trữ lần ở và đi của bệnh nhân  | chartevents |
| itemid | Mã định danh thông số  |  |
| icu_intime | Mã định danh thông số  | patient_master (bảng **core** liên kết) |
| icu_outtime | Mã định danh thông số  |  |


- `laboratory`

| Đặc trưng     | Mô tả     | Vị trí bảng     |
|-----------|-----------|-----------|
| stay_id | Mã lưu trữ lần ở và đi của bệnh nhân  | patient_master (bảng **core** liên kết) |
| icu_outtime | Thời điểm vào ICU  |  |
| icu_intime | Thời điểm ra ICU | |
| itemid | Mã định danh thông số  | labevents |

- `medication`

| Đặc trưng     | Mô tả     | Vị trí bảng     |
|-----------|-----------|-----------|
| stay_id | Mã lưu trữ lần ở và đi của bệnh nhân  | patient_master (bảng **core** liên kết) |
| itemid | Mã định danh thông số  | inputevents |
| amount | Tổng lượng truyền  |  |
| rate | Tốc độ truyền   |  |
| statusdescription | Trạng thái |  |

- `outputs`

| Đặc trưng     | Mô tả     | Vị trí bảng     |
|-----------|-----------|-----------|
| stay_id | Mã lưu trữ lần ở và đi của bệnh nhân  | patient_master (bảng **core** liên kết) |
| itemid | Mã định danh thông số  | outputevents |
| charttime | Thời điểm ghi nhận  |  |
| value | Lượng dịch bài tiết (thoát ra)  |  |

- `procedures` 

| Đặc trưng     | Mô tả     | Vị trí bảng     |
|-----------|-----------|-----------|
| stay_id | Mã lưu trữ lần ở và đi của bệnh nhân  | patient_master (bảng **core** liên kết) |
| itemid | Mã định danh thông số  | procedureevents |
| starttime | Thời điểm ban đầu |   |
| endtime | Thời điểm kết thúc |  |
| statusdescription | Trạng thái thủ thuật  |  |

# Quy trình bước tiền xử lý và Tổng hợp dữ liệu 

