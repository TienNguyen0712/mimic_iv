Ngày 1: Thực hiện viết README soạn Tech và demo pipeline

Ngày 2: Thực hiện tổ chức thư mục, configs, hoàn thành Tầng Bronze
```
mimic-feature-store/
│
├── README.md
├── pyproject.toml                 # hoặc requirements.txt / poetry.lock
├── Makefile                       # các lệnh tắt: make bronze, make silver, make gold, make test
├── .env.example                   # biến môi trường (đường dẫn data, config Prefect...)
├── .gitignore
│
├── conf/                          # Hydra configs
│   ├── config.yaml                # entrypoint config
│   ├── paths/
│   │   ├── local.yaml
│   │   └── colab.yaml
│   ├── itemid_mapping/
│   │   ├── vital_signs.yaml
│   │   ├── laboratory.yaml
│   │   ├── medication.yaml
│   │   ├── outputs.yaml
│   │   └── procedures.yaml
│   └── time_windows.yaml          # 1h/6h/12h/24h config
│
├── data/                          # KHÔNG commit — chỉ để chạy local
│   ├── raw/                       # csv.gz gốc từ MIMIC-IV
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── src/
│   └── mimic_pipeline/
│       ├── __init__.py
│       │
│       ├── bronze/
│       │   ├── __init__.py
│       │   ├── ingest.py          # đọc csv.gz -> DuckDB -> parquet
│       │   ├── schema.py          # định nghĩa schema/kiểu dữ liệu từng bảng
│       │   └── partition.py       # logic partition (theo hadm_id/năm...)
│       │
│       ├── silver/
│       │   ├── __init__.py
│       │   ├── patient_master.py
│       │   ├── vital_signs.py
│       │   ├── laboratory.py
│       │   ├── medication.py
│       │   ├── outputs.py
│       │   ├── procedures.py
│       │   ├── mapping.py         # chuẩn hóa itemid -> tên feature
│       │   ├── unit_conversion.py # chuẩn hóa đơn vị đo
│       │   └── validation/
│       │       ├── expectations_suite.py   # Great Expectations suites
│       │       └── checkpoints.py
│       │
│       ├── gold/
│       │   ├── __init__.py
│       │   ├── patient_static_features.py
│       │   ├── vital_features.py
│       │   ├── lab_features.py
│       │   ├── medication_features.py
│       │   ├── output_features.py
│       │   ├── procedure_features.py
│       │   ├── time_windows.py    # logic tổng hợp theo cửa sổ thời gian
│       │   └── unify.py           # ghép thành patient_feature_store
│       │
│       ├── common/
│       │   ├── duckdb_utils.py    # kết nối, query helper
│       │   ├── io_utils.py        # đọc/ghi parquet
│       │   ├── logging_utils.py
│       │   └── constants.py
│       │
│       └── orchestration/
│           ├── flows.py           # Prefect flows (bronze_flow, silver_flow, gold_flow)
│           └── tasks.py           # Prefect tasks
│
├── dbt/                            # nếu tách riêng dbt-duckdb project cho Gold
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── silver/
│   │   └── gold/
│   ├── tests/                     # dbt tests (schema tests, custom tests)
│   └── macros/
│
├── great_expectations/             # GE project (nếu không nhúng trong src/)
│   ├── great_expectations.yml
│   ├── expectations/
│   ├── checkpoints/
│   └── data_docs/
│
├── notebooks/                      # EDA, thử nghiệm — KHÔNG chứa logic pipeline chính thức
│   ├── 01_explore_chartevents.ipynb
│   └── 02_validate_gold_features.ipynb
│
├── tests/
│   ├── conftest.py                 # fixtures dùng chung (duckdb connection tạm, sample data)
│   ├── fixtures/
│   │   ├── sample_admissions.csv
│   │   ├── sample_icustays.csv
│   │   ├── sample_chartevents.csv
│   │   └── ...
│   │
│   ├── unit/
│   │   ├── bronze/
│   │   │   ├── test_ingest.py
│   │   │   └── test_schema.py
│   │   ├── silver/
│   │   │   ├── test_patient_master.py
│   │   │   ├── test_vital_signs.py
│   │   │   ├── test_mapping.py
│   │   │   └── test_unit_conversion.py
│   │   └── gold/
│   │       ├── test_vital_features.py
│   │       ├── test_time_windows.py
│   │       └── test_unify.py
│   │
│   ├── integration/
│   │   ├── test_bronze_to_silver.py
│   │   ├── test_silver_to_gold.py
│   │   └── test_full_pipeline_smoke.py
│   │
│   └── data_quality/
│       └── test_great_expectations_suites.py
│
├── scripts/
│   ├── run_bronze.py
│   ├── run_silver.py
│   ├── run_gold.py
│   └── run_full_pipeline.py
│
└── docs/
    ├── architecture.md
    ├── data_dictionary.md          # mapping itemid <-> feature name
    └── feature_catalog.md          # mô tả từng Feature Group
```
