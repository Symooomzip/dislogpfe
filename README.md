# Dislog PFE — CLV, Segmentation & Recommendation System

Customer analytics project for **Dislog Group**: build a SQL Server **star schema** from raw CSVs, explore the data (EDA + RFM), and later train ML models for **segmentation, churn, CLV, and recommendations**.

---

## Stack & Architecture

- **Language**: Python (pandas, numpy, SQLAlchemy/pyodbc)
- **Database**: SQL Server (star schema in `DislogDWH`)
- **Analytics**: Jupyter Notebooks (EDA, RFM, future ML)
- **BI**: Power BI (ROLAP on the SQL Server star schema)
- **Orchestration**: Python modules in `src/etl/` (no Airflow yet)

High-level flow:

```text
Raw CSVs (Data/) ──> Python ETL (src/etl) ──> SQL Server star schema (StarSchema.sql)
                                      └──> Jupyter notebooks (EDA, RFM, ML)
```

---

## Phase Status

- **Phase 0 — Planning**: ✅ complete  
  Cahier de charge rédigé et validé (Dislog + PFE).

- **Phase 1 — Project Setup & ETL**: ✅ complete  
  - Project structure, virtual env, dependencies, GitHub.  
  - ETL fully loaded to SQL Server **star schema**:
    - `FactSales`: **9,229,070** rows  
    - `FactInvoices`: **1,437,421** rows  
    - `DimCustomer`: **87,653** rows (anonymous names mapped to `CLIENT_{AccountID}`)
  - Data quality checks and referential integrity enforced.

- **Phase 2 — EDA & Feature Engineering**: ✅ complete  
  - `notebooks/01_data_profiling.ipynb`: profiling of raw CSVs.  
  - `notebooks/02_eda.ipynb`: full EDA on the star schema + RFM features + story-telling charts.

- **Phase 3 — ML (Segmentation & Churn)**: 🔄 **next**  
  - K-Means / DBSCAN segmentation on RFM + behavioral features.  
  - Churn prediction (Logistic Regression / Random Forest / XGBoost).  
  - CLV features in `DimCustomer` to be populated via `UPDATE` queries.

Subsequent phases (CLV dashboards, recommendation system, report/defense) are planned but not started.

---

## How to Run the ETL

### 1. Prerequisites

- **SQL Server** installed and reachable (local instance is enough).
- **Database** created (example): `DislogDWH` on SSD for performance.
- Run `StarSchema.sql` in SSMS to create:
  - 6 dimensions: `DimDate`, `DimCustomer`, `DimSeller`, `DimProduct`, `DimPromotion`, `DimPaymentMethod`
  - 2 facts: `FactSales`, `FactInvoices`

Create a `.env` file at the project root:

```env
SQLSERVER_SERVER=localhost\SQLEXPRESS
SQLSERVER_DATABASE=DislogDWH
# Optional:
# SQLSERVER_USERNAME=...
# SQLSERVER_PASSWORD=...
```

Place the raw CSVs in `Data/`:

- `Region.csv`, `Sector.csv`, `Customer.csv`, `Seller.csv`,
- `Products.csv`, `SalesHeader.csv`, `SalesLine.csv`, `Invoice.csv`

### 2. Full ETL (clean + load)

From the project root:

```bash
python -m src.etl.run_star_etl
```

This will:

1. Load CSVs with robust encoding handling (`load_csv.py`).
2. Run the cleaning pipeline (`src/etl/cleaning/`).
3. Truncate star tables in reverse FK order.
4. Build `DimDate` from header date range.
5. Load all dimensions and both fact tables.

### 3. Cleaning only

To generate cleaned CSVs in `Data/cleaned/` without loading SQL Server:

```bash
python -m src.etl.run_cleaning
```

### 4. Resume ETL (FactInvoices only)

If a full ETL failed after loading FactSales but before FactInvoices, you can resume:

```bash
python -m src.etl.run_star_etl_resume
```

This reloads **only FactInvoices** using Invoice + SalesHeader (from `Data/cleaned/` or by re-running cleaning in memory) and existing dimension lookups from the database.

### 5. Check row counts

To verify that all tables are loaded correctly:

```bash
python -m src.etl.check_db
```

Expected ballpark:

- FactSales ~9.2M rows; FactInvoices ~1.4M rows  
- All dimensions fully populated.

---

## How to Run the Notebooks

### 1. Environment

Create/activate your virtual environment and install dependencies:

```bash
pip install -r requirements.txt
```

Start Jupyter:

```bash
jupyter notebook
```

### 2. Notebooks

- `notebooks/01_data_profiling.ipynb`  
  - Profiles each raw CSV in `Data/` (shape, dtypes, missing, duplicates, basic stats, encoding issues).
  - Output report: `notebooks/data_profiling_report.md`.

- `notebooks/02_eda.ipynb`  
  - Connects to SQL Server via `src.config.get_sqlserver_connection_string()`.  
  - Loads the **8 star tables** and performs:
    - Per-table profiling (dimensions + facts) using styled HTML helpers.
    - Date range and FK sanity checks between facts and dimensions.
    - RFM computation at customer level (Recency, Frequency, Monetary).
    - Visualizations: top customers/products, region/sector breakdowns, RFM distributions, RFM correlation heatmap, boxplots, monthly revenue/orders.

**Note**: For machines with lower RAM, you can adapt the notebook to sample facts (e.g. `SELECT TOP (N)` on FactSales / FactInvoices) to avoid `MemoryError`.

---

## Folder Structure

High-level structure (logical, not all files listed):

```text
dislog-pfe/
├─ Data/
│  ├─ Region.csv
│  ├─ Sector.csv
│  ├─ Customer.csv
│  ├─ Seller.csv
│  ├─ Products.csv
│  ├─ SalesHeader.csv
│  ├─ SalesLine.csv
│  └─ Invoice.csv
│  └─ cleaned/           # (optional) written by run_cleaning
│
├─ notebooks/
│  ├─ 01_data_profiling.ipynb
│  ├─ 02_eda.ipynb
│  └─ data_profiling_report.md
│
├─ src/
│  ├─ config.py          # Paths, CSV config, SQL connection
│  └─ etl/
│     ├─ load_csv.py
│     ├─ date_dimension.py
│     ├─ run_star_etl.py
│     ├─ run_star_etl_resume.py
│     ├─ run_cleaning.py
│     ├─ check_db.py
│     ├─ star_loader.py
│     ├─ reload_dim_customer.py
│     └─ cleaning/
│        ├─ schema.py
│        ├─ staging.py
│        ├─ validators.py
│        ├─ referential.py
│        ├─ metrics.py
│        └─ pipeline.py
│
├─ reports/
│  ├─ phase1_report.tex          # Phase 1 report (ETL, star schema)
│  ├─ phase2_report.tex          # Phase 2 report (EDA & features)
│  ├─ cahier_de_charge.tex       # LaTeX Cahier de Charge
│  ├─ star_schema.png            # Star schema diagram (used in reports)
│  └─ *.pdf                      # Generated PDF reports
│
├─ SQL-QUERY/
│  ├─ creation du bd.sql
│  └─ Customer_name.sql
│  └─ StarSchema.sql             # (intended canonical location for schema DDL)
│
├─ task-imple-walktrhi28-02/
│  └─ task.plan.md               # Planning artifact (updated)
│
├─ PROJECT_REPORT.md             # Codebase & file reference (detailed)
├─ CHANGELOG.md                  # High-level ETL / schema / EDA changes
├─ Cahier_de_Charge_PFE.md       # PFE cahier de charge (to move under reports/)
├─ Cahier_de_charge_by_dislog.md # Client spec (to move under reports/)
├─ STARV2.jpg                    # Star schema image (to move under reports/)
├─ Schema.sql                    # Old OLTP schema (obsolete; superseded by StarSchema.sql)
├─ data_profiling.py             # Standalone profiling script (redundant with 01_data_profiling.ipynb)
└─ Dislog Group Data Analysis.pdf# Original Dislog training doc (to group under reports/)
```

---

## Files Flagged for Cleanup / Reorganisation

These files are **not deleted** in this repo but are flagged for future cleanup or reorganisation:

- `data_profiling.py` (root): redundant with `notebooks/01_data_profiling.ipynb` and `notebooks/data_profiling_report.md`.  
  → Suggestion: keep the notebook as the canonical profiling tool.

- `Schema.sql` (root): old normalized OLTP schema, no longer used by the ETL (which reads from CSV and writes the star schema).  
  → Suggestion: keep only as reference in `SQL-QUERY/` or archive it.

- `STARV2.jpg` (root): star schema image used for documentation.  
  → Suggestion: move under `reports/` with other report assets.

- `Cahier_de_Charge_PFE.md`, `Cahier_de_charge_by_dislog.md`, `Cahier de Charge_by_FAKIR.pdf` (root): specification documents.  
  → Suggestion: move under `reports/` to keep the root focused on code.

- `StarSchema.sql` (root): main DDL for the star schema.  
  → Suggestion: canonical location is `SQL-QUERY/StarSchema.sql` (currently still present at root).

- `task-imple-walktrhi28-02/`: temporary planning folder created by Cursor.  
  → Suggestion: safe to delete or archive once the main plan is stable.

No `.py` file in `src/etl/` is currently unused: all are either entrypoints (`run_*`, `check_db`, `reload_dim_customer`) or imported by the cleaning/loader pipeline.

