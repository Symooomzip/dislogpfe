



# Dislog PFE — Project Report: Codebase Overview & File Reference

This report documents every important file in the project, what it does, and how the code works. It also flags unused or redundant items so you can decide what to keep or remove.

---

## 1. Project overview

- **Goal**: Customer Lifetime Value (CLV), Segmentation, and Recommendation System for Dislog.
- **Current phase**: Phase 1 (Project Setup & ETL) is complete. Data is loaded into a SQL Server star schema.
- **Stack**: Python, pandas, SQLAlchemy/pyodbc, Jupyter. Data lives in SQL Server (Dim* and Fact* tables).

**High-level flow**: Raw CSVs → ETL (clean + load) → Star schema in SQL Server. Downstream: EDA, RFM, segmentation, CLV, recommendations (Phase 2+).

---

## 2. Project structure (folders)

| Folder / file       | Purpose |
|---------------------|--------|
| `Data/`             | Raw CSV files (Region, Sector, Customer, Seller, Products, SalesHeader, SalesLine, Invoice). Optional `Data/cleaned/` after running cleaning. |
| `notebooks/`        | Jupyter notebooks: data profiling (01), future EDA/features (02, …). |
| `src/`              | Main Python package: config, ETL, and placeholders for features, models, viz. |
| `src/etl/`          | ETL pipeline: load CSV, clean, build date dimension, load star schema, resume script, DB check. |
| `src/etl/cleaning/` | Cleaning subpackage: schema definitions, staging, validators, referential integrity, metrics, pipeline. |
| `dashboard/`        | Placeholder for future dashboards (e.g. Streamlit/Power BI). |
| `reports/`          | Placeholder for reports/outputs. |

---

## 3. File-by-file documentation

### 3.1 Root-level files

| File | Role | Details |
|------|------|--------|
| `requirements.txt` | Python dependencies | pandas, numpy, sqlalchemy, pyodbc, scikit-learn, xgboost, matplotlib, seaborn, plotly, jupyter, python-dotenv, mlxtend, tqdm, python-docx. |
| `.gitignore` | Git ignore rules | Standard Python/IDE/env ignores. |
| `StarSchema.sql` | Target DW schema | Defines 6 dimensions (DimDate, DimCustomer, DimSeller, DimProduct, DimPromotion, DimPaymentMethod) and 2 facts (FactSales line-level, FactInvoices). Run this in SSMS before ETL. |
| `Schema.sql` | Source schema (reference) | Normalized source model (Region, Sector, Customer, Seller, Product, SalesHeader, SalesLine, Invoice). Used for documentation; ETL reads from CSV, not from this DB. |
| `Cahier_de_Charge_PFE.md` | Project spec | Cahier des charges (requirements, scope). |
| `Cahier_de_charge_by_dislog.md` | Client requirements | Dislog-provided specification. |
| `star.png` | Diagram | Star schema visual (reference). |

### 3.2 `data_profiling.py` (root)

- **What it does**: Standalone script that reads all 8 raw CSVs, profiles them (shape, dtypes, missing, duplicates, basic stats), and writes a markdown report to `notebooks/data_profiling_report.md`.
- **How it works**: Uses a fixed CSV config (file names, encodings), `;` delimiter, and optional decimal-comma handling. Can use chunked reads for large files (e.g. SalesLine). No dependency on `src/`; run from project root.
- **Used by**: Nothing in the codebase imports it. The notebook `01_data_profiling.ipynb` does similar exploration interactively. So this script is **redundant** with the notebook (see “Unused / redundant items” below).

---

## 4. Configuration

### 4.1 `src/config.py`

- **Role**: Single place for paths, CSV settings, and SQL Server connection.
- **Main contents**:
  - `PROJECT_ROOT`: `src`’s parent.
  - `DATA_DIR`: `Data/` (or `data/` fallback).
  - `CSV_DELIMITER`: `";"`.
  - `CSV_ENCODINGS`: default encoding order; `TABLE_ENCODING_ORDER`: per-table order (e.g. Sector, SalesLine, Invoice use `cp1252` first).
  - `TABLE_CSV_MAP`: logical table name → CSV filename (e.g. `"Product"` → `"Products.csv"`).
  - `CLEANED_DATA_DIR`: `Data/cleaned`.
  - `EXPECTED_COLUMNS`: optional column-count checks.
  - `get_sqlserver_connection_string()`: builds SQLAlchemy connection string from env (`SQLSERVER_SERVER`, `SQLSERVER_DATABASE`, optional `SQLSERVER_USERNAME`/`SQLSERVER_PASSWORD`, or `SQLSERVER_CONNECTION_STRING`).
- **Used by**: All ETL code (load_csv, cleaning staging, run_star_etl, run_star_etl_resume, check_db).

---

## 5. ETL package (`src/etl/`)

### 5.1 `src/etl/load_csv.py`

- **Role**: Load one CSV by logical table name with encoding fallback and optional decimal-comma columns.
- **Logic**: Uses `TABLE_CSV_MAP` and `TABLE_ENCODING_ORDER` (or `CSV_ENCODINGS`). Tries each encoding until one works; optionally normalizes decimal comma to dot for listed columns (e.g. Invoice `paymentamount`). Raises if no encoding works.
- **Used by**: `cleaning/staging.py` only (which calls it for each entity).

### 5.2 `src/etl/date_dimension.py`

- **Role**: Build the DimDate DataFrame for a date range.
- **Logic**: `build_date_dimension(start_date, end_date)` generates one row per day with columns: DateKey (YYYYMMDD int), FullDate, Year, Quarter, Month, Day, DayOfWeek, DayName, MonthName, IsWeekend. Matches `StarSchema.sql` DimDate.
- **Used by**: `run_star_etl.py` only (date range derived from SalesHeader order/delivery dates).

### 5.3 `src/etl/cleaning/` (subpackage)

#### `cleaning/schema.py`

- **Role**: Defines the “contract” for each source entity: key columns, required/optional columns, types, string lengths, business rules.
- **Contents**: `EntitySchema` dataclass (name, source_table, key_columns, required/optional, string/numeric/integer/date columns, max_lengths, business_rules). One constant per entity: REGION, SECTOR, CUSTOMER, SELLER, PRODUCT, SALES_HEADER, SALES_LINE, INVOICE. Also `UNKNOWN_NATURAL_KEY = "__UNKNOWN__"` for RI “unknown” rows.
- **Used by**: staging, validators, referential, pipeline.

#### `cleaning/staging.py`

- **Role**: Load raw CSV for an entity and hand off to the pipeline with normalized column names and validation.
- **Logic**: `load_raw_staging(source_table, data_dir, decimal_comma_columns)` calls `load_csv`, then `normalize_column_names` (strip), and returns (DataFrame, DataQualityMetrics). `load_raw_staging_for_entity(schema, ...)` does the same but uses `schema.source_table` and runs `validate_columns(df, schema)` (required columns must exist).
- **Used by**: `pipeline.py` only.

#### `cleaning/validators.py`

- **Role**: Validation and type coercion so data is SQL Server–ready.
- **Logic**:
  - `validate_columns`: checks required columns exist.
  - `normalize_column_names`: strip column names.
  - `coerce_numeric` / `coerce_integers` / `coerce_dates`: type conversion (with decimal-comma support for numeric).
  - `trim_and_truncate_strings`: strip, replace newlines, truncate to max_lengths.
  - `apply_business_rules`: filter rows by rules (e.g. qty > 0 for SalesLine).
  - `drop_null_keys` / `drop_duplicates_by_key`: drop rows with null keys or duplicate keys (keep first).
  - `cast_for_sql_server`: runs numeric, integer, date, and string trimming/truncation from schema.
- **Used by**: staging (normalize, validate), pipeline (drop nulls, dedup, business rules, cast).

#### `cleaning/referential.py`

- **Role**: Enforce referential integrity: fact FKs must exist in dimension key sets; optionally map invalid keys to `UNKNOWN_NATURAL_KEY`.
- **Logic**: `apply_ri_for_fact(df, fk_specs, use_unknown)` takes a list of (column_name, valid_key_set). For each column, rows with values not in the set are either set to `UNKNOWN_NATURAL_KEY` (if use_unknown) or dropped. Returns (df, dropped_count, mapped_count). `ensure_unknown_in_dimension` adds a single “Unknown” row to a dimension if not already present.
- **Used by**: pipeline (SalesHeader: accountid, sellerid; SalesLine: itemid; Invoice: salesid handling in pipeline).

#### `cleaning/metrics.py`

- **Role**: Track and log data quality metrics per entity.
- **Logic**: `DataQualityMetrics` holds raw_row_count, after_null_drop, after_dedup, after_business_rules, after_ri_filter, final_row_count, plus null_key_dropped, duplicates_removed, business_rule_violations, ri_violations, ri_mapped_to_unknown, null_counts. `log()` prints a summary; `compute_null_counts(df, columns)` returns null counts per column.
- **Used by**: pipeline and staging; metrics are filled during cleaning and logged.

#### `cleaning/pipeline.py`

- **Role**: Orchestrate cleaning for all entities in dependency order and return clean DataFrames for the star loader.
- **Logic**:
  - Dimensions (Region, Sector, Customer, Seller, Product): load → drop null keys → dedup by key → cast → optionally add unknown row → log metrics.
  - SalesHeader: load → drop null keys → dedup → business rules → RI on accountid/sellerid (using valid sets from Customer/Seller) → cast.
  - SalesLine: load → drop null keys → dedup → business rules → filter saleid to valid SalesHeader saleids → RI on itemid (unknown allowed) → cast.
  - Invoice: load → drop null keys → dedup → RI on salesid (valid SaleIDs; optional unknown) → cast.
  - `run_cleaning_pipeline(data_dir, use_unknown)` runs all in order and returns dict entity_name → (clean_df, metrics).
- **Used by**: `run_star_etl.py`, `run_star_etl_resume.py`, `run_cleaning.py`.

#### `cleaning/__init__.py`

- **Role**: Re-exports schema constants, pipeline (run_cleaning_pipeline, clean_*), staging, validators, referential, metrics so callers can `from .cleaning import ...`.

### 5.4 `src/etl/star_loader.py`

- **Role**: Truncate star tables (in reverse FK order) and load cleaned DataFrames into Dim* and Fact* in SQL Server.
- **Constants**: `CHUNKSIZE = 100` (SQL Server parameter limit); `STAR_DELETE_ORDER`: FactInvoices, FactSales, DimPaymentMethod, DimPromotion, DimProduct, DimSeller, DimCustomer, DimDate.
- **Main functions**:
  - `truncate_star_schema(engine)`: DELETE from each table in `STAR_DELETE_ORDER`.
  - `_date_to_key(d)`: convert date to YYYYMMDD int.
  - `load_dim_date(engine, df)`: append DimDate (DateKey, FullDate, Year, Quarter, Month, Day, DayOfWeek, DayName, MonthName, IsWeekend).
  - `load_dim_promotion(engine, promo_types)`: distinct PromoType → DimPromotion; returns PromoType → PromotionKey.
  - `load_dim_payment_method(engine, payment_codes)`: distinct payment codes → DimPaymentMethod; returns code → PaymentMethodKey.
  - `get_customer_lookup_from_db(engine)` / `get_payment_method_lookup_from_db(engine)`: read AccountID→CustomerKey and PaymentMethodCode→PaymentMethodKey from DB (for resume).
  - `load_dim_customer(engine, df_customer, df_region, df_sector)`: join customer to region/sector descriptions, write DimCustomer, return AccountID→CustomerKey.
  - `load_dim_seller` / `load_dim_product`: write dims, return natural key→surrogate key.
  - `load_fact_sales`: join SalesLine to SalesHeader, compute OrderDateKey/DeliveryDateKey, map Customer/Seller/Product/Promotion keys, build line-level measures (Quantity, UnitPrice, LineBruteAmount, LineDiscountAmount, LineNetAmount, LineTaxAmount, LineTotalAmount), append to FactSales.
  - `load_fact_invoices`: map salesid → (accountid, orderdate) from header via a **dictionary lookup** (no merge) to avoid memory blow-up; resolve PaymentDateKey, CustomerKey, PaymentMethodKey; append InvoiceID, SaleID, PaymentDateKey, CustomerKey, PaymentMethodKey, PaymentAmount to FactInvoices.
- **Used by**: `run_star_etl.py`, `run_star_etl_resume.py` (FactInvoices + lookup helpers).

### 5.5 `src/etl/run_star_etl.py`

- **Role**: Full ETL entry point: clean all data, then load the entire star schema.
- **Logic**: Load .env from PROJECT_ROOT; run `run_cleaning_pipeline`; create engine; truncate star schema; build date dimension from header date range; load DimDate, DimPromotion, DimPaymentMethod, DimCustomer, DimSeller, DimProduct; load FactSales then FactInvoices. Exits 0 on success, 1 on error.
- **Run**: `python -m src.etl.run_star_etl` (from project root).

### 5.6 `src/etl/run_star_etl_resume.py`

- **Role**: Load only FactInvoices when a full ETL has already loaded dimensions and FactSales (e.g. after a previous failure at FactInvoices).
- **Logic**: Load .env; get Invoice + SalesHeader from `Data/cleaned/` if present, else run cleaning in memory; create engine; build customer_lookup and payment_method_lookup from existing DimCustomer and DimPaymentMethod; call `load_fact_invoices`. No truncate, no reload of other tables.
- **Run**: `python -m src.etl.run_star_etl_resume`.

### 5.7 `src/etl/run_cleaning.py`

- **Role**: Run only the cleaning pipeline and optionally write cleaned DataFrames to `Data/cleaned/*.csv`.
- **Logic**: Load .env; run `run_cleaning_pipeline`; for each entity, write `{Entity}.csv` to CLEANED_DATA_DIR (semicolon, utf-8). No DB connection.
- **Run**: `python -m src.etl.run_cleaning`.

### 5.8 `src/etl/check_db.py`

- **Role**: Verify DB connection and print row counts for all 8 star tables.
- **Logic**: Create engine from `get_sqlserver_connection_string()` (no dotenv in script; rely on env or run after run_star_etl which loads .env). For each of DimDate, DimCustomer, DimSeller, DimProduct, DimPromotion, DimPaymentMethod, FactSales, FactInvoices: `SELECT COUNT(*)` and print. No dependency on `src.config` path loading; assumes run from project root so `from src.config` works.
- **Run**: `python -m src.etl.check_db` or `python src/etl/check_db.py` from project root.

### 5.9 `src/etl/README.md`

- **Role**: ETL user guide: prerequisites, run commands (full ETL, cleaning only, resume), check_db, and short architecture description. Small inaccuracy: it says “load_csv (src/etl/extract.py)” but the real file is `load_csv.py`.

---

## 6. Notebooks

| File | Role |
|------|------|
| `notebooks/01_data_profiling.ipynb` | Interactive profiling of raw CSVs: load each file, show shape/dtypes/missing/duplicates, basic stats and visuals. Complements or replaces the standalone `data_profiling.py` script. |
| `notebooks/data_profiling_report.md` | Markdown report generated by `data_profiling.py` (or similar). Contains table overview, per-table stats, and quality notes. |

---

## 7. Placeholder packages

- `src/features/__init__.py`, `src/models/__init__.py`, `src/viz/__init__.py`: Empty or minimal; reserved for Phase 2+ (feature engineering, ML models, visualizations).

---

## 8. Other folders (reference / planning)

- **`etl-files-to-see-there-architecture/`**: Copy of an ETL structure from another project. Used only as a **reference** when building `src/etl/`. No code in the main project imports or runs anything from this folder. Safe to delete if you no longer need the reference.
- **`task-imple-walktrhi28-02/`**: Resolved task/implementation/walkthrough docs (e.g. `implementation_plan.md.resolved`, `task.md.resolved`, `walkthrough.md.resolved`). Planning/audit only; not executed by the codebase.

---

## 9. Unused / redundant items — what to delete or keep

Items that are not used by the live pipeline or are redundant with other artifacts:

| Item | Why it’s unused / redundant | Recommendation |
|------|-----------------------------|----------------|
| **`data_profiling.py`** (root) | Standalone script that profiles raw CSVs and writes `notebooks/data_profiling_report.md`. No other code imports it. The notebook `01_data_profiling.ipynb` does the same exploration interactively and is the main profiling artifact. | **Optional delete**: Remove if you rely only on the notebook. Keep if you want a one-command script to regenerate the markdown report without opening the notebook. |
| **`etl-files-to-see-there-architecture/`** | Reference ETL code from another project. Not imported or run anywhere. | **Optional delete**: Remove when you no longer need the reference. |
| **`task-imple-walktrhi28-02/`** | Resolved planning docs. Not code; not executed. | **Keep** if you want the audit trail; **delete** if you only care about the main repo docs (e.g. Cahier_de_Charge_PFE.md, implementation_plan elsewhere). |
| **`Schema.sql`** | Describes the normalized source model. ETL reads from CSV, not from a DB that uses this schema. | **Keep** as documentation of the source model. |
| **`star.png`** | Image of the star schema. | **Keep** as documentation. |

**Summary**: The only file that clearly duplicates functionality is **`data_profiling.py`** (script vs notebook). The **`etl-files-to-see-there-architecture/`** folder is reference-only and can be removed once you no longer need it. The rest are either in use or useful as docs.

---

## 10. Quick reference — who calls what

- **config.py**: used by load_csv, staging (via load_csv), run_star_etl, run_star_etl_resume, check_db.
- **load_csv.py**: used by cleaning/staging only.
- **date_dimension.py**: used by run_star_etl only.
- **cleaning/** (schema, staging, validators, referential, metrics, pipeline): used by run_star_etl, run_star_etl_resume, run_cleaning.
- **star_loader.py**: used by run_star_etl and run_star_etl_resume.
- **run_star_etl.py**, **run_star_etl_resume.py**, **run_cleaning.py**, **check_db.py**: entry points; no other repo code depends on them.

---

*End of report. You can use this to onboard, refactor, or clean up the repo. For anything you decide to delete, run the pipeline and check_db once after to confirm nothing breaks.*
