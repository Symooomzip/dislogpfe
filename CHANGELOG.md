## ETL Pipeline

- **What was built**
  - End-to-end Python ETL that reads raw CSVs from `Data/`, cleans them via `src/etl/cleaning/`, and loads a SQL Server star schema defined in `StarSchema.sql`.
  - Full reload entrypoint `python -m src.etl.run_star_etl` (cleaning + truncate + load all dimensions and facts).
  - Resume entrypoint `python -m src.etl.run_star_etl_resume` that reloads **FactInvoices only** when dimensions and FactSales are already present.
  - Cleaning-only entrypoint `python -m src.etl.run_cleaning` that writes `Data/cleaned/*.csv` without touching the database.
  - Integrity checker `python -m src.etl.check_db` that prints live row counts for all 8 star tables.

- **Problems encountered and how they were fixed**
  - **Chunk size / SQL Server parameter limit**  
    - Problem: initial bulk inserts into Fact tables hit SQL Server parameter limits and failed on large batches.  
    - Fix: introduced a global `CHUNKSIZE = 100` in `star_loader.py` and used `fast_executemany=True` so inserts are chunked safely while remaining fast.
  - **Very long ETL runtime (up to ~6 hours)**  
    - Problem: running the full ETL on ~10.5M raw SalesLine rows and ~1.4M Invoice rows from an HDD took several hours.  
    - Fix: moved the SQL Server database files to SSD (`C:\SQLData`) and recreated `DislogDWH` there (see T-SQL script in project history). Runtime dropped significantly.
  - **Database corruption (CRC error on FactSales)**  
    - Problem: `pyodbc.Error` with CRC (cyclic redundancy check) on FactSales indicated disk-level corruption of the `.mdf` file.  
    - Fix: stopped all writes, ran `DBCC CHECKDB`, then **dropped and recreated** `DislogDWH` on a reliable SSD and reran the entire ETL from scratch.
  - **ETL resume after partial failure**  
    - Problem: if ETL failed after loading FactSales but before FactInvoices, rerunning the full pipeline was expensive.  
    - Fix: added `run_star_etl_resume.py`, which reloads only FactInvoices using cleaned data (or re-cleaning Invoice + SalesHeader in memory) and dimension lookups from the database.
  - **Anonymous / `Unknown` customers in DimCustomer**  
    - Problem: many rows in `DimCustomer.AccountName` were null or `"Unknown"`, making EDA and reporting harder.  
    - Fix: updated the DimCustomer loader so that missing/blank/`Unknown`/`nan` names are replaced by a synthetic `CLIENT_{AccountID}`. Added `src/etl/reload_dim_customer.py` to safely reload only DimCustomer (with FK constraints temporarily disabled) without touching the heavy fact tables.

- **Current status**
  - Full star schema is loaded and validated:
    - `FactSales`: **9,229,070** rows
    - `FactInvoices`: **1,437,421** rows
    - `DimCustomer`: **87,653** rows (all anonymous customers renamed to `CLIENT_{AccountID}`)
  - `check_db` confirms that all 6 dimensions and 2 facts are populated with expected volumes.
  - The ETL is considered **Phase 1: complete**.

---

## Star Schema

- **Design decisions**
  - Chosen a **star schema** with 6 dimensions and 2 fact tables:
    - Dimensions: `DimDate`, `DimCustomer`, `DimSeller`, `DimProduct`, `DimPromotion`, `DimPaymentMethod`.
    - Facts: `FactSales` (line-level sales) and `FactInvoices` (payments).
  - Used **surrogate keys** (integer identity) for all dimensions to decouple analytics from noisy natural keys in the CSV.
  - Implemented **SCD Type 2** for core business entities (Customer, Seller, Product) using `ValidFrom`, `ValidTo`, `IsCurrent` in the dimensions, so historical changes are preserved.
  - Kept **degenerate dimensions** (e.g. `SaleID` in FactSales, `InvoiceID` in FactInvoices) directly in the fact tables for detailed drill-downs.
  - Target architecture is **ROLAP**: Power BI connects directly to SQL Server and generates SQL on the star schema (no SSAS cube).

- **Changes made from original design**
  - **DimCustomer — ML / analytics columns added**  
    - Added future-facing columns to store segmentation and model outputs without recreating the database:  
      `Segment`, `ChurnRisk`, `CLV_Historical`, `CLV_Predicted`, `RFM_Recency`, `RFM_Frequency`, `RFM_Monetary`.  
    - These will be populated in Phase 3–4 via `UPDATE` statements once ML models are trained.
  - **FactSales — header amount columns denormalized**  
    - Added header-level measures at line level for better aggregation performance in Power BI:  
      `HeaderBruteAmount`, `HeaderNetAmount`, `HeaderTaxAmount`, `HeaderTotalAmount`.  
    - Rationale: avoid slow header-level joins over ~9M rows by materializing these amounts on each line.
  - **FactSales — performance indexes**  
    - Created nonclustered indexes on `CustomerKey`, `OrderDateKey`, and `ProductKey` to accelerate common filters and group-bys in BI tools.

- **Final schema description**
  - **DimDate**: calendar attributes with `DateKey` (YYYYMMDD), `FullDate`, `Year`, `Quarter`, `Month`, `Day`, `DayOfWeek`, `DayName`, `MonthName`, `IsWeekend`.
  - **DimCustomer**: customer account attributes and SCD2 tracking, enriched with Region/Sector, plus ML columns (`Segment`, `ChurnRisk`, CLV and RFM features).
  - **DimSeller**: seller master data with SCD2 attributes.
  - **DimProduct**: product catalog with brand and descriptive fields, SCD2.
  - **DimPromotion**: promotion dimension from promotion types on SalesLine.
  - **DimPaymentMethod**: payment method codes and descriptions from Invoice.
  - **FactSales**: line-level sales with foreign keys to all dimensions and line/header-level financial measures.
  - **FactInvoices**: payment facts with foreign keys to Customer, Date, and PaymentMethod, and the `PaymentAmount` measure.

---

## EDA Notebook

- **What was analyzed**
  - All **8 tables** in the star schema: 6 dimensions and 2 fact tables.
  - Shape (row/column counts), dtypes, missing values, distinct key counts, date ranges, and referential integrity.
  - Customer-level RFM features (Recency, Frequency, Monetary) based on FactSales and FactInvoices.

- **Key findings**
  - Customer base: ~87k customers in `DimCustomer`, with a long tail of low-activity clients and a small set of very high-value customers.
  - Product catalog: ~2.3k products with a skewed revenue distribution — a small subset of products drives a large share of revenue.
  - Regional and sector performance: clear differences in revenue and order volumes by region/sector.
  - RFM distributions: highly skewed Monetary and Frequency, with many low-frequency/low-monetary customers and a minority of high-value champions.
  - Segmentation readiness: RFM features are clean and suitable for clustering (K-Means/DBSCAN) and churn modeling in Phase 3.

- **Charts produced (02_eda.ipynb)**
  - Top 10 customers by **Frequency** and **Monetary** (two horizontal bar charts).
  - Revenue and order count by **Region** and by **Sector** (2×2 grid of bar/line charts).
  - Top 15 products by **Revenue** and by **Quantity sold** (horizontal bar charts with truncated names).
  - RFM dashboard (2×3 layout): histograms for Recency/Frequency/Monetary, segment bar chart, RFM correlation heatmap, and boxplot of Monetary by segment.
  - Monthly time series: revenue (line + fill), order count (line + fill), and sales lines (bar chart).

---

## Known Issues / Pending Fixes

- **CLIENT\_\_\_UNKNOWN\_\_ naming / anonymous customers (historic issue)**  
  - Original problem: many customers had `AccountName` set to `Unknown` or null, which created confusing “Unknown” entries in DimCustomer and in all EDA charts.  
  - Implemented fix: ETL now enforces `AccountName = CLIENT_{AccountID}` whenever the original name is null/empty/`Unknown`/`nan`, and `src/etl/reload_dim_customer.py` can safely reload DimCustomer with this rule.  
  - Status: **fixed** in the current warehouse. If the ETL is rerun from scratch, `run_star_etl` and (optionally) `reload_dim_customer.py` must both be used so the CLIENT\_ naming remains consistent.

- **Temporary / redundant code and assets (flagged for cleanup)**  
  - `data_profiling.py` (root): standalone CSV profiling script; functionality is duplicated and extended in `notebooks/01_data_profiling.ipynb`. **Flagged as redundant** — keep notebook as the canonical profiling tool.
  - `Schema.sql` (root): old normalized OLTP schema used only as reference; the project now uses `StarSchema.sql` and the SQL Server star schema. **Flagged as obsolete** — replaced by the star schema.
  - `task-imple-walktrhi28-02/` folder: temporary Cursor planning artifacts (including an older `task.plan.md`). **Flagged as temporary** — safe to delete or archive once the main plan is stable.
  - `STARV2.jpg` (root): star schema image used only in reports. **Flagged to move to** `reports/` (canonical location for documentation assets).
  - `Cahier_de_Charge_PFE.md`, `Cahier_de_charge_by_dislog.md`, and `Cahier de Charge_by_FAKIR.pdf` (root): specification documents. **Flagged to move to** `reports/` so that the root stays code-focused.
  - `StarSchema.sql` (root): core schema DDL. **Flagged to move to** `SQL-QUERY/` to group all SQL scripts together (note: currently still at project root; README documents the intended location).

- **Other considerations**
  - Large fact tables in EDA can still stress memory on low-RAM machines. The notebook offers sampling options (`SELECT TOP (N)`) to mitigate this; for full 9.2M / 1.4M rows, ensure enough free RAM (≥12 GB recommended).  
  - Phase 3+ (ML models and CLV) are not started yet; the ML columns in DimCustomer are placeholders awaiting future updates.

