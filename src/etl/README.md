# ETL Pipeline — Dislog PFE Star Schema

Extract from CSV, clean, and load into SQL Server using the star schema defined in `StarSchema.sql`.

## Prerequisites

1. **SQL Server** with the database created (e.g. `DislogDWH`). Run `StarSchema.sql` in SSMS to create all the `Dim*` and `Fact*` tables before running the pipeline.
2. **Data**: Place raw CSV files in `Data/` (Region.csv, Sector.csv, Customer.csv, Seller.csv, Products.csv, SalesHeader.csv, SalesLine.csv, Invoice.csv). Note: Semicolon delimiter; Sector, SalesLine, and Invoice strictly use `cp1252` encoding.
3. **Environment**: In the project root, create a `.env` file (or set environment variables):
   - `SQLSERVER_SERVER` (e.g. `localhost\SQLEXPRESS` or `zeus\SQLEXPRESS`)
   - `SQLSERVER_DATABASE` (e.g. `DislogDWH`)
   - Optional: `SQLSERVER_USERNAME`, `SQLSERVER_PASSWORD`; if omitted, the connection defaults to Windows Authentication.

## Running the Pipeline

From the project root:

```bash
# 1. Full ETL: clean + bulk load into SQL Server (uses fast_executemany for speed)
python -m src.etl.run_star_etl

# 2. Cleaning only (writes Data/cleaned/*.csv without touching the database)
python -m src.etl.run_cleaning

# 3. Resume: load only FactInvoices (no truncate, no reload of dimensions/FactSales).
# Uses Data/cleaned/*.csv if present (fast); otherwise runs cleaning in memory.
# Extremely useful if a full ETL failed partway after FactSales loaded.
python -m src.etl.run_star_etl_resume
```

## Verifying the Data Load

To verify that your ETL pipeline successfully loaded all the extracted and cleaned rows into the SQL Server Data Warehouse, you can run the built-in database checker. It connects directly to the database and returns the live exact row counts.

```bash
# Check database row counts
python -m src.etl.check_db
```

**Expected Successful Benchmarks:**

- `FactSales`: ~9.2 million rows (cleaned down from ~10.5M raw lines to remove duplicates and nulls).
- `FactInvoices`: ~1.4 million rows.
- All Dimensions (`DimDate`, `DimCustomer`, `DimSeller`, `DimProduct`, `DimPromotion`, `DimPaymentMethod`) should be fully populated.

## Pipeline Architecture & Layout

- **config** (`src/config.py`): Central configuration for paths, CSV mapping, encodings, and the SQL Server connection string.
- **load_csv** (`src/etl/load_csv.py`): Reads CSVs with encoding fallback and optional decimal-comma handling.
- **cleaning** (`src/etl/cleaning/`): Package that stages the data → drops null keys → deduplicates → enforces business rules → checks referential integrity (RI) → outputs clean DataFrames.
- **star_loader** (`src/etl/star_loader.py`): Bulk-inserts via SQLAlchemy. Truncates the tables first in reverse Foreign Key order, then loads dimensions and facts. Uses `fast_executemany=True` for speed.
- **check_db** (`src/etl/check_db.py`): Standalone utility to fetch live row counts from the database tables.

---

## How It Works (Detailed)

### High-level flow

```
Raw CSVs (Data/)  →  load_csv  →  cleaning (staging → validate → dedup → RI → cast)  →  star_loader  →  SQL Server (Dim* / Fact*)
```

The pipeline runs in **dependency order**: dimensions first (Region, Sector, Customer, Seller, Product, then SalesHeader, SalesLine, Invoice), so referential integrity can be enforced during cleaning and again at load time.

---

### 1. Configuration (`src/config.py`)

- **PROJECT_ROOT**, **DATA_DIR**: Where the project and raw CSV folder live (`Data/` or `data/`).
- **CSV_DELIMITER**: `";"` for all source files.
- **TABLE_CSV_MAP**: Maps logical table names (e.g. `"Product"`) to CSV filenames (e.g. `"Products.csv"`).
- **TABLE_ENCODING_ORDER**: For some tables (Sector, SalesLine, Invoice) encoding is tried in a specific order (e.g. `cp1252` first) because the files are ANSI, not UTF-8.
- **CLEANED_DATA_DIR**: `Data/cleaned/`; used when writing or reading cleaned CSVs (run_cleaning, run_star_etl_resume).
- **get_sqlserver_connection_string()**: Builds the SQLAlchemy connection string from `.env` (or env vars): server, database, optional username/password or Windows auth, and TrustServerCertificate. Used by all scripts that connect to the database.

Everything that needs paths or DB connection imports from `config`.

---

### 2. CSV loading (`src/etl/load_csv.py`)

- **Purpose**: Load one CSV by **logical table name** (e.g. `"Invoice"`) with robust encoding and optional decimal handling.
- **How it works**:
  1. Resolves the file path using `TABLE_CSV_MAP` and `DATA_DIR` (e.g. `"Invoice"` → `Data/Invoice.csv`).
  2. Tries encodings in order: per-table order from `TABLE_ENCODING_ORDER` if set, otherwise the global `CSV_ENCODINGS`. Stops at the first encoding that does not raise.
  3. Reads with `pd.read_csv(sep=CSV_DELIMITER, ...)` and `on_bad_lines="skip"`.
  4. Optionally normalizes decimal-comma columns (e.g. `paymentamount` in Invoice) by replacing `,` with `.` and coercing to numeric.
- **Used by**: Only the cleaning package, via `cleaning/staging.py`. No other ETL code reads CSVs directly; they all go through this layer.

---

### 3. Cleaning package (`src/etl/cleaning/`)

Cleaning turns raw CSVs into **clean DataFrames** that the star_loader can map into Dim* and Fact* tables. It runs in a fixed order per entity.

#### 3.1 Schema (`cleaning/schema.py`)

- **EntitySchema**: Dataclass that describes each source entity: key columns, required/optional columns, string/numeric/integer/date columns, max lengths, and business rules (e.g. `qty > 0` for SalesLine).
- **Constants**: One schema per table (REGION, SECTOR, CUSTOMER, SELLER, PRODUCT, SALES_HEADER, SALES_LINE, INVOICE). Each has `source_table` (used to call `load_csv`) and `name` (display/logging).
- **UNKNOWN_NATURAL_KEY**: `"__UNKNOWN__"` — used when a foreign key (e.g. accountid) does not exist in the dimension; the row can be kept and the FK set to this value so it can later map to an "Unknown" dimension row.

All other cleaning modules use these schemas to know what to validate and how to cast.

#### 3.2 Staging (`cleaning/staging.py`)

- **Purpose**: Load raw data for one entity and hand it to the pipeline with normalized column names and basic validation.
- **load_raw_staging(source_table, data_dir, decimal_comma_columns)**:
  1. Calls `load_csv(source_table, ...)` to get a DataFrame.
  2. Normalizes column names (strip whitespace).
  3. Returns the DataFrame and a `DataQualityMetrics` object with the raw row count.
- **load_raw_staging_for_entity(schema, ...)**:
  1. Calls `load_raw_staging(schema.source_table, ...)`.
  2. Runs `validate_columns(df, schema)` to ensure all required columns exist (raises if any are missing).
  3. Returns the same plus metrics with the entity name set from the schema.

The pipeline always uses `load_raw_staging_for_entity` so that each entity is loaded and validated in one step.

#### 3.3 Validators (`cleaning/validators.py`)

- **validate_columns**: Checks that every column in `schema.required_columns` exists in the DataFrame; raises `ValidationError` otherwise.
- **normalize_column_names**: Strips leading/trailing spaces from column names.
- **coerce_numeric / coerce_integers / coerce_dates**: Convert columns to numeric, integer, or datetime; numeric supports decimal-comma (replace `,` with `.`). Invalid values become NaN.
- **trim_and_truncate_strings**: For string columns, strip and replace newlines; optionally truncate to `max_lengths` from the schema.
- **apply_business_rules**: Applies rules defined in `schema.business_rules` (e.g. `qty > 0`). Rows that fail are dropped; returns the filtered DataFrame and the number of rows removed.
- **drop_null_keys**: Drops rows where any of the key columns are null. Returns the DataFrame and the number of rows dropped.
- **drop_duplicates_by_key**: Keeps the first row per key (e.g. per saleid, or per (saleid, itemid)); drops the rest. Returns the DataFrame and the number of duplicates removed.
- **cast_for_sql_server**: Runs the full type-casting for an entity: numeric, integer, date, then string trim/truncate according to the schema. Used at the end of cleaning so the DataFrame is ready for the star_loader.

The pipeline calls these in sequence: drop null keys → dedup → business rules → (for facts) referential integrity → cast.

#### 3.4 Referential integrity (`cleaning/referential.py`)

- **apply_ri_for_fact(df, fk_specs, use_unknown)**:
  - `fk_specs` is a list of `(column_name, valid_key_set)` (e.g. `("accountid", set of valid Customer accountids)`).
  - For each column, any value not in the valid set is either **replaced** by `UNKNOWN_NATURAL_KEY` (if `use_unknown=True`) or the row is **dropped** (if `use_unknown=False`).
  - Returns the DataFrame and counts of rows dropped and rows mapped to unknown.
- **ensure_unknown_in_dimension(df, key_column, unknown_display)**: If the dimension DataFrame does not already contain a row with key `UNKNOWN_NATURAL_KEY`, appends one row with that key and default display values (e.g. `"Unknown"`) for other columns. So every dimension has an "Unknown" row for FKs that were invalid in the source.

SalesHeader is cleaned with valid sets from Customer and Seller; SalesLine with valid SaleIDs from SalesHeader and valid ItemIDs from Product; Invoice with valid SaleIDs. Invalid FKs are mapped to unknown (or dropped if you disable that).

#### 3.5 Metrics (`cleaning/metrics.py`)

- **DataQualityMetrics**: Holds counts at each step (raw, after null drop, after dedup, after business rules, after RI, final) plus counts of null-key drops, duplicates removed, business-rule violations, RI violations, and RI mapped to unknown. Optional: null counts per column, encoding used.
- **log()**: Prints a one-line summary and optional warnings (e.g. "Rows dropped (null in key): N").
- **compute_null_counts(df, columns)**: Returns a dict of column → null count for the given columns.

The pipeline fills these metrics as it runs and calls `metrics.log()` at the end of each entity so you see what was dropped or mapped.

#### 3.6 Pipeline (`cleaning/pipeline.py`)

- **Purpose**: Run cleaning for all entities in the correct order and return a dict `entity_name → (clean_df, metrics)`.
- **Order**: Region → Sector → Customer → Seller → Product → SalesHeader → SalesLine → Invoice. Dimensions do not depend on each other (except Customer uses Region/Sector only for enrichment in the star_loader, not for RI). Facts depend on dimensions and on each other (SalesHeader on Customer/Seller; SalesLine on SalesHeader and Product; Invoice on SalesHeader).
- **Per dimension** (Region, Sector, Customer, Seller, Product):
  1. Load via `load_raw_staging_for_entity(schema, ...)`.
  2. Drop null keys, then deduplicate by key.
  3. Cast for SQL Server.
  4. Optionally add the "Unknown" row via `ensure_unknown_in_dimension`.
  5. Log metrics.
- **SalesHeader**:
  1. Load, drop null keys, dedup.
  2. Apply business rules (if any).
  3. Apply RI for `accountid` (valid Customer accountids) and `sellerid` (valid Seller ids); invalid rows are mapped to unknown (or dropped).
  4. Cast; log metrics.
- **SalesLine**:
  1. Load, drop null keys, dedup, business rules (e.g. qty > 0).
  2. Filter `saleid` to valid SaleIDs from SalesHeader (rows with invalid saleid are dropped).
  3. Apply RI for `itemid` (valid Product itemids); invalid mapped to unknown.
  4. Cast; log metrics.
- **Invoice**:
  1. Load, drop null keys, dedup.
  2. Filter `salesid` to valid SaleIDs from SalesHeader (or map invalid to unknown if desired; in the current code, valid_saleids is required so invalid are dropped or handled as per pipeline logic).
  3. Cast; log metrics.

**run_cleaning_pipeline(data_dir, use_unknown)** runs all of the above and returns the dict of (DataFrame, metrics). This dict is what `run_star_etl` and `run_cleaning` use; `run_star_etl_resume` either reads cleaned CSVs from disk or calls `run_cleaning_pipeline` in memory to get Invoice and SalesHeader only.

---

### 4. Date dimension (`src/etl/date_dimension.py`)

- **Purpose**: Build the DimDate DataFrame for the star schema (one row per calendar day in a range).
- **build_date_dimension(start_date, end_date)**:
  - Accepts strings or datetime-like values; normalizes to a date range.
  - Generates one row per day with: **DateKey** (integer YYYYMMDD), **FullDate**, **Year**, **Quarter**, **Month**, **Day**, **DayOfWeek**, **DayName**, **MonthName**, **IsWeekend** (0/1).
- Used only by **run_star_etl**: the date range is derived from the min/max of order and delivery dates in SalesHeader (or a default range if there are no dates). The result is passed to `star_loader.load_dim_date`.

---

### 5. Star loader (`src/etl/star_loader.py`)

- **Purpose**: Truncate the star schema tables (in reverse FK order) and load the cleaned DataFrames into Dim* and Fact* in SQL Server. Uses chunked inserts (`CHUNKSIZE = 100`) to stay under SQL Server’s parameter limit.
- **truncate_star_schema(engine)**: Deletes all rows from FactInvoices, FactSales, DimPaymentMethod, DimPromotion, DimProduct, DimSeller, DimCustomer, DimDate in that order so no FK constraint is violated.
- **Dimension loaders**:
  - **load_dim_date(engine, df)**: Inserts the date dimension DataFrame as-is (columns must match DimDate).
  - **load_dim_promotion(engine, promo_types)**: Builds distinct PromoType from a Series (e.g. from SalesLine), inserts into DimPromotion, then reads back PromoType → PromotionKey and returns that lookup.
  - **load_dim_payment_method(engine, payment_codes)**: Same idea for payment method codes (e.g. from Invoice); returns code → PaymentMethodKey.
  - **load_dim_customer(engine, df_customer, df_region, df_sector)**: Joins customer to region/sector to get descriptions, renames columns to match DimCustomer, inserts, then returns AccountID → CustomerKey.
  - **load_dim_seller(engine, df)** / **load_dim_product(engine, df)**: Insert and return SellerID → SellerKey and ItemID → ProductKey.
- **Resume helpers**: **get_customer_lookup_from_db(engine)** and **get_payment_method_lookup_from_db(engine)** read the existing DimCustomer and DimPaymentMethod tables and return the same key → surrogate key dicts. Used by run_star_etl_resume so FactInvoices can be loaded without re-running the full ETL.
- **load_fact_sales(engine, df_header, df_line, customer_lookup, seller_lookup, product_lookup, promotion_lookup)**:
  1. Joins SalesLine to SalesHeader on saleid (inner).
  2. Converts orderdate/delivdate to OrderDateKey/DeliveryDateKey (YYYYMMDD int) via `_date_to_key`.
  3. Maps accountid → CustomerKey, sellerid → SellerKey, itemid → ProductKey, promotype → PromotionKey using the lookups.
  4. Builds line-level measures: Quantity, UnitPrice, LineBruteAmount (e.g. httotalamount), LineDiscountAmount (promovalue), LineNetAmount, LineTaxAmount (ttc − ht), LineTotalAmount (ttc).
  5. Drops rows with any null FK, then appends to FactSales in chunks.
- **load_fact_invoices(engine, df_invoice, df_header, customer_lookup, payment_method_lookup)**:
  1. Builds a **dictionary** from SaleID → (accountid, orderdate) from the header (no pandas merge, to avoid memory blow-up when keys contain NaN).
  2. For each invoice row, looks up (accountid, orderdate) by salesid; derives PaymentDateKey from orderdate and CustomerKey from accountid; PaymentMethodKey from paymentmethod.
  3. Drops rows that cannot be resolved; builds the FactInvoices DataFrame (InvoiceID, SaleID, PaymentDateKey, CustomerKey, PaymentMethodKey, PaymentAmount) and appends in chunks.

All inserts use `to_sql(..., method="multi", chunksize=CHUNKSIZE)` and the engine is created with `fast_executemany=True` in the runners for better performance.

---

### 6. Entry points (what you run)

- **run_star_etl.py**  
  1. Loads `.env` from PROJECT_ROOT.  
  2. Runs `run_cleaning_pipeline(data_dir)` to get clean DataFrames for all entities.  
  3. Creates the SQLAlchemy engine.  
  4. Truncates the star schema.  
  5. Builds the date dimension from the header date range and loads DimDate.  
  6. Loads DimPromotion (from SalesLine promotype), DimPaymentMethod (from Invoice paymentmethod), DimCustomer (Customer + Region + Sector), DimSeller, DimProduct; keeps the lookups.  
  7. Loads FactSales (header + line + lookups), then FactInvoices (invoice + header + lookups).  
  8. Exits 0 on success, 1 on error.

- **run_cleaning.py**  
  1. Loads `.env`; runs `run_cleaning_pipeline(data_dir)`.  
  2. Writes each clean DataFrame to `Data/cleaned/{Entity}.csv` (semicolon, UTF-8).  
  3. Does not connect to the database.

- **run_star_etl_resume.py**  
  1. Loads `.env`.  
  2. Gets Invoice and SalesHeader: if `Data/cleaned/Invoice.csv` and `SalesHeader.csv` exist, reads them from disk; otherwise runs `run_cleaning_pipeline` in memory and uses the Invoice and SalesHeader DataFrames.  
  3. Creates the engine; builds customer_lookup and payment_method_lookup from the **existing** DimCustomer and DimPaymentMethod tables.  
  4. Calls only `load_fact_invoices(...)`. No truncate, no reload of dimensions or FactSales. Use this after a full ETL that failed partway (e.g. after FactSales loaded but FactInvoices failed).

- **check_db.py**  
  1. Imports `get_sqlserver_connection_string` from `src.config` (assumes run from project root so `src` is on the path).  
  2. Creates the engine and runs `SELECT COUNT(*)` on each of the eight star tables.  
  3. Prints the row counts. Does not load `.env` itself; you can run it after any script that has already set env, or set env vars before running. Useful to verify that the ETL loaded the expected number of rows.
