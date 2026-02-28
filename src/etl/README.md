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
- **cleaning** (`src/etl/cleaning.py`): Stages the data → drops null keys → deduplicates → enforces business rules → checks referential integrity (RI) → outputs clean DataFrames.
- **star_loader** (`src/etl/star_loader.py`): Bulk-inserts via SQLAlchemy. Truncates the tables first in reverse Foreign Key order, then loads dimensions and facts. Integrates `fast_executemany=True` to drastically speed up 10M+ row inserts.
- **check_db** (`src/etl/check_db.py`): Standalone utility to fetch live row counts from the database tables.
