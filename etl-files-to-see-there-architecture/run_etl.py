"""
ETL runner: load CSVs into SQL Server and build DateDimension.
Target: SQL Server. Run Schema.sql once to create tables, then run this script.
"""
import logging
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .config import PROJECT_ROOT, get_sqlserver_connection_string, LOAD_ORDER
from .load_csv import load_csv
from .date_dimension import build_date_dimension

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Columns that use comma as decimal separator in CSV (e.g. Invoice.paymentamount)
DECIMAL_COMMA_COLUMNS = {"Invoice": ["paymentamount"]}

# Primary key (or required) columns per table: drop rows with null in these before insert
TABLE_KEY_COLUMNS = {
    "Region": ["regionid"],
    "Sector": ["sectorid"],
    "Customer": ["accountid"],
    "Seller": ["sellerid"],
    "Product": ["itemid"],
    "SalesHeader": ["saleid"],
    "SalesLine": ["saleid", "itemid"],
    "Invoice": ["invoiceid"],
}


def get_engine() -> Engine:
    return create_engine(get_sqlserver_connection_string(), fast_executemany=True)


def truncate_all(engine: Engine) -> None:
    """Delete all data in reverse dependency order (respects FK)."""
    delete_order = ["Invoice", "SalesLine", "SalesHeader", "DateDimension", "Product", "Seller", "Customer", "Sector", "Region"]
    with engine.connect() as conn:
        for table in delete_order:
            try:
                result = conn.execute(text(f"DELETE FROM [{table}]"))
                conn.commit()
                logger.info("Cleared table %s", table)
            except Exception as e:
                logger.debug("Clear %s: %s", table, e)
                conn.rollback()


def clean_for_load(table_name: str, df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows with null in key columns so INSERT does not violate NOT NULL."""
    key_cols = TABLE_KEY_COLUMNS.get(table_name)
    if not key_cols:
        key_cols = [df.columns[0]]
    existing = [c for c in key_cols if c in df.columns]
    if not existing:
        return df
    before = len(df)
    df = df.dropna(subset=existing).copy()
    dropped = before - len(df)
    if dropped:
        logger.warning("Dropped %s rows with null key in %s (keys: %s)", dropped, table_name, existing)
    return df


# SQL Server allows max 2100 parameters per batch; (chunksize * n_columns) must be <= 2100.
# SalesLine has 8 columns → max 262 rows/chunk. Use 250 to be safe for all tables.
SQLSERVER_MAX_PARAMS = 2100
DEFAULT_CHUNKSIZE = 250


def load_table(engine: Engine, table_name: str, df: pd.DataFrame, chunksize: int | None = None) -> None:
    """Insert DataFrame into SQL Server table (append)."""
    df = clean_for_load(table_name, df)
    if df.empty:
        logger.warning("No rows to load for %s; skipping.", table_name)
        return
    if chunksize is None:
        n_cols = len(df.columns)
        chunksize = max(1, SQLSERVER_MAX_PARAMS // n_cols) if n_cols else DEFAULT_CHUNKSIZE
        chunksize = min(chunksize, DEFAULT_CHUNKSIZE)  # cap for consistency
    # Ensure date columns are datetime for SQL Server
    for col in df.columns:
        if df[col].dtype == "object" and col in ("orderdate", "delivdate", "full_date"):
            df[col] = pd.to_datetime(df[col], errors="coerce")
    df.to_sql(table_name, engine, if_exists="append", index=False, method="multi", chunksize=chunksize)
    logger.info("Loaded %s rows into %s", len(df), table_name)


def run_etl(refresh: bool = True) -> None:
    engine = get_engine()
    if refresh:
        logger.info("Truncating existing data...")
        truncate_all(engine)

    for table_name in LOAD_ORDER:
        logger.info("Loading %s...", table_name)
        decimal_cols = DECIMAL_COMMA_COLUMNS.get(table_name)
        df = load_csv(table_name, decimal_comma_columns=decimal_cols)
        # Normalize column names: CSV might have different casing
        df.columns = [c.strip() for c in df.columns]
        # Confirm columns are correctly parsed (e.g. Customer must have 4 columns)
        logger.info("%s: columns=%s, shape=%s", table_name, list(df.columns), df.shape)
        print(f"\n{table_name} — columns ({len(df.columns)}): {list(df.columns)}")
        print(df.head())
        load_table(engine, table_name, df)

    # Build and load DateDimension from SalesHeader date range
    logger.info("Building DateDimension...")
    with engine.connect() as conn:
        result = conn.execute(text("SELECT MIN(orderdate) AS min_d, MAX(orderdate) AS max_d FROM SalesHeader"))
        row = result.fetchone()
    if row and row[0] and row[1]:
        start_date, end_date = str(row[0])[:10], str(row[1])[:10]
        df_dim = build_date_dimension(start_date, end_date)
        # SQL Server BIT: use 0/1
        df_dim["is_weekend"] = df_dim["is_weekend"].astype(int)
        load_table(engine, "DateDimension", df_dim)
    else:
        logger.warning("No dates in SalesHeader; skipping DateDimension.")

    logger.info("ETL finished successfully.")


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    try:
        run_etl(refresh=True)
        return 0
    except FileNotFoundError as e:
        logger.error("Data file not found: %s", e)
        return 1
    except Exception as e:
        logger.exception("ETL failed: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
