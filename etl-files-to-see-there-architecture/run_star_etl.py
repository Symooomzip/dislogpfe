"""
Star schema ETL: run cleaning pipeline, then load into Dim* and Fact* (SQL Server).
Usage: python -m etl.run_star_etl
Requires: Schema_Star.sql applied to the database; .env configured for SQL Server.
"""
import logging
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

from .cleaning import run_cleaning_pipeline
from .config import PROJECT_ROOT, get_sqlserver_connection_string
from .date_dimension import build_date_dimension
from .star_loader import (
    get_fact_orders_lookups,
    load_dim_customer,
    load_dim_date,
    load_dim_product,
    load_dim_region,
    load_dim_sector,
    load_dim_seller,
    load_fact_orders,
    load_fact_payments,
    load_fact_sales,
    truncate_star_schema,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    data_dir = PROJECT_ROOT / "Data"
    if not data_dir.exists():
        data_dir = PROJECT_ROOT / "data"
    if not data_dir.exists():
        logger.error("Data directory not found (tried Data/ and data/)")
        return 1

    try:
        logger.info("Running cleaning pipeline...")
        results = run_cleaning_pipeline(data_dir=data_dir, use_unknown=True)
        df_region = results["DimRegion"][0]
        df_sector = results["DimSector"][0]
        df_customer = results["DimCustomer"][0]
        df_seller = results["DimSeller"][0]
        df_product = results["DimProduct"][0]
        df_orders = results["FactOrders"][0]
        df_sales = results["FactSales"][0]
        df_payments = results["FactPayments"][0]

        engine = create_engine(get_sqlserver_connection_string(), fast_executemany=True)

        logger.info("Truncating star schema...")
        truncate_star_schema(engine)

        # DimDate from order + delivery date range
        if df_orders.empty or "orderdate" not in df_orders.columns:
            logger.warning("No order dates; using default date range for DimDate")
            start_date, end_date = "2020-01-01", "2025-12-31"
        else:
            order_dates = pd.to_datetime(df_orders["orderdate"], errors="coerce").dropna()
            deliv_dates = pd.to_datetime(df_orders.get("delivdate"), errors="coerce").dropna()
            all_dates = pd.concat([order_dates, deliv_dates])
            if all_dates.empty:
                start_date, end_date = "2020-01-01", "2025-12-31"
            else:
                start_date = str(all_dates.min())[:10]
                end_date = str(all_dates.max())[:10]
        df_dim_date = build_date_dimension(start_date, end_date)
        df_dim_date["is_weekend"] = df_dim_date["is_weekend"].astype(int)
        load_dim_date(engine, df_dim_date)

        region_lookup = load_dim_region(engine, df_region)
        sector_lookup = load_dim_sector(engine, df_sector)
        customer_lookup = load_dim_customer(engine, df_customer, region_lookup, sector_lookup)
        seller_lookup = load_dim_seller(engine, df_seller)
        product_lookup = load_dim_product(engine, df_product)

        load_fact_orders(engine, df_orders, customer_lookup, seller_lookup)
        order_key_lookup_db, order_customer_date = get_fact_orders_lookups(engine)

        load_fact_sales(
            engine, df_sales, df_orders, order_key_lookup_db,
            customer_lookup, seller_lookup, product_lookup,
        )
        load_fact_payments(engine, df_payments, order_key_lookup_db, order_customer_date)

        logger.info("Star schema ETL finished successfully.")
        return 0
    except FileNotFoundError as e:
        logger.error("File not found: %s", e)
        return 1
    except Exception as e:
        logger.exception("Star ETL failed: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
