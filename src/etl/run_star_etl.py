"""
Star schema ETL: run cleaning pipeline, then load into Dim* and Fact* (SQL Server).
Usage: python -m src.etl.run_star_etl
Requires: StarSchema.sql applied to the database; .env or env vars for SQL Server.
"""
import logging
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

from ..config import PROJECT_ROOT, get_sqlserver_connection_string
from .date_dimension import build_date_dimension
from .cleaning import run_cleaning_pipeline
from .star_loader import (
    load_dim_customer,
    load_dim_date,
    load_dim_payment_method,
    load_dim_product,
    load_dim_promotion,
    load_dim_seller,
    load_fact_invoices,
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

        df_region = results["Region"][0]
        df_sector = results["Sector"][0]
        df_customer = results["Customer"][0]
        df_seller = results["Seller"][0]
        df_product = results["Product"][0]
        df_header = results["SalesHeader"][0]
        df_line = results["SalesLine"][0]
        df_invoice = results["Invoice"][0]

        engine = create_engine(get_sqlserver_connection_string(), fast_executemany=True)

        logger.info("Truncating star schema...")
        truncate_star_schema(engine)

        # DimDate from order + delivery date range
        if df_header.empty or "orderdate" not in df_header.columns:
            logger.warning("No order dates; using default date range for DimDate")
            start_date, end_date = "2024-01-01", "2025-12-31"
        else:
            order_dates = pd.to_datetime(df_header["orderdate"], errors="coerce").dropna()
            deliv = pd.to_datetime(df_header.get("delivdate"), errors="coerce").dropna()
            all_dates = pd.concat([order_dates, deliv])
            if all_dates.empty:
                start_date, end_date = "2024-01-01", "2025-12-31"
            else:
                start_date = str(all_dates.min())[:10]
                end_date = str(all_dates.max())[:10]
        df_dim_date = build_date_dimension(start_date, end_date)
        df_dim_date["IsWeekend"] = df_dim_date["IsWeekend"].astype(int)
        load_dim_date(engine, df_dim_date)

        # DimPromotion from distinct promotype (SalesLine)
        promo_series = df_line["promotype"].dropna().astype(str).str.strip() if "promotype" in df_line.columns else pd.Series(dtype=object)
        promotion_lookup = load_dim_promotion(engine, promo_series)

        # DimPaymentMethod from distinct paymentmethod (Invoice)
        pay_series = (
            df_invoice["paymentmethod"].dropna().astype(str).str.strip()
            if "paymentmethod" in df_invoice.columns
            else pd.Series(dtype=object)
        )
        payment_method_lookup = load_dim_payment_method(engine, pay_series)

        customer_lookup = load_dim_customer(engine, df_customer, df_region, df_sector)
        seller_lookup = load_dim_seller(engine, df_seller)
        product_lookup = load_dim_product(engine, df_product)

        load_fact_sales(
            engine,
            df_header,
            df_line,
            customer_lookup,
            seller_lookup,
            product_lookup,
            promotion_lookup,
        )
        load_fact_invoices(
            engine,
            df_invoice,
            df_header,
            customer_lookup,
            payment_method_lookup,
        )

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
