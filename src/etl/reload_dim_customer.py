"""
Reload only DimCustomer with the CLIENT_{AccountID} fix for anonymous customers.
Disables FK constraints temporarily to avoid reloading FactSales and FactInvoices.
Usage: python -m src.etl.reload_dim_customer
"""
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from ..config import PROJECT_ROOT, get_sqlserver_connection_string
from .cleaning import clean_customer, clean_region, clean_sector
from .cleaning.schema import SALES_HEADER
from .cleaning.staging import load_raw_staging_for_entity
from .star_loader import load_dim_customer

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

    engine = create_engine(get_sqlserver_connection_string(), fast_executemany=True)

    try:
        logger.info("Disabling FK constraints...")
        with engine.connect() as conn:
            conn.execute(text("ALTER TABLE FactSales NOCHECK CONSTRAINT ALL"))
            conn.execute(text("ALTER TABLE FactInvoices NOCHECK CONSTRAINT ALL"))
            conn.execute(text("DELETE FROM DimCustomer"))
            conn.commit()

        logger.info("Reloading DimCustomer with CLIENT_ fix (including header-only accounts)...")
        df_region, _ = clean_region(data_dir)
        df_sector, _ = clean_sector(data_dir)
        df_customer, _ = clean_customer(data_dir)
        # Load raw SalesHeader staging BEFORE RI, to see all real accountid values
        df_header_raw, _ = load_raw_staging_for_entity(SALES_HEADER, data_dir=data_dir)
        df_header_accounts = df_header_raw[["accountid"]].dropna().drop_duplicates()
        load_dim_customer(engine, df_customer, df_region, df_sector, df_header_accounts)

        logger.info("Re-enabling FK constraints...")
        with engine.connect() as conn:
            conn.execute(text("ALTER TABLE FactSales CHECK CONSTRAINT ALL"))
            conn.execute(text("ALTER TABLE FactInvoices CHECK CONSTRAINT ALL"))
            conn.commit()

        logger.info(
            "Done. Verify with: SELECT TOP 20 AccountName FROM DimCustomer WHERE AccountName LIKE 'CLIENT_%%'"
        )
        return 0

    except Exception as e:
        logger.exception("Failed: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
