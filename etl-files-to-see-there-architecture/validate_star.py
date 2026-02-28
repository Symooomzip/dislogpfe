"""
Validate star schema after ETL: row counts and basic referential checks.
Run after: python -m etl.run_star_etl
Usage: python -m etl.validate_star
"""
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from .config import PROJECT_ROOT, get_sqlserver_connection_string

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    engine = create_engine(get_sqlserver_connection_string())
    failed = False

    with engine.connect() as conn:
        # Row counts
        tables = ["DimDate", "DimRegion", "DimSector", "DimCustomer", "DimSeller", "DimProduct", "FactOrders", "FactSales", "FactPayments"]
        logger.info("Row counts:")
        for table in tables:
            try:
                r = conn.execute(text(f"SELECT COUNT(*) FROM [{table}]")).scalar()
                logger.info("  %s: %s", table, r)
            except Exception as e:
                logger.error("  %s: %s", table, e)
                failed = True

        # Sanity: FactSales >= FactOrders (more lines than orders)
        try:
            orders = conn.execute(text("SELECT COUNT(*) FROM FactOrders")).scalar()
            sales = conn.execute(text("SELECT COUNT(*) FROM FactSales")).scalar()
            if sales < orders:
                logger.warning("FactSales (%s) < FactOrders (%s); expected sales lines >= orders", sales, orders)
        except Exception as e:
            logger.error("Count check: %s", e)
            failed = True

        # Orphan check: FactSales.order_key all in FactOrders
        try:
            orphan = conn.execute(text(
                "SELECT COUNT(*) FROM FactSales s LEFT JOIN FactOrders o ON s.order_key = o.order_key WHERE o.order_key IS NULL"
            )).scalar()
            if orphan and orphan > 0:
                logger.warning("FactSales rows with missing order: %s", orphan)
                failed = True
            else:
                logger.info("FactSales order_key referential check: OK")
        except Exception as e:
            logger.error("Orphan check: %s", e)
            failed = True

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
