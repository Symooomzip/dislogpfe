"""
Resume star ETL: load only tables that did not complete (e.g. FactInvoices).
Uses cleaned CSVs from Data/cleaned/ if present; otherwise runs cleaning in memory.
Does not truncate or reload dimensions or FactSales.
Usage: python -m src.etl.run_star_etl_resume
Requires: Full ETL has run at least once (dimensions + FactSales already in DB).
"""
import logging
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

from ..config import CLEANED_DATA_DIR, CSV_DELIMITER, PROJECT_ROOT, get_sqlserver_connection_string
from .cleaning import run_cleaning_pipeline
from .star_loader import (
    get_customer_lookup_from_db,
    get_payment_method_lookup_from_db,
    load_fact_invoices,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

CLEANED_CSV_ENCODING = "utf-8"


def load_cleaned_csv(cleaned_dir: Path, table_name: str) -> pd.DataFrame:
    """Read a single cleaned CSV from Data/cleaned/ (same format as run_cleaning writes)."""
    path = cleaned_dir / f"{table_name}.csv"
    if not path.exists():
        raise FileNotFoundError(f"Cleaned file not found: {path}")
    return pd.read_csv(path, sep=CSV_DELIMITER, encoding=CLEANED_CSV_ENCODING, low_memory=False)


def get_cleaned_invoice_and_header(cleaned_dir: Path, data_dir: Path):
    """
    Return (df_invoice, df_header) from disk if cleaned CSVs exist, else from cleaning pipeline.
    """
    invoice_path = cleaned_dir / "Invoice.csv"
    header_path = cleaned_dir / "SalesHeader.csv"
    if invoice_path.exists() and header_path.exists():
        logger.info("Using cleaned CSVs from %s", cleaned_dir)
        return load_cleaned_csv(cleaned_dir, "Invoice"), load_cleaned_csv(cleaned_dir, "SalesHeader")
    logger.info("Cleaned CSVs not found; running cleaning pipeline in memory...")
    results = run_cleaning_pipeline(data_dir=data_dir, use_unknown=True)
    return results["Invoice"][0], results["SalesHeader"][0]


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")

    data_dir = PROJECT_ROOT / "Data"
    if not data_dir.exists():
        data_dir = PROJECT_ROOT / "data"
    if not data_dir.exists():
        logger.error("Data directory not found (tried Data/ and data/)")
        return 1

    cleaned_dir = Path(CLEANED_DATA_DIR)

    try:
        df_invoice, df_header = get_cleaned_invoice_and_header(cleaned_dir, data_dir)
        logger.info("Invoice: %s rows | SalesHeader: %s rows", len(df_invoice), len(df_header))

        engine = create_engine(get_sqlserver_connection_string(), fast_executemany=True)

        logger.info("Building lookups from existing dimension tables...")
        customer_lookup = get_customer_lookup_from_db(engine)
        payment_method_lookup = get_payment_method_lookup_from_db(engine)
        logger.info(
            "Customer lookup: %s keys | PaymentMethod lookup: %s keys",
            len(customer_lookup),
            len(payment_method_lookup),
        )

        logger.info("Loading FactInvoices (only)...")
        load_fact_invoices(
            engine,
            df_invoice,
            df_header,
            customer_lookup,
            payment_method_lookup,
        )

        logger.info("Resume ETL finished. FactInvoices loaded successfully.")
        return 0
    except FileNotFoundError as e:
        logger.error("%s", e)
        return 1
    except Exception as e:
        logger.exception("Resume ETL failed: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
