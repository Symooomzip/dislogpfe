"""
Central configuration for the Dislog PFE project.
Database connection, file paths, and analysis constants.
"""

import os
from pathlib import Path

# ──────────────────────────────────────────
# Paths
# ──────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "Data"
NOTEBOOKS_DIR = PROJECT_ROOT / "notebooks"

# ──────────────────────────────────────────
# Raw CSV files
# ──────────────────────────────────────────
CSV_FILES = {
    "region": DATA_DIR / "Region.csv",
    "sector": DATA_DIR / "Sector.csv",
    "customer": DATA_DIR / "Customer.csv",
    "seller": DATA_DIR / "Seller.csv",
    "product": DATA_DIR / "Products.csv",
    "sales_header": DATA_DIR / "SalesHeader.csv",
    "sales_line": DATA_DIR / "SalesLine.csv",
    "invoice": DATA_DIR / "Invoice.csv",
}

# Files that use ANSI (cp1252) encoding instead of UTF-8
ANSI_ENCODED_FILES = {"sales_line", "invoice"}

# ──────────────────────────────────────────
# Database (SQL Server)
# ──────────────────────────────────────────
DB_CONFIG = {
    "driver": "{ODBC Driver 17 for SQL Server}",
    "server": "localhost",       # Change if using a remote server
    "database": "DislogPFE",
    "trusted_connection": "yes", # Windows auth; set to "no" + add user/pass for SQL auth
}

def get_connection_string():
    """Build SQLAlchemy connection string for SQL Server."""
    c = DB_CONFIG
    conn = (
        f"mssql+pyodbc://@{c['server']}/{c['database']}"
        f"?driver={c['driver']}&trusted_connection={c['trusted_connection']}"
    )
    return conn

def get_pyodbc_connection_string():
    """Build raw pyodbc connection string."""
    c = DB_CONFIG
    return (
        f"DRIVER={c['driver']};"
        f"SERVER={c['server']};"
        f"DATABASE={c['database']};"
        f"Trusted_Connection={c['trusted_connection']};"
    )

# ──────────────────────────────────────────
# Analysis constants
# ──────────────────────────────────────────
CHURN_THRESHOLD_DAYS = 90  # Customer inactive > 90 days = churned
RFM_REFERENCE_DATE = None  # Set to None to use max date in data + 1 day
CHUNK_SIZE = 50_000        # For reading large CSVs in chunks
