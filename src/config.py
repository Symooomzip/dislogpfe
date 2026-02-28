"""
ETL configuration: paths, encodings, table map, and SQL Server connection.
Single source of truth for the Dislog PFE star schema ETL.
"""
import os
from pathlib import Path
from urllib.parse import quote_plus

# Project root (parent of src/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Data directory: raw CSV files
DATA_DIR = PROJECT_ROOT / "Data"
if not DATA_DIR.exists():
    DATA_DIR = PROJECT_ROOT / "data"

# CSV settings
CSV_DELIMITER = ";"
# Try utf-8 first; Sector, SalesLine, Invoice need cp1252 — try in order
CSV_ENCODINGS = ("utf-8", "utf-8-sig", "cp1252")

# Per-table encoding priority (optional): try cp1252 first for ANSI files
TABLE_ENCODING_ORDER = {
    "Sector": ("cp1252", "utf-8", "utf-8-sig"),
    "SalesLine": ("cp1252", "utf-8", "utf-8-sig"),
    "Invoice": ("cp1252", "utf-8", "utf-8-sig"),
}

# CSV filename to logical table name
TABLE_CSV_MAP = {
    "Region": "Region.csv",
    "Sector": "Sector.csv",
    "Customer": "Customer.csv",
    "Seller": "Seller.csv",
    "Product": "Products.csv",
    "SalesHeader": "SalesHeader.csv",
    "SalesLine": "SalesLine.csv",
    "Invoice": "Invoice.csv",
}

# Order to load raw tables (for reference; star load order is in star_loader)
LOAD_ORDER = ["Region", "Sector", "Customer", "Seller", "Product", "SalesHeader", "SalesLine", "Invoice"]

# Output directory for cleaned CSVs (optional)
CLEANED_DATA_DIR = DATA_DIR / "cleaned"

# Expected column counts per table (validation)
EXPECTED_COLUMNS = {
    "Region": 2,
    "Sector": 2,
    "Customer": 4,
    "Seller": 2,
    "Product": 4,
    "SalesHeader": 9,
    "SalesLine": 8,
    "Invoice": 4,
}


def get_sqlserver_connection_string() -> str:
    """
    Build SQLAlchemy connection string for SQL Server.
    Uses SQLSERVER_CONNECTION_STRING (ODBC-style) or SERVER/DATABASE/USERNAME/PASSWORD from env.
    """
    conn = os.getenv("SQLSERVER_CONNECTION_STRING", "").strip()
    if conn:
        if conn.startswith("odbc:") or (not conn.startswith("mssql+pyodbc://") and "Server=" in conn):
            odbc_str = conn.removeprefix("odbc:").strip()
            return f"mssql+pyodbc://?odbc_connect={quote_plus(odbc_str)}"
        return conn
    server = os.getenv("SQLSERVER_SERVER", "localhost\\SQLEXPRESS")
    database = os.getenv("SQLSERVER_DATABASE", "DislogDWH")
    username = os.getenv("SQLSERVER_USERNAME", "")
    password = os.getenv("SQLSERVER_PASSWORD", "")
    driver = os.getenv("SQLSERVER_DRIVER", "ODBC Driver 17 for SQL Server")
    odbc_parts = [
        f"Driver={{{driver}}}",
        f"Server={server}",
        f"Database={database}",
    ]
    if username and password:
        odbc_parts.extend([f"UID={username}", f"PWD={password}"])
    else:
        odbc_parts.append("Trusted_Connection=yes")
    odbc_parts.append("TrustServerCertificate=yes")
    return f"mssql+pyodbc://?odbc_connect={quote_plus(';'.join(odbc_parts))}"
