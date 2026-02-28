"""
ETL configuration: paths, encodings, table order, and SQL Server connection.
"""
import os
from pathlib import Path
from urllib.parse import quote_plus

# Project root (parent of etl/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Data directory: CSV files (supports both Data/ and data/)
DATA_DIR = PROJECT_ROOT / "Data"
if not DATA_DIR.exists():
    DATA_DIR = PROJECT_ROOT / "data"

# CSV settings
CSV_DELIMITER = ";"
CSV_ENCODINGS = ("utf-8", "utf-8-sig", "cp1252")  # try in order for each file

# Load order: dimensions first (no FKs), then facts (depend on dimensions)
DIMENSION_TABLES = ["Region", "Sector", "Customer", "Seller", "Product"]
FACT_TABLES = ["SalesHeader", "SalesLine", "Invoice"]
LOAD_ORDER = DIMENSION_TABLES + FACT_TABLES

# CSV filename to table name (e.g. Products.csv -> Product)
TABLE_CSV_MAP = {
    "Region": "Region.csv",
    "Sector": "Sector.csv",
    "Customer": "Customer.csv",
    "Seller": "Seller.csv",
    "Product": "Products.csv",  # schema table is Product
    "SalesHeader": "SalesHeader.csv",
    "SalesLine": "SalesLine.csv",
    "Invoice": "Invoice.csv",
}

# Output directory for cleaned CSVs (used by clean_data.py; ETL loads from here if present)
CLEANED_DATA_DIR = DATA_DIR / "cleaned"

# Expected number of columns per table (for validation during clean). Must match CSV header.
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

# Max string length per column (schema NVARCHAR sizes). Keys: table -> { column_name: max_len }.
# Columns not listed are not truncated (e.g. numeric, date).
COLUMN_MAX_LENGTHS = {
    "Region": {"regionid": 50, "description": 100},
    "Sector": {"sectorid": 50, "description": 100},
    "Customer": {"accountid": 50, "accountname": 100, "regionid": 50, "sectorid": 50},
    "Seller": {"sellerid": 50, "sellername": 100},
    "Product": {"itemid": 50, "name": 100, "namealias": 100, "marque": 50},
    "SalesHeader": {"accountid": 50, "sellerid": 50},
    "SalesLine": {"itemid": 50, "promotype": 50},
    "Invoice": {"invoiceid": 50, "paymentmethod": 3},
}

# --- SQL Server (target database) ---
# Prefer SQLSERVER_CONNECTION_STRING (full URL or ODBC-style).
# For named instances (e.g. localhost\SQLEXPRESS), use ODBC format in .env:
#   SQLSERVER_CONNECTION_STRING=odbc:Driver={ODBC Driver 17 for SQL Server};Server=localhost\SQLEXPRESS;Database=DislogDW;UID=user;PWD=pass
# Or set SERVER (e.g. localhost\SQLEXPRESS), DATABASE, USERNAME, PASSWORD.
def get_sqlserver_connection_string() -> str:
    conn = os.getenv("SQLSERVER_CONNECTION_STRING", "").strip()
    if conn:
        # If it's an ODBC-style string (no "://"), use odbc_connect so instance names work
        if conn.startswith("odbc:") or (not conn.startswith("mssql+pyodbc://") and "Server=" in conn):
            odbc_str = conn.removeprefix("odbc:").strip()
            return f"mssql+pyodbc://?odbc_connect={quote_plus(odbc_str)}"
        return conn
    server = os.getenv("SQLSERVER_SERVER", "localhost")
    database = os.getenv("SQLSERVER_DATABASE", "DislogDW")
    username = os.getenv("SQLSERVER_USERNAME", "")
    password = os.getenv("SQLSERVER_PASSWORD", "")
    driver = os.getenv("SQLSERVER_DRIVER", "ODBC Driver 17 for SQL Server")
    # Use ODBC connection string so named instance (server\instance) works reliably
    odbc_parts = [
        f"Driver={{{driver}}}",
        f"Server={server}",
        f"Database={database}",
    ]
    if username and password:
        odbc_parts.extend([f"UID={username}", f"PWD={password}"])
    else:
        odbc_parts.append("Trusted_Connection=yes")
    # Match SSMS when encryption is mandatory and "Trust server certificate" is checked
    odbc_parts.append("TrustServerCertificate=yes")
    return f"mssql+pyodbc://?odbc_connect={quote_plus(';'.join(odbc_parts))}"
