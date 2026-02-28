"""
Load a single CSV with encoding fallback and optional decimal comma handling.
"""
import csv
import sys
from pathlib import Path

import pandas as pd

from .config import DATA_DIR, CSV_DELIMITER, CSV_ENCODINGS, TABLE_CSV_MAP

# Python engine's CSV reader has a default field size limit (131072); raise it for large/malformed fields.
csv.field_size_limit(min(sys.maxsize, 2**31 - 1))


def load_csv(table_name: str, decimal_comma_columns: list[str] | None = None) -> pd.DataFrame:
    """
    Load CSV for a given table. Tries multiple encodings. Optionally normalizes
    decimal comma to dot for given columns.
    """
    filename = TABLE_CSV_MAP.get(table_name, f"{table_name}.csv")
    path = DATA_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"Data file not found: {path}")

    df = None
    last_error = None
    for encoding in CSV_ENCODINGS:
        try:
            df = pd.read_csv(
                path,
                sep=CSV_DELIMITER,
                encoding=encoding,
                engine="python",
            )
            break
        except UnicodeDecodeError as e:
            last_error = e
            continue

    if df is None:
        raise ValueError(f"Could not read {path} with any of {CSV_ENCODINGS}") from last_error

    # Normalize decimal comma (e.g. "1234,56" -> 1234.56)
    decimal_comma_columns = decimal_comma_columns or []
    for col in decimal_comma_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", ".", regex=False),
                errors="coerce"
            )
    return df
