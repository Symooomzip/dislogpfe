"""
Load a single CSV with encoding fallback and optional decimal-comma handling.
Used by both direct ETL and cleaning staging.
"""
import csv
import sys
from pathlib import Path

import pandas as pd

from ..config import (
    CSV_DELIMITER,
    CSV_ENCODINGS,
    DATA_DIR,
    TABLE_CSV_MAP,
    TABLE_ENCODING_ORDER,
)

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))


def load_csv(
    table_name: str,
    data_dir: Path | None = None,
    decimal_comma_columns: list[str] | None = None,
) -> pd.DataFrame:
    """
    Load CSV for a given table. Tries encodings (per-table order if set, else global).
    Optionally normalizes decimal comma to dot for given columns.
    """
    data_dir = data_dir or DATA_DIR
    filename = TABLE_CSV_MAP.get(table_name, f"{table_name}.csv")
    path = data_dir / filename
    if not path.exists():
        raise FileNotFoundError(f"Data file not found: {path}")

    encodings = TABLE_ENCODING_ORDER.get(table_name, CSV_ENCODINGS)
    df = None
    last_error = None
    for encoding in encodings:
        try:
            df = pd.read_csv(
                path,
                sep=CSV_DELIMITER,
                encoding=encoding,
                engine="python",
                on_bad_lines="skip",
            )
            break
        except (UnicodeDecodeError, Exception) as e:
            last_error = e
            continue

    if df is None:
        raise ValueError(f"Could not read {path} with any of {encodings}") from last_error

    # Normalize decimal comma (e.g. Invoice.paymentamount)
    decimal_comma_columns = decimal_comma_columns or []
    for col in decimal_comma_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", ".", regex=False),
                errors="coerce",
            )
    return df
