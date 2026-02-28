"""
Staging layer: load raw CSV into memory with encoding fallback and optional row validation.
Produces a DataFrame that can then be validated and cleaned by the pipeline.
"""
import csv
import logging
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

from ..config import CSV_DELIMITER, CSV_ENCODINGS, DATA_DIR, TABLE_CSV_MAP
from .metrics import DataQualityMetrics
from .schema import EntitySchema
from .validators import normalize_column_names, validate_columns, ValidationError

logger = logging.getLogger(__name__)

# Allow large fields (e.g. malformed lines) so we can detect and drop by row validation
csv.field_size_limit(min(sys.maxsize, 2**31 - 1))


def _read_csv_safe(path: Path, encoding: str, delimiter: str) -> pd.DataFrame:
    """Read CSV with given encoding. Skips malformed lines if pandas >= 1.3."""
    try:
        return pd.read_csv(
            path,
            sep=delimiter,
            encoding=encoding,
            engine="python",
            on_bad_lines="skip",
        )
    except TypeError:
        # pandas < 1.3: no on_bad_lines
        return pd.read_csv(path, sep=delimiter, encoding=encoding, engine="python")


def load_raw_staging(
    source_table: str,
    data_dir: Optional[Path] = None,
    decimal_comma_columns: Optional[list[str]] = None,
) -> tuple[pd.DataFrame, DataQualityMetrics]:
    """
    Load raw CSV for a source table into a staging DataFrame.
    Tries multiple encodings; normalizes column names; validates required columns exist.
    Returns (staging_df, metrics with raw_row_count and encoding_used).
    """
    data_dir = data_dir or DATA_DIR
    filename = TABLE_CSV_MAP.get(source_table, f"{source_table}.csv")
    path = data_dir / filename
    if not path.exists():
        raise FileNotFoundError(f"Staging source not found: {path}")

    df = None
    encoding_used = None
    last_error = None
    for enc in CSV_ENCODINGS:
        try:
            df = _read_csv_safe(path, enc, CSV_DELIMITER)
            encoding_used = enc
            break
        except UnicodeDecodeError as e:
            last_error = e
            continue

    if df is None:
        raise ValueError(
            f"Could not read {path} with any of {CSV_ENCODINGS}"
        ) from last_error

    raw_count = len(df)
    df = normalize_column_names(df)

    # Optional: normalize decimal comma for known columns (e.g. Invoice.paymentamount)
    decimal_comma_columns = decimal_comma_columns or []
    for col in decimal_comma_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", ".", regex=False),
                errors="coerce",
            )

    metrics = DataQualityMetrics(
        entity=source_table,
        raw_row_count=raw_count,
        encoding_used=encoding_used,
    )
    logger.info("[%s] Staging loaded: rows=%s, encoding=%s", source_table, raw_count, encoding_used)
    return df, metrics


def load_raw_staging_for_entity(
    schema: EntitySchema,
    data_dir: Optional[Path] = None,
    decimal_comma_columns: Optional[list[str]] = None,
) -> tuple[pd.DataFrame, DataQualityMetrics]:
    """
    Load raw CSV for an entity (using schema.source_table), normalize columns,
    and validate that required columns exist. Raises ValidationError if columns are missing.
    """
    df, metrics = load_raw_staging(
        schema.source_table,
        data_dir=data_dir,
        decimal_comma_columns=decimal_comma_columns,
    )
    validate_columns(df, schema)
    metrics.entity = schema.name
    return df, metrics
