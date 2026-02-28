"""
Staging: load raw CSV with encoding fallback and optional decimal-comma handling.
Produces DataFrame for pipeline validation and cleaning.
"""
import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from ..load_csv import load_csv
from ...config import DATA_DIR
from .metrics import DataQualityMetrics
from .schema import EntitySchema
from .validators import normalize_column_names, validate_columns, ValidationError

logger = logging.getLogger(__name__)

DECIMAL_COMMA_BY_SOURCE = {"Invoice": ["paymentamount"]}


def load_raw_staging(
    source_table: str,
    data_dir: Optional[Path] = None,
    decimal_comma_columns: Optional[list[str]] = None,
) -> tuple[pd.DataFrame, DataQualityMetrics]:
    """
    Load raw CSV for source_table; normalize column names.
    Returns (staging_df, metrics with raw_row_count).
    """
    data_dir = data_dir or DATA_DIR
    decimal_comma_columns = decimal_comma_columns or DECIMAL_COMMA_BY_SOURCE.get(source_table)
    df = load_csv(source_table, data_dir=data_dir, decimal_comma_columns=decimal_comma_columns)
    raw_count = len(df)
    df = normalize_column_names(df)
    metrics = DataQualityMetrics(
        entity=source_table,
        raw_row_count=raw_count,
    )
    logger.info("[%s] Staging loaded: rows=%s", source_table, raw_count)
    return df, metrics


def load_raw_staging_for_entity(
    schema: EntitySchema,
    data_dir: Optional[Path] = None,
    decimal_comma_columns: Optional[list[str]] = None,
) -> tuple[pd.DataFrame, DataQualityMetrics]:
    """
    Load raw CSV for entity (schema.source_table); normalize and validate required columns.
    """
    df, metrics = load_raw_staging(
        schema.source_table,
        data_dir=data_dir,
        decimal_comma_columns=decimal_comma_columns,
    )
    validate_columns(df, schema)
    metrics.entity = schema.name
    return df, metrics
