"""
Referential integrity: validate FKs against loaded dimension key sets and apply unknown strategy.
"""
import logging
from typing import Optional

import pandas as pd

from .schema import UNKNOWN_NATURAL_KEY

logger = logging.getLogger(__name__)


def build_valid_key_set(df: pd.DataFrame, key_column: str) -> set:
    """Build set of valid natural keys from a dimension DataFrame (after cleaning)."""
    if key_column not in df.columns:
        return set()
    return set(df[key_column].dropna().astype(str).str.strip().unique())


def validate_and_resolve_ri(
    df: pd.DataFrame,
    fk_column: str,
    valid_keys: set,
    use_unknown: bool = True,
) -> tuple[pd.DataFrame, int, int]:
    """
    Validate FK column against valid_keys. Optionally map invalid keys to UNKNOWN_NATURAL_KEY.
    Returns (df, n_dropped, n_mapped_to_unknown).
    """
    if fk_column not in df.columns:
        return df, 0, 0
    valid_keys = set(str(k) for k in valid_keys)
    valid_keys.add(UNKNOWN_NATURAL_KEY)
    before = len(df)
    # Normalize for comparison
    values = df[fk_column].astype(str).str.strip()
    invalid_mask = ~values.isin(valid_keys)
    invalid_count = invalid_mask.sum()
    if invalid_count == 0:
        return df, 0, 0
    if use_unknown:
        df = df.copy()
        df.loc[invalid_mask, fk_column] = UNKNOWN_NATURAL_KEY
        return df, 0, int(invalid_count)
    else:
        df = df.loc[~invalid_mask].copy()
        return df, int(invalid_count), 0


def apply_ri_for_fact(
    df: pd.DataFrame,
    fk_specs: list[tuple[str, set]],
    use_unknown: bool = True,
) -> tuple[pd.DataFrame, int, int]:
    """
    Apply RI for multiple FK columns. fk_specs = [(column_name, valid_key_set), ...].
    Returns (df, total_dropped, total_mapped_to_unknown).
    """
    total_dropped = 0
    total_mapped = 0
    for fk_column, valid_keys in fk_specs:
        df, dropped, mapped = validate_and_resolve_ri(df, fk_column, valid_keys, use_unknown=use_unknown)
        total_dropped += dropped
        total_mapped += mapped
    return df, total_dropped, total_mapped


def ensure_unknown_in_dimension(
    df: pd.DataFrame,
    key_column: str,
    unknown_display: str = "Unknown",
) -> pd.DataFrame:
    """
    Ensure one row with natural key = UNKNOWN_NATURAL_KEY exists for a dimension.
    Used so the loader can rely on this row for unknown FK resolution.
    """
    if df.empty:
        return df
    keys = set(df[key_column].astype(str).str.strip())
    if UNKNOWN_NATURAL_KEY in keys:
        return df
    # Build one row: key_column = UNKNOWN, other string cols = unknown_display
    row = {key_column: UNKNOWN_NATURAL_KEY}
    for col in df.columns:
        if col == key_column:
            continue
        if df[col].dtype == object or pd.api.types.is_string_dtype(df[col]):
            row[col] = unknown_display
        else:
            row[col] = None
    return pd.concat([pd.DataFrame([row]), df], ignore_index=True)
