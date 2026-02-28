"""
Referential integrity: validate FKs against dimension key sets; optional unknown strategy.
"""
import logging
from typing import Optional

import pandas as pd

from .schema import UNKNOWN_NATURAL_KEY

logger = logging.getLogger(__name__)


def apply_ri_for_fact(
    df: pd.DataFrame,
    fk_specs: list[tuple[str, set]],
    use_unknown: bool = True,
) -> tuple[pd.DataFrame, int, int]:
    """
    fk_specs = [(column_name, valid_key_set), ...].
    Returns (df, total_dropped, total_mapped_to_unknown).
    """
    df = df.copy()
    total_dropped = 0
    total_mapped = 0
    for fk_column, valid_keys in fk_specs:
        valid_keys = set(str(k) for k in valid_keys)
        valid_keys.add(UNKNOWN_NATURAL_KEY)
        if fk_column not in df.columns:
            continue
        values = df[fk_column].astype(str).str.strip()
        invalid_mask = ~values.isin(valid_keys)
        n_invalid = int(invalid_mask.sum())
        if n_invalid == 0:
            continue
        if use_unknown:
            df = df.copy()
            df.loc[invalid_mask, fk_column] = UNKNOWN_NATURAL_KEY
            total_mapped += n_invalid
        else:
            df = df.loc[~invalid_mask].copy()
            total_dropped += n_invalid
    return df, total_dropped, total_mapped


def ensure_unknown_in_dimension(
    df: pd.DataFrame,
    key_column: str,
    unknown_display: str = "Unknown",
) -> pd.DataFrame:
    if df.empty:
        return df
    keys = set(df[key_column].astype(str).str.strip())
    if UNKNOWN_NATURAL_KEY in keys:
        return df
    row = {key_column: UNKNOWN_NATURAL_KEY}
    for col in df.columns:
        if col == key_column:
            continue
        if df[col].dtype == object or pd.api.types.is_string_dtype(df[col]):
            row[col] = unknown_display
        else:
            row[col] = None
    return pd.concat([pd.DataFrame([row]), df], ignore_index=True)
