"""
Validation and type coercion: column presence, types, business rules, SQL Server–compatible casting.
"""
import logging
from typing import Any

import pandas as pd

from .schema import EntitySchema

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Column validation
# ---------------------------------------------------------------------------


class ValidationError(Exception):
    """Raised when validation fails (e.g. missing required columns)."""
    pass


def validate_columns(df: pd.DataFrame, schema: EntitySchema) -> None:
    """
    Ensure all required columns exist (after normalizing names).
    Optionally allow optional columns to be missing.
    """
    required = set(schema.required_columns)
    missing = required - set(df.columns)
    if missing:
        raise ValidationError(
            f"[{schema.name}] Missing required columns: {sorted(missing)}. "
            f"Found: {sorted(df.columns)}"
        )


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Strip whitespace from column names (and optionally lowercase for consistency)."""
    df = df.rename(columns=lambda c: c.strip() if isinstance(c, str) else c)
    return df


# ---------------------------------------------------------------------------
# Type coercion (SQL Server–compatible)
# ---------------------------------------------------------------------------


def coerce_numeric(df: pd.DataFrame, columns: tuple[str, ...], decimal_comma: bool = True) -> pd.DataFrame:
    """Coerce columns to numeric; treat comma as decimal separator if decimal_comma."""
    for col in columns:
        if col not in df.columns:
            continue
        if decimal_comma:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", ".", regex=False),
                errors="coerce",
            )
        else:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def coerce_integers(df: pd.DataFrame, columns: tuple[str, ...]) -> pd.DataFrame:
    """
    Coerce columns to pandas nullable integer (Int64). Safely handles float64
    with decimal values by rounding; preserves null/NA; avoids unsafe cast exceptions.
    """
    for col in columns:
        if col not in df.columns:
            continue
        s = pd.to_numeric(df[col], errors="coerce")
        s = s.replace([float("inf"), float("-inf")], pd.NA)
        s = s.round(0)
        df[col] = pd.array(s, dtype="Int64")
    return df


def coerce_dates(df: pd.DataFrame, columns: tuple[str, ...]) -> pd.DataFrame:
    """Coerce to datetime; invalid → NaT."""
    for col in columns:
        if col not in df.columns:
            continue
        df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def trim_and_truncate_strings(
    df: pd.DataFrame,
    string_columns: tuple[str, ...],
    max_lengths: dict[str, int],
) -> pd.DataFrame:
    """Trim whitespace and truncate to max length (SQL Server NVARCHAR)."""
    for col in string_columns:
        if col not in df.columns:
            continue
        df[col] = df[col].astype(str).str.strip().str.replace("\r\n", " ").str.replace("\n", " ")
        max_len = max_lengths.get(col)
        if max_len is not None:
            df[col] = df[col].str.slice(0, max_len)
    return df


# ---------------------------------------------------------------------------
# Business rules
# ---------------------------------------------------------------------------


def apply_business_rules(
    df: pd.DataFrame,
    schema: EntitySchema,
) -> tuple[pd.DataFrame, int]:
    """
    Filter out rows that violate business rules (e.g. quantity > 0).
    Returns (filtered DataFrame, number of rows removed).
    """
    if not schema.business_rules:
        return df, 0
    before = len(df)
    mask = pd.Series(True, index=df.index)
    for rule in schema.business_rules:
        col = rule.get("column")
        op = rule.get("op")
        value = rule.get("value")
        if col not in df.columns or op is None:
            continue
        if op == ">":
            mask &= (pd.to_numeric(df[col], errors="coerce") > value)
        elif op == ">=":
            mask &= (pd.to_numeric(df[col], errors="coerce") >= value)
        elif op == "<":
            mask &= (pd.to_numeric(df[col], errors="coerce") < value)
        elif op == "!=":
            mask &= (df[col] != value)
        elif op == "not_null":
            mask &= df[col].notna()
    df = df.loc[mask].copy()
    removed = before - len(df)
    return df, removed


# ---------------------------------------------------------------------------
# Null key drop & deduplication
# ---------------------------------------------------------------------------


def drop_null_keys(df: pd.DataFrame, key_columns: tuple[str, ...]) -> tuple[pd.DataFrame, int]:
    """Drop rows with null in any key column. Returns (df, dropped_count)."""
    existing = [c for c in key_columns if c in df.columns]
    if not existing:
        return df, 0
    before = len(df)
    df = df.dropna(subset=existing).copy()
    return df, before - len(df)


def drop_duplicates_by_key(
    df: pd.DataFrame,
    key_columns: tuple[str, ...],
    keep: str = "first",
) -> tuple[pd.DataFrame, int]:
    """Remove duplicate rows by key. Returns (df, removed_count)."""
    existing = [c for c in key_columns if c in df.columns]
    if not existing:
        return df, 0
    before = len(df)
    df = df.drop_duplicates(subset=existing, keep=keep).copy()
    return df, before - len(df)


# ---------------------------------------------------------------------------
# Full clean pipeline (staging → typed, trimmed, no null keys, no dups)
# ---------------------------------------------------------------------------


def cast_for_sql_server(df: pd.DataFrame, schema: EntitySchema) -> pd.DataFrame:
    """
    Apply all type coercion and string trimming/truncation for the entity.
    Call after validate_columns and null/dedup steps.
    """
    df = df.copy()
    if schema.numeric_columns:
        coerce_numeric(df, schema.numeric_columns, decimal_comma=True)
    if schema.integer_columns:
        coerce_integers(df, schema.integer_columns)
    if schema.date_columns:
        coerce_dates(df, schema.date_columns)
    if schema.string_columns:
        trim_and_truncate_strings(df, schema.string_columns, schema.max_lengths)
    return df
