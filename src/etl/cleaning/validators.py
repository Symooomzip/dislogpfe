"""
Validation and type coercion: column presence, types, business rules, SQL Server–compatible casting.
"""
import logging
from typing import Any

import pandas as pd

from .schema import EntitySchema

logger = logging.getLogger(__name__)


class ValidationError(Exception):
    pass


def validate_columns(df: pd.DataFrame, schema: EntitySchema) -> None:
    required = set(schema.required_columns)
    missing = required - set(df.columns)
    if missing:
        raise ValidationError(
            f"[{schema.name}] Missing required columns: {sorted(missing)}. Found: {sorted(df.columns)}"
        )


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=lambda c: c.strip() if isinstance(c, str) else c)
    return df


def coerce_numeric(
    df: pd.DataFrame, columns: tuple[str, ...], decimal_comma: bool = True
) -> pd.DataFrame:
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
    for col in columns:
        if col not in df.columns:
            continue
        s = pd.to_numeric(df[col], errors="coerce")
        s = s.replace([float("inf"), float("-inf")], pd.NA)
        s = s.round(0)
        df[col] = pd.array(s, dtype="Int64")
    return df


def coerce_dates(df: pd.DataFrame, columns: tuple[str, ...]) -> pd.DataFrame:
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
    for col in string_columns:
        if col not in df.columns:
            continue
        df[col] = df[col].astype(str).str.strip().str.replace("\r\n", " ").str.replace("\n", " ")
        max_len = max_lengths.get(col)
        if max_len is not None:
            df[col] = df[col].str.slice(0, max_len)
    return df


def apply_business_rules(
    df: pd.DataFrame,
    schema: EntitySchema,
) -> tuple[pd.DataFrame, int]:
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
            mask &= pd.to_numeric(df[col], errors="coerce") > value
        elif op == ">=":
            mask &= pd.to_numeric(df[col], errors="coerce") >= value
        elif op == "<":
            mask &= pd.to_numeric(df[col], errors="coerce") < value
        elif op == "!=":
            mask &= df[col] != value
        elif op == "not_null":
            mask &= df[col].notna()
    df = df.loc[mask].copy()
    return df, before - len(df)


def drop_null_keys(df: pd.DataFrame, key_columns: tuple[str, ...]) -> tuple[pd.DataFrame, int]:
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
    existing = [c for c in key_columns if c in df.columns]
    if not existing:
        return df, 0
    before = len(df)
    df = df.drop_duplicates(subset=existing, keep=keep).copy()
    return df, before - len(df)


def cast_for_sql_server(df: pd.DataFrame, schema: EntitySchema) -> pd.DataFrame:
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
