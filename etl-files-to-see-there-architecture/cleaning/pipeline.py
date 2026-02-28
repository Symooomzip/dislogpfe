"""
Cleaning pipeline: per-entity orchestration (staging → validate → clean → RI → cast → metrics).
Produces clean DataFrames ready for bulk insert into the star schema.
"""
import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from .metrics import DataQualityMetrics, compute_null_counts
from .referential import apply_ri_for_fact, ensure_unknown_in_dimension
from .schema import (
    CUSTOMER,
    INVOICE,
    PRODUCT,
    REGION,
    SALES_HEADER,
    SALES_LINE,
    SECTOR,
    SELLER,
    UNKNOWN_NATURAL_KEY,
    EntitySchema,
)
from .staging import load_raw_staging_for_entity
from .validators import (
    apply_business_rules,
    cast_for_sql_server,
    drop_duplicates_by_key,
    drop_null_keys,
)

logger = logging.getLogger(__name__)

DECIMAL_COMMA_BY_SOURCE = {"Invoice": ["paymentamount"]}


def _clean_dimension(
    schema: EntitySchema,
    data_dir: Optional[Path] = None,
    add_unknown_row: bool = True,
) -> tuple[pd.DataFrame, DataQualityMetrics]:
    """
    Full clean pipeline for a dimension: load → drop null keys → dedup → cast → optional unknown row.
    """
    decimal_cols = DECIMAL_COMMA_BY_SOURCE.get(schema.source_table)
    df, metrics = load_raw_staging_for_entity(
        schema,
        data_dir=data_dir,
        decimal_comma_columns=decimal_cols,
    )
    metrics.raw_row_count = len(df)

    df, dropped = drop_null_keys(df, schema.key_columns)
    metrics.null_key_dropped = dropped
    metrics.after_null_drop = len(df)

    df, dup_removed = drop_duplicates_by_key(df, schema.key_columns, keep="first")
    metrics.duplicates_removed = dup_removed
    metrics.after_dedup = len(df)

    df = cast_for_sql_server(df, schema)
    metrics.null_counts = compute_null_counts(
        df,
        list(schema.required_columns) + list(schema.optional_columns),
    )
    metrics.after_business_rules = len(df)
    metrics.after_ri_filter = len(df)
    metrics.final_row_count = len(df)

    if add_unknown_row:
        key_col = schema.key_columns[0]
        df = ensure_unknown_in_dimension(df, key_col, unknown_display="Unknown")

    metrics.log()
    return df, metrics


def clean_region(data_dir: Optional[Path] = None) -> tuple[pd.DataFrame, DataQualityMetrics]:
    """Clean Region → DimRegion staging."""
    return _clean_dimension(REGION, data_dir=data_dir)


def clean_sector(data_dir: Optional[Path] = None) -> tuple[pd.DataFrame, DataQualityMetrics]:
    """Clean Sector → DimSector staging."""
    return _clean_dimension(SECTOR, data_dir=data_dir)


def clean_customer(data_dir: Optional[Path] = None) -> tuple[pd.DataFrame, DataQualityMetrics]:
    """Clean Customer → DimCustomer staging."""
    return _clean_dimension(CUSTOMER, data_dir=data_dir)


def clean_seller(data_dir: Optional[Path] = None) -> tuple[pd.DataFrame, DataQualityMetrics]:
    """Clean Seller → DimSeller staging."""
    return _clean_dimension(SELLER, data_dir=data_dir)


def clean_product(data_dir: Optional[Path] = None) -> tuple[pd.DataFrame, DataQualityMetrics]:
    """Clean Product → DimProduct staging."""
    return _clean_dimension(PRODUCT, data_dir=data_dir)


def clean_fact_orders(
    data_dir: Optional[Path] = None,
    valid_accountids: Optional[set] = None,
    valid_sellerids: Optional[set] = None,
    use_unknown: bool = True,
) -> tuple[pd.DataFrame, DataQualityMetrics]:
    """
    Clean SalesHeader → FactOrders staging.
    valid_accountids and valid_sellerids from cleaned DimCustomer and DimSeller.
    """
    decimal_cols = DECIMAL_COMMA_BY_SOURCE.get(SALES_HEADER.source_table)
    df, metrics = load_raw_staging_for_entity(
        SALES_HEADER,
        data_dir=data_dir,
        decimal_comma_columns=decimal_cols,
    )
    metrics.raw_row_count = len(df)

    df, dropped = drop_null_keys(df, SALES_HEADER.key_columns)
    metrics.null_key_dropped = dropped
    metrics.after_null_drop = len(df)

    df, dup_removed = drop_duplicates_by_key(df, SALES_HEADER.key_columns, keep="first")
    metrics.duplicates_removed = dup_removed
    metrics.after_dedup = len(df)

    df, rules_removed = apply_business_rules(df, SALES_HEADER)
    metrics.business_rule_violations = rules_removed
    metrics.after_business_rules = len(df)

    valid_accountids = set(str(x) for x in (valid_accountids or set())) | {UNKNOWN_NATURAL_KEY}
    valid_sellerids = set(str(x) for x in (valid_sellerids or set())) | {UNKNOWN_NATURAL_KEY}
    df, ri_dropped, ri_mapped = apply_ri_for_fact(
        df,
        [("accountid", valid_accountids), ("sellerid", valid_sellerids)],
        use_unknown=use_unknown,
    )
    metrics.ri_violations = ri_dropped + ri_mapped
    metrics.ri_mapped_to_unknown = ri_mapped
    metrics.after_ri_filter = len(df)

    df = cast_for_sql_server(df, SALES_HEADER)
    metrics.null_counts = compute_null_counts(
        df,
        list(SALES_HEADER.required_columns) + list(SALES_HEADER.optional_columns),
    )
    metrics.final_row_count = len(df)
    metrics.log()
    return df, metrics


def clean_fact_sales(
    data_dir: Optional[Path] = None,
    valid_order_ids: Optional[set] = None,
    valid_itemids: Optional[set] = None,
    use_unknown: bool = True,
) -> tuple[pd.DataFrame, DataQualityMetrics]:
    """
    Clean SalesLine → FactSales staging.
    saleid must exist in FactOrders (strict). itemid can map to unknown.
    """
    df, metrics = load_raw_staging_for_entity(SALES_LINE, data_dir=data_dir)
    metrics.raw_row_count = len(df)

    df, dropped = drop_null_keys(df, SALES_LINE.key_columns)
    metrics.null_key_dropped = dropped
    metrics.after_null_drop = len(df)

    df, dup_removed = drop_duplicates_by_key(df, SALES_LINE.key_columns, keep="first")
    metrics.duplicates_removed = dup_removed
    metrics.after_dedup = len(df)

    df, rules_removed = apply_business_rules(df, SALES_LINE)
    metrics.business_rule_violations = rules_removed
    metrics.after_business_rules = len(df)

    saleid_valid = set(str(x) for x in (valid_order_ids or set()))
    itemid_valid = set(str(x) for x in (valid_itemids or set())) | {UNKNOWN_NATURAL_KEY}

    if saleid_valid:
        values = df["saleid"].astype(str).str.strip()
        invalid_mask = ~values.isin(saleid_valid)
        metrics.ri_violations = int(invalid_mask.sum())
        df = df.loc[~invalid_mask].copy()
    else:
        metrics.ri_violations = 0

    df, ri_drop, ri_map = apply_ri_for_fact(
        df, [("itemid", itemid_valid)], use_unknown=use_unknown
    )
    metrics.ri_violations += ri_drop
    metrics.ri_mapped_to_unknown = ri_map

    metrics.after_ri_filter = len(df)
    df = cast_for_sql_server(df, SALES_LINE)
    metrics.null_counts = compute_null_counts(
        df,
        list(SALES_LINE.required_columns) + list(SALES_LINE.optional_columns),
    )
    metrics.final_row_count = len(df)
    metrics.log()
    return df, metrics


def clean_fact_payments(
    data_dir: Optional[Path] = None,
    valid_order_ids: Optional[set] = None,
    use_unknown: bool = True,
) -> tuple[pd.DataFrame, DataQualityMetrics]:
    """
    Clean Invoice → FactPayments staging.
    Invoice has invoiceid, salesid, paymentamount, paymentmethod (no accountid).
    Only salesid is validated against FactOrders.order_id.
    """
    decimal_cols = DECIMAL_COMMA_BY_SOURCE.get(INVOICE.source_table)
    df, metrics = load_raw_staging_for_entity(
        INVOICE,
        data_dir=data_dir,
        decimal_comma_columns=decimal_cols,
    )
    metrics.raw_row_count = len(df)

    df, dropped = drop_null_keys(df, INVOICE.key_columns)
    metrics.null_key_dropped = dropped
    metrics.after_null_drop = len(df)

    df, dup_removed = drop_duplicates_by_key(df, INVOICE.key_columns, keep="first")
    metrics.duplicates_removed = dup_removed
    metrics.after_dedup = len(df)

    valid_order_ids = set(str(x) for x in (valid_order_ids or set()))
    if valid_order_ids:
        values = df["salesid"].astype(str).str.strip()
        invalid_mask = ~values.isin(valid_order_ids)
        n_invalid = int(invalid_mask.sum())
        metrics.ri_violations = n_invalid
        if use_unknown:
            df = df.copy()
            df.loc[invalid_mask, "salesid"] = UNKNOWN_NATURAL_KEY
            metrics.ri_mapped_to_unknown = n_invalid
        else:
            df = df.loc[~invalid_mask].copy()
    else:
        metrics.ri_violations = 0
        metrics.ri_mapped_to_unknown = 0

    metrics.after_ri_filter = len(df)
    df = cast_for_sql_server(df, INVOICE)
    metrics.null_counts = compute_null_counts(
        df,
        list(INVOICE.required_columns) + list(INVOICE.optional_columns),
    )
    metrics.final_row_count = len(df)
    metrics.log()
    return df, metrics


def run_cleaning_pipeline(
    data_dir: Optional[Path] = None,
    use_unknown: bool = True,
) -> dict[str, tuple[pd.DataFrame, DataQualityMetrics]]:
    """
    Run full cleaning pipeline in dependency order.
    Returns dict entity_name -> (clean_df, metrics).
    Dimensions first; then FactOrders (needs valid customer/seller); then FactSales (needs valid orders + products); then FactPayments (needs valid orders).
    """
    results: dict[str, tuple[pd.DataFrame, DataQualityMetrics]] = {}
    data_dir = data_dir or Path(__file__).resolve().parent.parent.parent / "Data"

    df_region, m_region = clean_region(data_dir)
    results["DimRegion"] = (df_region, m_region)

    df_sector, m_sector = clean_sector(data_dir)
    results["DimSector"] = (df_sector, m_sector)

    df_customer, m_customer = clean_customer(data_dir)
    results["DimCustomer"] = (df_customer, m_customer)

    df_seller, m_seller = clean_seller(data_dir)
    results["DimSeller"] = (df_seller, m_seller)

    df_product, m_product = clean_product(data_dir)
    results["DimProduct"] = (df_product, m_product)

    valid_accountids = set(df_customer["accountid"].dropna().astype(str).str.strip())
    valid_sellerids = set(df_seller["sellerid"].dropna().astype(str).str.strip())
    df_orders, m_orders = clean_fact_orders(
        data_dir,
        valid_accountids=valid_accountids,
        valid_sellerids=valid_sellerids,
        use_unknown=use_unknown,
    )
    results["FactOrders"] = (df_orders, m_orders)

    valid_order_ids = set(df_orders["saleid"].dropna().astype(str).str.strip())
    valid_itemids = set(df_product["itemid"].dropna().astype(str).str.strip())
    df_sales, m_sales = clean_fact_sales(
        data_dir,
        valid_order_ids=valid_order_ids,
        valid_itemids=valid_itemids,
        use_unknown=use_unknown,
    )
    results["FactSales"] = (df_sales, m_sales)

    df_payments, m_payments = clean_fact_payments(
        data_dir,
        valid_order_ids=valid_order_ids,
        use_unknown=use_unknown,
    )
    results["FactPayments"] = (df_payments, m_payments)

    return results
