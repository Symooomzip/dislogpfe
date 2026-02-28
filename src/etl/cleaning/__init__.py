"""
Cleaning layer: staging → validate → business rules → RI → cast → metrics.
"""
from .metrics import DataQualityMetrics, compute_null_counts
from .pipeline import (
    clean_customer,
    clean_invoice,
    clean_product,
    clean_region,
    clean_sales_header,
    clean_sales_line,
    clean_sector,
    clean_seller,
    run_cleaning_pipeline,
)
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
    EntitySchema,
    UNKNOWN_NATURAL_KEY,
)
from .staging import load_raw_staging, load_raw_staging_for_entity
from .validators import (
    ValidationError,
    apply_business_rules,
    cast_for_sql_server,
    drop_duplicates_by_key,
    drop_null_keys,
    validate_columns,
)

__all__ = [
    "DataQualityMetrics",
    "compute_null_counts",
    "clean_region",
    "clean_sector",
    "clean_customer",
    "clean_seller",
    "clean_product",
    "clean_sales_header",
    "clean_sales_line",
    "clean_invoice",
    "run_cleaning_pipeline",
    "apply_ri_for_fact",
    "ensure_unknown_in_dimension",
    "EntitySchema",
    "REGION",
    "SECTOR",
    "CUSTOMER",
    "SELLER",
    "PRODUCT",
    "SALES_HEADER",
    "SALES_LINE",
    "INVOICE",
    "UNKNOWN_NATURAL_KEY",
    "load_raw_staging",
    "load_raw_staging_for_entity",
    "ValidationError",
    "apply_business_rules",
    "cast_for_sql_server",
    "drop_duplicates_by_key",
    "drop_null_keys",
    "validate_columns",
]
