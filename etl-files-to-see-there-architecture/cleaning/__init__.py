"""
Production-grade ETL cleaning and validation layer for the Star Schema data warehouse.
Staging → column validation → business rules → referential integrity → type casting → metrics.
"""
from .metrics import DataQualityMetrics, compute_null_counts
from .pipeline import (
    clean_customer,
    clean_fact_orders,
    clean_fact_payments,
    clean_fact_sales,
    clean_product,
    clean_region,
    clean_sector,
    clean_seller,
    run_cleaning_pipeline,
)
from .referential import (
    apply_ri_for_fact,
    build_valid_key_set,
    ensure_unknown_in_dimension,
    validate_and_resolve_ri,
)
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
    "clean_fact_orders",
    "clean_fact_sales",
    "clean_fact_payments",
    "run_cleaning_pipeline",
    "apply_ri_for_fact",
    "build_valid_key_set",
    "ensure_unknown_in_dimension",
    "validate_and_resolve_ri",
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
