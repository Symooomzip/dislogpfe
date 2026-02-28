"""
Data quality metrics: row counts, nulls, duplicates, RI violations.
"""
import logging
from dataclasses import dataclass, field

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class DataQualityMetrics:
    """Metrics for one entity's cleaning run."""

    entity: str
    raw_row_count: int = 0
    after_null_drop: int = 0
    after_dedup: int = 0
    after_business_rules: int = 0
    after_ri_filter: int = 0
    final_row_count: int = 0
    null_key_dropped: int = 0
    duplicates_removed: int = 0
    business_rule_violations: int = 0
    ri_violations: int = 0
    ri_mapped_to_unknown: int = 0
    null_counts: dict = field(default_factory=dict)
    parse_errors: int = 0
    encoding_used: str | None = None

    def log(self) -> None:
        logger.info(
            "[%s] DQ metrics: raw=%s | after_null_drop=%s | after_dedup=%s | after_rules=%s | after_ri=%s | final=%s",
            self.entity,
            self.raw_row_count,
            self.after_null_drop,
            self.after_dedup,
            self.after_business_rules,
            self.after_ri_filter,
            self.final_row_count,
        )
        if self.null_key_dropped:
            logger.warning("[%s] Rows dropped (null in key): %s", self.entity, self.null_key_dropped)
        if self.duplicates_removed:
            logger.warning("[%s] Duplicate rows removed: %s", self.entity, self.duplicates_removed)
        if self.business_rule_violations:
            logger.warning("[%s] Rows dropped (business rules): %s", self.entity, self.business_rule_violations)
        if self.ri_violations:
            logger.warning("[%s] RI violations (dropped or mapped): %s", self.entity, self.ri_violations)
        if self.ri_mapped_to_unknown:
            logger.info("[%s] Rows mapped to unknown dimension: %s", self.entity, self.ri_mapped_to_unknown)


def compute_null_counts(df: pd.DataFrame, columns: list[str]) -> dict[str, int]:
    result = {}
    for col in columns:
        if col in df.columns:
            result[col] = int(df[col].isna().sum())
    return result
