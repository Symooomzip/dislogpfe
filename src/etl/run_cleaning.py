"""
Run only the cleaning pipeline (no DB load). Optionally write cleaned CSVs to Data/cleaned/.
Usage: python -m src.etl.run_cleaning
"""
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

from ..config import CLEANED_DATA_DIR, PROJECT_ROOT
from .cleaning import run_cleaning_pipeline

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    data_dir = PROJECT_ROOT / "Data"
    if not data_dir.exists():
        data_dir = PROJECT_ROOT / "data"
    if not data_dir.exists():
        logger.error("Data directory not found (tried Data/ and data/)")
        return 1
    try:
        results = run_cleaning_pipeline(data_dir=data_dir, use_unknown=True)
        logger.info("Cleaning finished. Entities: %s", list(results.keys()))
        for name, (df, metrics) in results.items():
            logger.info("  %s: %s rows (final)", name, len(df) if df is not None else 0)
            if CLEANED_DATA_DIR and df is not None and not df.empty:
                CLEANED_DATA_DIR.mkdir(parents=True, exist_ok=True)
                out_path = CLEANED_DATA_DIR / f"{name}.csv"
                df.to_csv(out_path, index=False, sep=";", encoding="utf-8")
                logger.info("  Written: %s", out_path)
        return 0
    except FileNotFoundError as e:
        logger.error("File not found: %s", e)
        return 1
    except Exception as e:
        logger.exception("Cleaning failed: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
