from pathlib import Path

# Directories
ROOT = Path(__file__).resolve().parents[2]
WAREHOUSE_DIR = ROOT / "data"

# filenames
METADATA_XL = "table_config.xlsx"

LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"