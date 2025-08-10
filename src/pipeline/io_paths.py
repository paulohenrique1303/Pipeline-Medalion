from pathlib import Path


def get_project_root() -> Path:
    # src/pipeline/io_paths.py -> parents[0]=pipeline, [1]=src, [2]=<root>
    return Path(__file__).resolve().parents[2]


PROJECT_ROOT: Path = get_project_root()
DATA_DIR: Path = PROJECT_ROOT / "data"
RAW_DIR: Path = DATA_DIR / "raw"
BRONZE_DIR: Path = DATA_DIR / "bronze" / "sales"
SILVER_DIR: Path = DATA_DIR / "silver" / "sales"
GOLD_DIR: Path = DATA_DIR / "gold"


def ensure_dirs() -> None:
    for path in [DATA_DIR, RAW_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR]:
        path.mkdir(parents=True, exist_ok=True)


