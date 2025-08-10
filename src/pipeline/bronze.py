from __future__ import annotations

import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

import pandas as pd

from .io_paths import RAW_DIR, BRONZE_DIR, ensure_dirs


def _random_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=seconds)


def _generate_synthetic_sales(num_rows: int = 2000) -> pd.DataFrame:
    random.seed(42)

    products = [
        ("Notebook Pro 14", 6500.0),
        ("Notebook Air 13", 4500.0),
        ("Mouse Wireless", 120.0),
        ("Teclado Mecânico", 380.0),
        ('Monitor 27"', 1450.0),
        ("Headset Gamer", 520.0),
        ("Dock USB-C", 280.0),
    ]
    cities_states = [
        ("São Paulo", "SP"),
        ("Rio de Janeiro", "RJ"),
        ("Belo Horizonte", "MG"),
        ("Curitiba", "PR"),
        ("Porto Alegre", "RS"),
        ("Salvador", "BA"),
        ("Recife", "PE"),
    ]

    start = datetime(2024, 1, 1)
    end = datetime(2024, 6, 30, 23, 59, 59)

    records = []
    for order_id in range(1, num_rows + 1):
        customer_id = random.randint(1000, 1999)
        order_date = _random_date(start, end)
        product, unit_price = random.choice(products)
        quantity = random.randint(1, 5)
        city, state = random.choice(cities_states)
        records.append(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "order_date": order_date.strftime("%Y-%m-%d"),
                "product": product,
                "quantity": quantity,
                "unit_price": unit_price,
                "city": city,
                "state": state,
            }
        )

    df = pd.DataFrame.from_records(records)
    return df

def _ensure_raw_data() -> List[Path]:
    ensure_dirs()
    csv_files = list(RAW_DIR.glob("*.csv"))
    if not csv_files:
        # cria dataset sintético
        df = _generate_synthetic_sales()
        dest = RAW_DIR / "sales.csv"
        df.to_csv(dest, index=False)
        csv_files = [dest]
    return csv_files


def run() -> Path:
    """Executa a camada Bronze: lê CSVs brutos e escreve Parquet consolidado."""
    csv_files = _ensure_raw_data()

    frames = [pd.read_csv(path, parse_dates=["order_date"]) for path in csv_files]
    df = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]

    ensure_dirs()
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    bronze_file = BRONZE_DIR / "sales.parquet"
    df.to_parquet(bronze_file, engine="pyarrow", index=False)

    print(f"[Bronze] Linhas: {len(df):,} | Arquivo: {bronze_file}")
    return bronze_file


