from __future__ import annotations

from pathlib import Path
import pandas as pd

from .io_paths import SILVER_DIR, GOLD_DIR, ensure_dirs


def _read_silver() -> pd.DataFrame:
    parquet_files = list(SILVER_DIR.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"Nenhum arquivo Parquet encontrado em {SILVER_DIR}")
    frames = [pd.read_parquet(p) for p in parquet_files]
    df = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
    return df


def _gold_revenue_by_month(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["order_month"] = df["order_date"].dt.to_period("M").dt.to_timestamp()
    agg = (
        df.groupby("order_month")
        .agg(
            revenue=("revenue", "sum"),
            total_orders=("order_id", "nunique"),
            unique_customers=("customer_id", "nunique"),
        )
        .reset_index()
        .sort_values("order_month")
    )
    return agg


def _gold_top_products(df: pd.DataFrame) -> pd.DataFrame:
    agg = (
        df.groupby("product")
        .agg(
            revenue=("revenue", "sum"),
            quantity=("quantity", "sum"),
        )
        .reset_index()
        .sort_values(["revenue", "quantity"], ascending=[False, False])
    )
    return agg


def _gold_revenue_by_state(df: pd.DataFrame) -> pd.DataFrame:
    agg = (
        df.groupby("state")
        .agg(revenue=("revenue", "sum"), orders=("order_id", "nunique"))
        .reset_index()
        .sort_values("revenue", ascending=False)
    )
    return agg


def run() -> dict[str, Path]:
    """Executa a camada Gold: data marts e agregações."""
    df_silver = _read_silver()

    tables = {
        "revenue_by_month": _gold_revenue_by_month(df_silver),
        "top_products": _gold_top_products(df_silver),
        "revenue_by_state": _gold_revenue_by_state(df_silver),
    }

    ensure_dirs()
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    outputs: dict[str, Path] = {}
    for name, table in tables.items():
        out_path = GOLD_DIR / name / f"{name}.parquet"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        table.to_parquet(out_path, engine="pyarrow", index=False)
        outputs[name] = out_path
        print(f"[Gold] {name}: linhas={len(table):,} | arquivo={out_path}")

    return outputs


