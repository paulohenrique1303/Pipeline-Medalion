from __future__ import annotations

from pathlib import Path
import pandas as pd

from .io_paths import BRONZE_DIR, SILVER_DIR, ensure_dirs


def _read_bronze() -> pd.DataFrame:
    parquet_files = list(BRONZE_DIR.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"Nenhum arquivo Parquet encontrado em {BRONZE_DIR}")
    frames = [pd.read_parquet(p) for p in parquet_files]
    df = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
    return df


def _clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Tipos
    df["order_id"] = df["order_id"].astype(int)
    df["customer_id"] = df["customer_id"].astype(int)
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce").fillna(0.0).astype(float)

    # Normalização de texto
    for col in ["product", "city", "state"]:
        df[col] = (
            df[col].astype(str).str.strip().str.replace("\s+", " ", regex=True).str.title()
        )

    # Regras de qualidade
    df = df.dropna(subset=["order_id", "customer_id", "order_date"])  # registros essenciais
    df = df[df["quantity"] > 0]
    df = df[df["unit_price"] >= 0]

    # Derivadas
    df["revenue"] = df["quantity"] * df["unit_price"]

    # Duplicatas por pedido/linha
    df = df.drop_duplicates(subset=["order_id", "product"])  # heurística simples
    return df


def run() -> Path:
    """Executa a camada Silver: limpeza, tipagem e colunas derivadas."""
    df_bronze = _read_bronze()
    df_silver = _clean(df_bronze)

    ensure_dirs()
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    silver_file = SILVER_DIR / "sales_clean.parquet"
    df_silver.to_parquet(silver_file, engine="pyarrow", index=False)
    print(
        f"[Silver] Linhas: {len(df_silver):,} (de {len(df_bronze):,}) | Arquivo: {silver_file}"
    )
    return silver_file


