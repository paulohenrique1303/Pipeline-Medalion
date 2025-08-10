## Pipeline de Dados – Arquitetura Medallion (Bronze ▸ Silver ▸ Gold)

Pipeline local e minimalista seguindo o padrão Medallion para ingestão, qualidade e consumo analítico de dados.

- Bronze: ingestão de dados brutos e padronização em Parquet
- Silver: limpeza, tipagem, normalização de texto e colunas derivadas
- Gold: agregações analíticas e data marts prontos para consumo

Tecnologias: Python, Pandas, PyArrow e DuckDB.

### Pré‑requisitos
- Python 3.10+ instalado

### Estrutura do projeto
```
data/
  raw/                      # CSVs de entrada (opcional; gerado sintético se vazio)
  bronze/
    sales/
  silver/
    sales/
  gold/
    revenue_by_month/
    top_products/
    revenue_by_state/
src/
  pipeline/
    orchestrate.py          # CLI de orquestração
    bronze.py               # camada Bronze
    silver.py               # camada Silver
    gold.py                 # camada Gold
    io_paths.py             # convenções de caminhos
requirements.txt
README.md
```

### Como executar
- Executar a pipeline completa (gera dados sintéticos se `data/raw` estiver vazio):
  - Windows:
    ```powershell
    ./.venv/Scripts/python.exe -m src.pipeline.orchestrate run-all
    ```
  - Linux/macOS (após ativar o venv):
    ```bash
    python -m src.pipeline.orchestrate run-all
    ```

- Executar etapas individuais:
  ```bash
  python -m src.pipeline.orchestrate bronze
  python -m src.pipeline.orchestrate silver
  python -m src.pipeline.orchestrate gold
  ```

- Limpar artefatos processados (recria diretórios vazios):
  ```bash
  python -m src.pipeline.orchestrate clean
  ```

### Notas de implementação
- Bronze lê CSV(s) e escreve `data/bronze/sales/sales.parquet`.
- Silver aplica limpeza e deriva `revenue`, salvando em `data/silver/sales/sales_clean.parquet`.
- Gold cria três data marts: por mês, por produto e por estado.

