## Pipeline de Dados – Arquitetura Medallion (Bronze, Silver, Gold)

Este projeto implementa uma pipeline de dados local seguindo a arquitetura Medallion:

- Bronze: ingestão de dados brutos em Parquet
- Silver: padronização/limpeza e tipagem
- Gold: agregações e data marts analíticos

Stack local escolhida para simplicidade e rodar bem no Windows:
- Python + Pandas + PyArrow + DuckDB

### Pré‑requisitos
- Python 3.10+ instalado no sistema

### Como executar
1. Criar ambiente virtual e instalar dependências:
   ```powershell
python -m venv .venv
./.venv/Scripts/python.exe -m pip install --upgrade pip
./.venv/Scripts/python.exe -m pip install -r requirements.txt
   ```

2. Rodar a pipeline completa (gera dados sintéticos se não houver CSV em `data/raw`):
   ```powershell
./.venv/Scripts/python.exe -m src.pipeline.orchestrate run-all
   ```

3. Executar etapas individuais:
   ```powershell
./.venv/Scripts/python.exe -m src.pipeline.orchestrate bronze
./.venv/Scripts/python.exe -m src.pipeline.orchestrate silver
./.venv/Scripts/python.exe -m src.pipeline.orchestrate gold
   ```

4. Estrutura de diretórios gerada:
```
data/
  raw/               # coloque CSVs aqui (opcional)
  bronze/
    sales/
  silver/
    sales/
  gold/
    revenue_by_month/
    top_products/
    revenue_by_state/
```

### Sobre os dados
- Se `data/raw` estiver vazio, a etapa Bronze cria um dataset sintético de vendas para permitir execução imediata.
- Se quiser usar seus próprios dados, coloque arquivos CSV em `data/raw/` contendo colunas: `order_id,customer_id,order_date,product,quantity,unit_price,city,state`.

### Observações
- Projeto minimalista, fácil de expandir para Spark/Delta Lake no futuro mantendo o desenho Medallion.


