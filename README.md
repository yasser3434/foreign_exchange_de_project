# FX Pipeline — Foreign Exchange Rates ETL

A production-grade ETL pipeline that ingests daily FX rates for 7 European currencies, computes all cross-currency pairs, and loads them into a SQLite data warehouse. Orchestrated with Apache Airflow running on Docker.

## Currencies

NOK, EUR, SEK, PLN, RON, DKK, CZK — all 42 cross-pairs daily.

## Architecture

```
                    ┌──────────────┐
                    │ ExchangeRate │
                    │     API      │
                    └──────┬───────┘
                           │
              ┌────────────▼────────────┐
              │     Task 1: Extract     │
              │  latest_fx() / history  │
              │  Saves to raw_fx_rates  │
              └────────────┬────────────┘
                           │
              ┌────────────▼────────────┐
              │   Task 2: Transform     │
              │  Compute 42 cross-pairs │
              │  Load into fact table   │
              └────────────┬────────────┘
                           │
              ┌────────────▼────────────┐
              │    SQLite Warehouse     │
              │  dim_currency           │
              │  dim_date               │
              │  raw_fx_rates           │
              │  fact_fx_rates          │
              └─────────────────────────┘
```

## Project Structure

```
fx_pipeline/
├── dags/
│   ├── fx_pipeline_dag.py           # Airflow DAG definition
│   └── scripts/
│       ├── extract.py               # API extraction (latest + history)
│       ├── transform.py             # Cross-pair computation + fact loading
│       └── load_dim_tables.py       # Dimension tables setup (run once)
├── sql/
│   ├── ddl/
│   │   ├── raw_fx_rates.sql         # Raw table DDL
│   │   ├── dim_currencies.sql       # Currency dimension DDL
│   │   ├── dim_date.sql             # Date dimension DDL
│   │   └── fact_fx_rates.sql        # Fact table DDL
│   └── queries/
│       ├── ytd_calculations.sql     # Year-to-Date metrics
│       └── example_queries.sql      # Sample queries
├── tests/
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_load.py
├── data/                            # SQLite database (gitignored)
├── logs/                            # Airflow logs
├── plugins/                         # Airflow plugins
├── config/                          # Airflow config
├── notebooks/                       # Jupyter notebooks for exploration
├── docker-compose.yaml              # Airflow infrastructure
├── .env                             # Secrets (gitignored)
├── .env.example                     # Template for .env
├── .gitignore
├── .pre-commit-config.yaml          # Pre-commit hooks
├── .github/workflows/ci.yml         # GitHub Actions CI
├── pyproject.toml                   # Ruff, pytest, SQLFluff config
├── requirements.txt
└── README.md
```

## Setup

### 1. Clone and configure environment

```bash
git clone <repo-url>
cd fx_pipeline
cp .env.example .env
```

Edit `.env` with your values:

```
API_KEY=your_exchangerate_api_key
AIRFLOW_UID=1000
```

To find your Airflow UID on Linux:

```bash
echo $(id -u)
```


### 2. Install Python dependencies (local development)

```bash
pip install -r requirements.txt
```

### 3. Setup pre-commit hooks

Pre-commit runs Ruff (linter + formatter) and SQLFluff (SQL linter) automatically before every `git commit`. If checks fail, the commit is blocked until you fix the issues.

```bash
pip install pre-commit
pre-commit install
```

### 4. Start Airflow

```bash
docker-compose up -d
```

Access the Airflow UI at `http://localhost:8080` (login: airflow / airflow).


## Data Source

**ExchangeRate-API** (https://www.exchangerate-api.com/)

We use EUR as the single base currency and derive all cross-pairs mathematically. This reduces API calls from 7 × 365 = 2,555 to just 365 per year (one call per day).

The derivation formula: `rate(A → B) = EUR_rate(B) / EUR_rate(A)`. Since both rates share EUR as the base.

## Schema Design

### Star Schema

The warehouse follows a star schema pattern, making it easy to join FX data with any other fact table in the warehouse:

```
dim_date  ←──  fact_fx_rates  ──→  dim_currency (base)
                     │
                     └──────────→  raw_fx_rates
```

### Why long format (not wide)?

The fact table stores one row per date per currency pair (long format):

```
date       | base_currency | target_currency | rate
2025-06-15 | NOK           | EUR             | 0.088
2025-06-15 | NOK           | SEK             | 0.952
```

Not wide format (currencies as columns):

```
date       | NOK  | EUR | SEK | ...
2025-06-15 | 1    | ... | ... |
```

Why :

- **Easy joins**: Any table with a `currency` column can join directly with `JOIN fx ON t.currency = fx.base_currency AND t.date = fx.date`. Wide format requires CASE statements for every currency.
- **Scalable**: Adding a new currency (e.g., GBP) is just new rows. No `ALTER TABLE`, no query changes.

### Tables

**dim_currency** — descriptive attributes for each currency

**dim_date** — shared calendar dimension with year, fiscal_year, month, quarter.

**raw_fx_rates** — EUR-based rates as received from the API. One row per day. Has a PRIMARY KEY on `date` to prevent duplicates.

**fact_fx_rates** — all 42 cross-pairs per day. Composite PRIMARY KEY on `(date, base_currency, target_currency)` to prevent duplicates.

## Idempotency and Duplicate Prevention

All inserts use `INSERT OR IGNORE` combined with PRIMARY KEY constraints. This means:

- Running the pipeline twice for the same date produces the same result
- No duplicate rows
- Safe to re-run after failures

**Important**: Do not use pandas `to_sql(if_exists="append")` on tables with primary keys — it uses plain `INSERT` and will crash on duplicates. Always use `cursor.executemany()` with `INSERT OR IGNORE`.

Also note: `to_sql(if_exists="replace")` drops and recreates the table without constraints, destroying your PRIMARY KEY.

## YTD Calculations

Year-to-Date metrics are computed via SQL (see `sql/queries/ytd_calculations.sql`):

- **YTD change %**: `((rate_today - rate_jan1) / rate_jan1) * 100`
- **YTD average rate**: mean of all daily rates from Jan 1 to today
- **YTD high / low**: max and min rate within the current year

## Example Queries

### Lookup rate for a specific date and pair

```sql
SELECT rate
FROM fact_fx_rates
WHERE date = '2026-01-01'
  AND base_currency = 'NOK'
  AND target_currency = 'EUR';
```


### Monthly average rate

```sql
SELECT
    strftime('%Y-%m', date) AS month,
    ROUND(AVG(rate), 6) AS avg_rate
FROM fact_fx_rates
WHERE base_currency = 'EUR'
  AND target_currency = 'NOK'
  AND strftime('%Y', date) = '2026'
GROUP BY month
ORDER BY month;
```

## Running Tests

```bash
pytest tests/ -v
```

Tests use mocked API responses and in-memory SQLite databases — no real API calls, no network dependency. They validate:

- Extraction returns correct structure and handles API errors
- Transformation produces 1 row for raw_fx_rates rates are postive
- Loading is idempotent (run twice, same result)
- Assertions over the ETL, DDL table, extraction and transformation

## Code Quality

### Linting and formatting

```bash
ruff check .         # lint
ruff format .        # format
sqlfluff lint sql/   # lint SQL
```

All configured in `pyproject.toml` — one file for all tool settings.

### Quality gates

| Layer | When | Where | Purpose |
|-------|------|-------|---------|
| **pyproject.toml** | Manual runs | Local | Configure rules |
| **pre-commit** | On `git commit` | Local | Catch issues before they enter Git |
| **GitHub Actions CI** | On push / PR | GitHub | Safety net, catches anything pre-commit missed |

Pre-commit auto-fixes safe issues (import sorting, formatting). Unsafe fixes (renaming variables, removing code) require manual intervention.

## Orchestration

### Airflow DAG

The `fx_pipeline` DAG runs three tasks daily at 4:15 PM :

```
ddl_tables >> extract_task >>  transfrom_load_task
```

- `catchup=False` — does not backfill
- `retries=1` with 30-sec delay

### Docker

Airflow runs via the official docker-compose with CeleryExecutor, Redis, and PostgreSQL. Key volume mounts:

```yaml
volumes:
  - ./dags:/opt/airflow/dags      # DAG code
  - ./data:/opt/airflow/data      # SQLite database
  - ./sql:/opt/airflow/sql        # DDL files
  - ./logs:/opt/airflow/logs      # Airflow logs
  - ./config:/opt/airflow/config
  - ./plugins:/opt/airflow/plugins
```

```
env_file: - ${ENV_FILE_PATH:-.env}
```
This takes automatically your variables

### Azure Deployment Proposal

For production, this pipeline would run on Azure using:

- **Azure Data Factory** with Managed Airflow for orchestration
- **Azure SQL Database** or **Synapse Analytics** replacing SQLite as the warehouse
- **Azure Key Vault** for API key management (replacing `.env`)
- **Azure Blob Storage** for raw data landing zone
- **Azure Monitor** for logging and alerting


### Check row counts

```sql
-- Should be 42 per day
SELECT date, COUNT(*) as pairs
FROM fact_fx_rates
GROUP BY date
ORDER BY date DESC
LIMIT 5;
```

### Check for duplicates

```sql
SELECT date, base_currency, target_currency, COUNT(*)
FROM fact_fx_rates
GROUP BY date, base_currency, target_currency
HAVING COUNT(*) > 1;
```

This should return **zero rows**.

### Verify inverse pair consistency

```sql
SELECT
    a.date,
    a.base_currency,
    a.target_currency,
    ROUND(a.rate * b.rate, 4) AS product
FROM fact_fx_rates a
JOIN fact_fx_rates b
    ON a.date = b.date
    AND a.base_currency = b.target_currency
    AND a.target_currency = b.base_currency
WHERE ABS(a.rate * b.rate - 1.0) > 0.001;
```

This should return **zero rows** (every pair × its inverse ≈ 1.0).
