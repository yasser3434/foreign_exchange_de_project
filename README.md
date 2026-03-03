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
├── azure_functions/
│   ├── function_app.py                 # Azure Functions (extract + transform)
│   ├── requirements.txt                # Function dependencies
│   ├── host.json                       # Azure Functions config
│   └── local.settings.json             # Local secrets 
|
├── dags/
│   ├── fx_pipeline_dag.py           
│   └── scripts/
│       ├── extract.py               
│       ├── transform.py             
│       └── ddl.py                   
├── sql/
│   ├── ddl/
│   │   ├── raw_fx_rates.sql         # Raw table DDL
│   │   ├── dim_currencies.sql       # Currency dimension DDL
│   │   ├── dim_date.sql             # Date dimension DDL
│   │   └── fact_fx_rates.sql        # Fact table DDL
│   └── queries/
│       ├── ytd_calculations.sql     # Year-to-Date metrics
│       
├── tests/
│   ├── test_extract.py

├── data/                            
├── logs/                            
├── plugins/                         
├── config/                          
├── notebooks/                       # Jupyter notebooks for exploration
├── docker-compose.yaml              
├── .env                             # Secrets (gitignored)
├── .env.example                     # Template for .env
├── .gitignore
├── .pre-commit-config.yaml          # Pre-commit hooks
├── .github/workflows/ci.yml         # Github actions CI
├── pyproject.toml                   # Ruff(lint and format), pytest, SQLFluff config
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

`http://localhost:8080` (login: airflow / airflow).


## Data Source

**ExchangeRate-API** (https://www.exchangerate-api.com/)

We use EUR as the single base currency and derive all cross-pairs mathematically. 
This reduces API calls from to only 365 per year (one call per day).

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

 **pyproject.toml** 
 **pre-commit** 
 **GitHub Actions CI**

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


## Setup — Cloud Deployment (Azure)

### Step 1: Create Resource Group

In Azure Portal, create a resource group `<my_resource_group>`

### Step 2: Create Azure SQL Database

1. Create a SQL Server with SQL authentication
2. Create a database named `<my_database>`
3. Enable public access, add your IP address and allow Azure services to connect

### Step 3: Create tables
Run the DDL scripts against Azure SQL. Refer to main_azure notebook


### Step 4: Load dimensions (once)

Run the adapted `load_dim_tables`. Uses `MERGE ... WHEN NOT MATCHED` instead of `INSERT OR IGNORE` for idempotent inserts.

### Step 5: Create Azure Function App


1. Deploy via VS Code Azure Functions extension:
- install azure function
- Init the env folder : func init --python --model V2

2. Set environment variables in the Function App:


### Step 6: Test the functions

```bash
curl -X POST "https://<function-app-name>.azurewebsites.net/api/extract?code=<key>"
curl -X POST "https://<function-app-name>.azurewebsites.net/api/transform?code=<key>"
```

### Step 7: Create Azure Data Factory
1. Create a Data Factory resource 
2. Create a pipeline `fx_daily_pipeline`
4. Add two <Web> activities chained together : Extract and Transform
5. Chain: Extract and Transform (green arrow)
6. Add a Schedule trigger : daily at 4:15 PM
7. Publish all



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
