# Design Note — FX Pipeline

## Overview

Design decisions and trade-offs behind the FX rates ETL pipeline.

## Problem Statement

Ingest daily foreign exchange rates for 7 currencies (NOK, EUR, SEK, PLN, RON, DKK, CZK), compute all cross-pairs, and make them available in a warehouse that supports YTD calculations and easy integration with other data.

## Data Source Decision

**Chosen:** ExchangeRate-API with EUR as the single base currency.

We fetch rates for all 7 currencies against EUR in one API call per day, then derive the cross-pairs mathematically using `rate(A→B) = EUR_rate(B) / EUR_rate(A)`. 

**Trade-off:** We depend on EUR being available in every API response. If EUR rates were missing for a day, all 42 cross-pairs will be loss

## Schema Design
**Chosen:** Star schema with a long-format fact table.

The fact table stores one row per date per currency pair (42 rows per day). This was chosen over a wide format (currencies as columns) for three reasons: simple joins instead of writing CASE statements, adding a new currency is easy, and data can be used in any warehouse.

**Trade-off:** The long format uses more rows (42 per day vs 1 in wide format) but for the exercise volume it is negligible

## Idempotency 

**Chosen:** Composite primary keys + `INSERT OR IGNORE` (SQLite) / `MERGE WHEN NOT MATCHED` (Azure SQL)

Every table has a primary key that prevents duplicate inserts. The pipeline can be re-run for the same date without producing duplicates or errors.

**Trade-off:** 
Avoid pandas `to_sql(if_exists="append")` for loading because it uses plain INSERT with no constraint. 
Also avoided `to_sql(if_exists="replace")` because it drops and recreates the table, destroying primary key constraints.

## Deployment

**Chosen:** Local (Airflow + SQLite) and Cloud (Azure Data Factory + Azure Functions + Azure SQL).

Local deployment has been used for testing. SQLite requires no infrastructure and produces a database file. Airflow orchestrate the tasks.

The cloud deployment uses Azure SQL to replaces SQLite with a relational database. Azure Functions execute the Python ETL logic serverlessly : Extract and Transfrom. Azure Data Factory orchestrates the schedule and tasks.


## Orchestration

**Local:** Airflow DAG with two PythonOperator tasks (extract → transform), scheduled via cron (`15 16 * * *`), `catchup=False`

**Cloud:** Azure Data Factory pipeline with two WEB activities calling Azure Functions via HTTP POST, chained with a success dependency, triggered on a daily schedule.

**Trade-off:** ADF doesn't run Python natively, so we use Azure Functions.

## YTD Calculations

**Chosen:** SQL-based calculations using CTEs, not materialized tables.

YTD metrics (change %, average, max, min) are computed at query time by comparing the first rate of the year to the latest rate. 


## Testing Strategy

Unit tests use mocked API responses and SQLite databases. They validate structure (correct columns, row counts), positive rates and idempotency 


## What I Would Do Differently in Production

- Store API keys and database credentials in **Azure Key Vault** instead of environment variables
- Use **Managed Identity** for Azure Function authentication instead of function keys
- Add **Azure Monitor alerts** for pipeline failures and data quality issues
- Implement a **dead letter queue** for failed API calls
- Add a dedicated **data quality task** in the pipeline checking for nulls and anomalous rates
- Consider **Azure Synapse Analytics** if the warehouse needs in the futur complex analytical queries
