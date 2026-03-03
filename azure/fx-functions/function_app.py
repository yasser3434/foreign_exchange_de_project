import azure.functions as func
import logging
import os
import json
import pyodbc
import requests
import pandas as pd
from datetime import datetime, timedelta
from itertools import permutations

app = func.FunctionApp()

# ============================ Config

API_KEY = os.getenv("API_KEY")
BASE_URL = "https://v6.exchangerate-api.com/v6"
CURRENCIES = ["EUR", "NOK", "SEK", "PLN", "RON", "DKK", "CZK"]


def get_connection():
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={os.getenv('AZURE_SQL_SERVER')};"
        f"DATABASE={os.getenv('AZURE_SQL_DATABASE')};"
        f"UID={os.getenv('AZURE_SQL_USERNAME')};"
        f"PWD={os.getenv('AZURE_SQL_PASSWORD')};"
        f"MARS_Connection=Yes;"
    )


# ============================ Extract

@app.route(route="extract", methods=["POST"])
def extract(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Extract function triggered!")

    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Check if raw_fx_rates has data
        cursor.execute("SELECT COUNT(*) FROM raw_fx_rates")
        row_count = cursor.fetchone()[0]

        # If raw_fx_rates empty, ingest history
        if row_count == 0:
            records = []
            current_date = datetime(2025, 1, 1)
            end_date = datetime.today()

            while current_date < end_date:
                url = f"{BASE_URL}/{API_KEY}/history/EUR/{current_date.year}/{current_date.month}/{current_date.day}"
                response = requests.get(url)
                data = response.json()

                try:
                    rates = data["conversion_rates"]
                    record = {"date": current_date.strftime("%Y-%m-%d")}
                    for currency in CURRENCIES:
                        record[currency] = rates.get(currency)
                    records.append(record)
                except Exception as e:
                    logging.error(f"{current_date.strftime('%Y-%m-%d')} error: {e}")

                current_date += timedelta(days=1)

            for record in records:
                cursor.execute("""
                    MERGE raw_fx_rates AS target
                    USING (SELECT ? AS date) AS source
                    ON target.date = source.date
                    WHEN NOT MATCHED THEN
                        INSERT (date, EUR, NOK, SEK, PLN, RON, DKK, CZK)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                """,
                    record["date"],
                    record["date"],
                    record["EUR"], record["NOK"], record["SEK"],
                    record["PLN"], record["RON"], record["DKK"], record["CZK"],
                )

            conn.commit()
            msg = f"Backfill: Extracted {len(records)} rows."


        # If data in raw_fx_rates, call append daily
        else:
            url = f"{BASE_URL}/{API_KEY}/latest/EUR"
            response = requests.get(url)
            data = response.json()

            rates = data["conversion_rates"]
            date = datetime.today().strftime("%Y-%m-%d")

            cursor.execute("""
                MERGE raw_fx_rates AS target
                USING (SELECT ? AS date) AS source
                ON target.date = source.date
                WHEN NOT MATCHED THEN
                    INSERT (date, EUR, NOK, SEK, PLN, RON, DKK, CZK)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """,
                date, date,
                rates.get("EUR"), rates.get("NOK"), rates.get("SEK"),
                rates.get("PLN"), rates.get("RON"), rates.get("DKK"), rates.get("CZK"),
            )

            conn.commit()
            msg = f"Daily: extracted 1 row for {date}."

        conn.close()
        logging.info(msg)
        return func.HttpResponse(json.dumps({"status": "success", "message": msg}), status_code=200)

    except Exception as e:
        logging.error(f"Extract failed: {e}")
        return func.HttpResponse(json.dumps({"status": "error", "message": str(e)}), status_code=500)


# ============================ Transform

@app.route(route="transform", methods=["POST"])
def transform(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Transform function triggered")

    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Check if fact table has data
        cursor.execute("SELECT COUNT(*) FROM fact_fx_rates")
        row_count = cursor.fetchone()[0]

        if row_count == 0:
            df = pd.read_sql("SELECT * FROM raw_fx_rates", conn)
        else:
            df = pd.read_sql("SELECT TOP 1 * FROM raw_fx_rates ORDER BY date DESC", conn)

        cross_pairs = []
        for _, row in df.iterrows():
            for base, target in permutations(CURRENCIES, 2):
                rate = row[target] / row[base]
                cross_pairs.append({
                    "date": row["date"],
                    "base_currency": base,
                    "target_currency": target,
                    "rate": round(rate, 6),
                })

        for pair in cross_pairs:
            cursor.execute("""
                MERGE fact_fx_rates AS target
                USING (SELECT ? AS date, ? AS base_currency, ? AS target_currency) AS source
                ON target.date = source.date
                    AND target.base_currency = source.base_currency
                    AND target.target_currency = source.target_currency
                WHEN NOT MATCHED THEN
                    INSERT (date, base_currency, target_currency, rate)
                    VALUES (?, ?, ?, ?);
            """,
                pair["date"], pair["base_currency"], pair["target_currency"],
                pair["date"], pair["base_currency"], pair["target_currency"], pair["rate"],
            )

        conn.commit()

        # Append new date to dim_date
        if row_count > 0:
            today = datetime.today()
            fiscal_year = today.year + 1 if today.month >= 10 else today.year
            cursor.execute("""
                MERGE dim_date AS target
                USING (SELECT ? AS date) AS source
                ON target.date = source.date
                WHEN NOT MATCHED THEN
                    INSERT (date, year, fiscal_year, month, day, quarter)
                    VALUES (?, ?, ?, ?, ?, ?);
            """,
                today.strftime("%Y-%m-%d"),
                today.strftime("%Y-%m-%d"),
                today.year, fiscal_year, today.month, today.day,
                (today.month - 1) // 3 + 1,
            )
            conn.commit()

        conn.close()
        msg = f"Transformed {len(cross_pairs)} cross-pair records."
        logging.info(msg)
        return func.HttpResponse(json.dumps({"status": "success", "message": msg}), status_code=200)

    except Exception as e:
        logging.error(f"Transform failed: {e}")
        return func.HttpResponse(json.dumps({"status": "error", "message": str(e)}), status_code=500)