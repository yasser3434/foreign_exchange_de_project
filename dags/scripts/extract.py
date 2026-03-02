import logging
import os
import sqlite3
from datetime import datetime, timedelta

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("api_key")


def latest_fx(base_url, currencies, base_currency="EUR") -> pd.DataFrame:
    """
    Get daily data
    """

    base_url = "https://v6.exchangerate-api.com/v6"
    currencies = ["EUR", "NOK", "SEK", "PLN", "RON", "DKK", "CZK"]

    # Get data
    url = f"{base_url}/{api_key}/latest/{base_currency}"
    response = requests.get(url)
    response.raise_for_status()

    data = response.json()

    # Calculate cross pairs
    records = []

    try:
        # Extract currencies rates
        rates = data["conversion_rates"]
        record = {"date": datetime.today().strftime("%Y-%m-%d")}

        for currency in currencies:
            record[currency] = rates.get(currency)

        records.append(record)

        # Validation
        assert (
            len(pd.DataFrame(records)) == 1
        ), f"1 row is expected, got {len(pd.DataFrame(records))}"
        return pd.DataFrame(records)

    except Exception as e:
        logging.error(f"{datetime.today().strftime('%Y-%m-%d')} error: {e}")
        return pd.DataFrame()


def history_fx(
    base_url, currencies, start_date=datetime(2026, 1, 1), base_currency="EUR"
) -> pd.DataFrame:
    """
    Get history data from start_date to end
    """

    records = []
    end_date = datetime.today()
    current_date = start_date

    while current_date < end_date:
        year = current_date.year
        month = current_date.month
        day = current_date.day

        url = f"{base_url}/{api_key}/history/{base_currency}/{year}/{month}/{day}"
        response = requests.get(url)
        data = response.json()

        try:
            rates = data["conversion_rates"]
            record = {"date": current_date.strftime("%Y-%m-%d")}

            for currency in currencies:
                record[currency] = rates.get(currency)

            records.append(record)

            current_date += timedelta(days=1)
        except Exception as e:
            logging.error(f"{datetime.today().strftime('%Y-%m-%d')} error: {e}")

    return pd.DataFrame(records)


def extract_run():
    base_url = "https://v6.exchangerate-api.com/v6"
    currencies = ["EUR", "NOK", "SEK", "PLN", "RON", "DKK", "CZK"]

    db_path = "/opt/airflow/data//fx_warehouse.sqlite"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    tables = pd.read_sql(
        """
        SELECT name FROM sqlite_master
        WHERE type = 'table' AND name = 'raw_fx_rates'
    """,
        conn,
    )

    if len(tables) == 0 or len(pd.read_sql("SELECT 1 FROM raw_fx_rates LIMIT 1", conn)) == 0:
        df = history_fx(base_url, currencies)
        records = df[["date", "EUR", "NOK", "SEK", "PLN", "RON", "DKK", "CZK"]].values.tolist()

        cursor.executemany(
            """
            INSERT OR IGNORE INTO raw_fx_rates(date, EUR, NOK, SEK, PLN, RON, DKK, CZK)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
        """,
            records,
        )
        conn.commit()

        logging.info(f"Extracted {len(df)} rows!")

    else:
        df = latest_fx(base_url, currencies)

        # KEEP IT TO SHOW DUPLICATES !!
        # df.to_sql("raw_fx_rates", conn, if_exists="append", index=False)

        records = df[["date", "EUR", "NOK", "SEK", "PLN", "RON", "DKK", "CZK"]].values.tolist()
        cursor.executemany(
            """
            INSERT OR IGNORE INTO raw_fx_rates(date, EUR, NOK, SEK, PLN, RON, DKK, CZK)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
        """,
            records,
        )
        conn.commit()

        logging.info(f"Extracted {len(records)} rows!")
