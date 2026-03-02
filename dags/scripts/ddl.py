import os
import sqlite3
import logging
import requests
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()
api_key = os.getenv("api_key")


# =================================== Load - Dim currencies

base_url = "https://v6.exchangerate-api.com/v6"
currencies = ["NOK", "EUR", "SEK", "PLN", "RON", "DKK", "CZK"]


def load_dim_curr():
    # CREATE DIMENSION TABLES IF NOT EXITSTS
    db_path = "/opt/airflow/data/fx_warehouse.sqlite"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    dim = []

    try:
        logging.info("Loading currencies dim table...")
        for currency in currencies:
            response = requests.get(f"{base_url}/{api_key}/enriched/EUR/{currency}")
            response.raise_for_status()

            data = response.json()

            dim.append(
                {
                    "currency": data["target_code"],
                    "locale": data["target_data"]["locale"],
                    "two_letter_code": data["target_data"]["two_letter_code"],
                    "currency_name": data["target_data"]["currency_name"],
                    "currency_name_short": data["target_data"]["currency_name_short"],
                }
            )
        df_dim = pd.DataFrame(dim)
        df_dim.to_sql("dim_currencies", conn, if_exists="replace", index=False)

        logging.info(f"Updated currencies dim : {len(df_dim)} currencies!")

    except Exception:
        logging.error("Error loading cuurrencies dim table")


# =================================== Load - Dim date


def load_dim_date(start_date=datetime(2026, 1, 1)):
    # CREATE DIMENSION TABLES IF NOT EXITSTS
    db_path = "/opt/airflow/data/fx_warehouse.sqlite"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    date_records = []
    current = start_date
    end = datetime.today()

    while current <= end:
        fiscal_year = current.year + 1 if current.month >= 10 else current.year
        date_records.append(
            (
                current.strftime("%Y-%m-%d"),
                current.year,
                fiscal_year,
                current.month,
                current.day,
                (current.month - 1) // 3 + 1,
            )
        )

        current += timedelta(days=1)

    cursor.executemany(
        """
        INSERT OR IGNORE INTO dim_date(date, year, fiscal_year, month, day, quarter)
        VALUES(?, ?, ?, ?, ?, ?)
    """,
        date_records,
    )
    conn.commit()

    logging.info(f"Updated date dim : {len(date_records)} dates!")


def load_dim_run():
    # CREATE DIMENSION TABLES IF NOT EXITSTS
    db_path = "/opt/airflow/data/fx_warehouse.sqlite"
    ddl_path = "/opt/airflow/sql/ddl"

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    if (
        len(
            pd.read_sql(
                """ SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'dim_currencies' """,
                conn,
            )
        )
        == 0
    ):
        folder = Path(ddl_path)
        if not folder.exists():
            raise FileNotFoundError(f"Folder {ddl_path} not found")

        sql_files = folder.glob("*.sql")

        try:
            for file in sql_files:
                sql_file = file.read_text()
                # print(sql_file)
                cursor.execute(sql_file)
                logging.info(f"Executed {file} successfully")
        except Exception:
            logging.error(f"Error with {file}")

        load_dim_curr()
        load_dim_date()
