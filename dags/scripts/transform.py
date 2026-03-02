import logging
import os
import sqlite3
from datetime import datetime
from itertools import permutations

import pandas as pd
from dotenv import load_dotenv
from scripts.ddl import load_dim_date

load_dotenv()
api_key = os.getenv("api_key")


def transfrom():
    db_path = "/opt/airflow/data/fx_warehouse.sqlite"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    currencies = ["EUR", "NOK", "SEK", "PLN", "RON", "DKK", "CZK"]

    # IF TABLE fact_fx_rates EXISTS APPEND LAST DAY DATA
    tables = pd.read_sql(
        """
        SELECT name FROM sqlite_master
        WHERE type = 'table' AND name = 'fact_fx_rates'
    """,
        conn,
    )

    # IF: TABLE fact_fx_rates DOES NOT EXIST OR HAS 0 ROWS TRANSFORM ALL DATA
    if len(tables) == 0 or len(pd.read_sql("SELECT 1 FROM fact_fx_rates LIMIT 1", conn)) == 0:
        df = pd.read_sql(""" SELECT * FROM raw_fx_rates """, conn)
        cross_pairs = []

        try:
            for _, row in df.iterrows():
                for base, target in permutations(currencies, 2):
                    rate = row[target] / row[base]
                    cross_pairs.append(
                        {
                            "date": row["date"],
                            "base_currency": base,
                            "target_currency": target,
                            "rate": round(rate, 6),
                        }
                    )
        except Exception as e:
            logging.error(f"{datetime.today().strftime('%Y-%m-%d')} error: {e}")

        df_cross_pairs = pd.DataFrame(cross_pairs)

        # Validation    
        assert(float(df_cross_pairs.iloc[-1]['rate']) > 0, f"Rate should always be positif, got {float(df_cross_pairs.iloc[-1]['rate'])}" )

        records = df_cross_pairs[
            ["date", "base_currency", "target_currency", "rate"]
        ].values.tolist()

        # Prevent duplicates
        cursor.executemany(
            """INSERT OR IGNORE INTO fact_fx_rates
            (date, base_currency, target_currency, rate)
            VALUES (?, ?, ?, ?)""",
            records,
        )
        conn.commit()

        logging.info(f"Transformed data and added {len(pd.DataFrame(cross_pairs))} rows!")

    else:
        df = pd.read_sql(
            """
            WITH SUB AS(
            SELECT
                *
                , ROW_NUMBER() OVER(PARTITION BY EUR ORDER BY date DESC) RN
            FROM raw_fx_rates
            )

            SELECT
                *
            FROM SUB
            WHERE RN = 1

        """,
            conn,
        )

        cross_pairs = []

        try:
            for _, row in df.iterrows():
                for base, target in permutations(currencies, 2):
                    rate = row[target] / row[base]
                    cross_pairs.append(
                        {
                            "date": row["date"],
                            "base_currency": base,
                            "target_currency": target,
                            "rate": round(rate, 6),
                        }
                    )
        except Exception as e:
            logging.error(f"{datetime.today().strftime('%Y-%m-%d')} error: {e}")

        assert (
            len(pd.DataFrame(cross_pairs)) == 42
        ), f"42 rows are expected, got {len(pd.DataFrame(cross_pairs))}"

        # Append new data
        df_cross_pairs = pd.DataFrame(cross_pairs)

        # Validation    
        assert(float(df_cross_pairs.iloc[-1]['rate']) > 0, f"Rate should always be positif, got {float(df_cross_pairs.iloc[-1]['rate'])}" )

        records = df_cross_pairs[
            ["date", "base_currency", "target_currency", "rate"]
        ].values.tolist()

        # Prevent duplicates
        cursor.executemany(
            """INSERT OR IGNORE INTO fact_fx_rates
            (date, base_currency, target_currency, rate)
            VALUES (?, ?, ?, ?)""",
            records,
        )
        conn.commit()

        logging.info(f"Transformed data and added {len(records)} rows!")

        # Append new date
        load_dim_date(datetime.today())

        logging.info(f"Appended last date {datetime.today()} | {len(records)} rows!")
