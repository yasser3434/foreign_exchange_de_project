import os
import logging
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()
api_key = os.getenv("api_key")

base_url = "https://v6.exchangerate-api.com/v6"
currencies = ["NOK", "EUR", "SEK", "PLN", "RON", "DKK", "CZK"]


def latest_fx(base_url, currency="EUR") -> pd.DataFrame:
    """
    Get daily data
    """
    # Get data
    url = f"{base_url}/{api_key}/latest/{currency}"
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
    base_url, start_date=datetime(2026, 1, 1), currency="EUR"
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

        url = f"{base_url}/{api_key}/history/{currency}/{year}/{month}/{day}"
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


if __name__ == "main":
    latest_fx(base_url)
