
import os
import logging
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta
from itertools import permutations
from extract import latest_fx, history_fx

load_dotenv()
api_key = os.getenv("api_key")

base_url = "https://v6.exchangerate-api.com/v6"
currencies = ['NOK', 'EUR', 'SEK', 'PLN', 'RON', 'DKK', 'CZK']

def tranform_records(latest_or_history="latest")-> pd.DataFrame:
    """
    if   fx_extract = latest_fx
        then it will return lastest date data
    elif fx_extract = history_fx
        then it will return history data
    """
    
    if   latest_or_history == 'latest':
        df = latest_fx(base_url)
    elif latest_or_history == 'history':
        df = history_fx(base_url)

    cross_pairs = []

    try:
        for _, row in df.iterrows():
            for base, target in permutations(currencies, 2):
                rate = row[target] / row[base]
                cross_pairs.append({
                    "date": row["date"],
                    "base_currency": base,
                    "target_currency": target,
                    "rate": round(rate, 6)
                })
    except Exception as e:
        logging.error(f"{datetime.today().strftime('%Y-%m-%d')} error: {e}")

    if latest_or_history == 'Latest':
        #Validation 
        assert(len(pd.DataFrame(cross_pairs)) == 42), f"42 rows are expected, got {len(pd.DataFrame(cross_pairs))}"

    df_pairs = pd.DataFrame(cross_pairs)
    return df_pairs
