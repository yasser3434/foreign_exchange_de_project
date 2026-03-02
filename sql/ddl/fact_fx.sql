
CREATE TABLE IF NOT EXISTS fact_fx_rates(
    date TEXT NOT NULL
    , base_currency TEXT NOT NULL
    , target_currency TEXT NOT NULL
    , rate REAL NOT NULL
    , PRIMARY KEY(date, base_currency, target_currency)
);