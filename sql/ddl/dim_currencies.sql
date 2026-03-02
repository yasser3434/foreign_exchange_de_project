
CREATE TABLE IF NOT EXISTS dim_currencies(
        currency TEXT PRIMARY KEY
        , locale TEXT NOT NULL
        , two_letter_code TEXT NOT NULL
        , currency_name TEXT NOT NULL
        , currency_name_short TEXT NOT NULL
    );