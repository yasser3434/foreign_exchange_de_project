
CREATE TABLE IF NOT EXISTS dim_date(
    date TEXT PRIMARY KEY,
    year INTEGER NOT NULL,
    fiscal_year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    quarter INTEGER NOT NULL
);
