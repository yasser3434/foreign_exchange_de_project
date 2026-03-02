ytd_query = """
    WITH first_rate AS(
        SELECT
            base_currency	
            , target_currency
            , date AS first_date
            , rate AS first_rate	
        FROM fact_fx_rates 
        WHERE date = (SELECT MIN(DATE) FROM fact_fx_rates WHERE strftime('%Y', date) = strftime('%Y', 'now')) 
    ), 

    last_rate AS(
        SELECT
            base_currency	
            , target_currency
            , date AS last_date
            , rate AS last_rate
        FROM fact_fx_rates 
        WHERE date = (SELECT MAX(DATE) FROM fact_fx_rates WHERE strftime('%Y', date) = strftime('%Y', 'now'))	
    ),
    
    all_rates AS(
        SELECT
            base_currency	
            , target_currency
            , MIN(rate) AS min_rate
            , MAX(rate) AS max_rate
            , AVG(rate) AS avg_rate
        FROM fact_fx_rates
        WHERE strftime('%Y', date) = strftime('%Y', 'now')
        GROUP BY base_currency	
            , target_currency
    ) 

    SELECT
        FR.*
        , LR.last_date
        , LR.last_rate
        , AR.min_rate
        , AR.max_rate
        , AR.avg_rate
        , ROUND((LR.last_rate - FR.first_rate) / FR.first_rate * 100, 2) AS YTD_change_pct 
    FROM first_rate AS FR
    LEFT JOIN last_rate AS LR 
        ON  FR.base_currency = LR.base_currency
        AND FR.target_currency = LR.target_currency

    LEFT JOIN all_rates AS AR
        ON  FR.base_currency = AR.base_currency
        AND FR.target_currency = AR.target_currency
"""