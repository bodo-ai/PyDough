WITH _t0 AS (
  SELECT
    COUNT(*) AS n_purchases,
    MAX(_s0.ca_dt) AS start_of_period
  FROM main.calendar AS _s0
  CROSS JOIN main.calendar AS _s1
  JOIN main.devices AS _s4
    ON _s1.ca_dt = DATE(_s4.de_purchase_ts, 'start of day')
  WHERE
    CAST(STRFTIME('%Y', _s0.ca_dt) AS INTEGER) = 2024
    AND _s0.ca_dt <= _s1.ca_dt
    AND _s1.ca_dt < DATETIME(_s0.ca_dt, '5 day')
  GROUP BY
    _s0.ca_dt
)
SELECT
  start_of_period,
  n_purchases
FROM _t0
ORDER BY
  n_purchases DESC,
  start_of_period
LIMIT 1
