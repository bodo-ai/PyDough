WITH _t2 AS (
  SELECT
    ca_dt
  FROM main.calendar
), _t0 AS (
  SELECT
    COUNT() AS n_purchases,
    MAX(_s1.ca_dt) AS start_of_period
  FROM _t2 AS _t2
  CROSS JOIN _t2 AS _s1
  JOIN main.devices AS devices
    ON _s1.ca_dt = DATE(devices.de_purchase_ts, 'start of day')
  WHERE
    CAST(STRFTIME('%Y', _t2.ca_dt) AS INTEGER) = 2024
    AND _s1.ca_dt < DATETIME(_s1.ca_dt, '5 day')
    AND _s1.ca_dt >= _s1.ca_dt
  GROUP BY
    _s1.ca_dt
)
SELECT
  start_of_period,
  n_purchases
FROM _t0
ORDER BY
  n_purchases DESC,
  start_of_period
LIMIT 1
