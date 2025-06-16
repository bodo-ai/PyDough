WITH _t2 AS (
  SELECT
    ca_dt
  FROM main.calendar
), _t0 AS (
  SELECT
    COUNT() AS n_purchases,
    ANY_VALUE(_s1.ca_dt) AS start_of_period
  FROM _t2 AS _t2
  CROSS JOIN _t2 AS _s1
  JOIN main.devices AS devices
    ON _s1.ca_dt = DATE_TRUNC('DAY', CAST(devices.de_purchase_ts AS TIMESTAMP))
  WHERE
    EXTRACT(YEAR FROM _t2.ca_dt) = 2024
    AND _s1.ca_dt < DATE_ADD(CAST(_s1.ca_dt AS TIMESTAMP), 5, 'DAY')
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
