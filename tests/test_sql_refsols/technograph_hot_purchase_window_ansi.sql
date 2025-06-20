WITH _t3 AS (
  SELECT
    ca_dt AS calendar_day
  FROM main.calendar
), _t0 AS (
  SELECT
    COUNT(*) AS n_purchases,
    ANY_VALUE(_t3.calendar_day) AS start_of_period
  FROM _t3 AS _t3
  CROSS JOIN _t3 AS _s1
  JOIN main.devices AS devices
    ON _s1.calendar_day = DATE_TRUNC('DAY', CAST(devices.de_purchase_ts AS TIMESTAMP))
  WHERE
    EXTRACT(YEAR FROM CAST(_t3.calendar_day AS DATETIME)) = 2024
    AND _s1.calendar_day < DATE_ADD(CAST(_t3.calendar_day AS TIMESTAMP), 5, 'DAY')
    AND _s1.calendar_day >= _t3.calendar_day
  GROUP BY
    _t3.calendar_day
)
SELECT
  start_of_period,
  n_purchases
FROM _t0
ORDER BY
  n_purchases DESC,
  start_of_period
LIMIT 1
