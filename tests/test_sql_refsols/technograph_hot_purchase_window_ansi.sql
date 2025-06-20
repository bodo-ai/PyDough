WITH _t0 AS (
  SELECT
    COUNT(*) AS n_purchases,
    ANY_VALUE(_s0.ca_dt) AS start_of_period
  FROM main.calendar AS _s0
  CROSS JOIN main.calendar AS _s1
  JOIN main.devices AS _s4
    ON _s1.ca_dt = DATE_TRUNC('DAY', CAST(_s4.de_purchase_ts AS TIMESTAMP))
  WHERE
    EXTRACT(YEAR FROM _s0.ca_dt) = 2024
    AND _s0.ca_dt <= _s1.ca_dt
    AND _s1.ca_dt < DATE_ADD(CAST(_s0.ca_dt AS TIMESTAMP), 5, 'DAY')
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
