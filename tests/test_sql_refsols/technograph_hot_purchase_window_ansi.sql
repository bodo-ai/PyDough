WITH _t0 AS (
  SELECT
    COUNT() AS n_purchases,
    ANY_VALUE(calendar.ca_dt) AS start_of_period
  FROM main.calendar AS calendar
  CROSS JOIN main.calendar AS calendar_2
  JOIN main.devices AS devices
    ON calendar_2.ca_dt = DATE_TRUNC('DAY', CAST(devices.de_purchase_ts AS TIMESTAMP))
  WHERE
    EXTRACT(YEAR FROM calendar.ca_dt) = 2024
    AND calendar.ca_dt <= calendar_2.ca_dt
    AND calendar_2.ca_dt < DATE_ADD(CAST(calendar.ca_dt AS TIMESTAMP), 5, 'DAY')
  GROUP BY
    calendar.ca_dt
)
SELECT
  start_of_period,
  n_purchases
FROM _t0
ORDER BY
  n_purchases DESC,
  start_of_period
LIMIT 1
