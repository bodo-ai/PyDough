SELECT
  ANY_VALUE(calendar.ca_dt) AS start_of_period,
  COUNT(*) AS n_purchases
FROM main.calendar AS calendar
JOIN main.calendar AS calendar_2
  ON calendar.ca_dt <= calendar_2.ca_dt
  AND calendar_2.ca_dt < DATE_ADD(CAST(calendar.ca_dt AS TIMESTAMP), 5, 'DAY')
JOIN main.devices AS devices
  ON calendar_2.ca_dt = DATE_TRUNC('DAY', CAST(devices.de_purchase_ts AS TIMESTAMP))
WHERE
  EXTRACT(YEAR FROM CAST(calendar.ca_dt AS DATETIME)) = 2024
GROUP BY
  calendar.ca_dt
ORDER BY
  n_purchases DESC,
  start_of_period
LIMIT 1
