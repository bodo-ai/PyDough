SELECT
  calendar.ca_dt AS start_of_period,
  COUNT(*) AS n_purchases
FROM main.calendar AS calendar
JOIN main.calendar AS calendar_2
  ON calendar.ca_dt <= calendar_2.ca_dt
  AND calendar_2.ca_dt < CAST(calendar.ca_dt AS TIMESTAMP) + INTERVAL '5 DAY'
JOIN main.devices AS devices
  ON calendar_2.ca_dt = DATE_TRUNC('DAY', CAST(devices.de_purchase_ts AS TIMESTAMP))
WHERE
  EXTRACT(YEAR FROM CAST(calendar.ca_dt AS TIMESTAMP)) = 2024
GROUP BY
  1
ORDER BY
  2 DESC NULLS LAST,
  1 NULLS FIRST
LIMIT 1
