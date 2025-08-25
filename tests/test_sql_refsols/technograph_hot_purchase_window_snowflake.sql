SELECT
  calendar.ca_dt AS start_of_period,
  COUNT(*) AS n_purchases
FROM main.calendar AS calendar
JOIN main.calendar AS calendar_2
  ON calendar.ca_dt <= calendar_2.ca_dt
  AND calendar_2.ca_dt < DATEADD(DAY, 5, CAST(calendar.ca_dt AS TIMESTAMP))
JOIN main.devices AS devices
  ON calendar_2.ca_dt = DATE_TRUNC('DAY', CAST(devices.de_purchase_ts AS TIMESTAMP))
WHERE
  YEAR(CAST(calendar.ca_dt AS TIMESTAMP)) = 2024
GROUP BY
  1
ORDER BY
  2 DESC NULLS LAST,
  1 NULLS FIRST
LIMIT 1
