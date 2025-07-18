SELECT
  MAX(calendar.ca_dt) AS start_of_period,
  COUNT(*) AS n_purchases
FROM main.calendar AS calendar
JOIN main.calendar AS calendar_2
  ON calendar.ca_dt <= calendar_2.ca_dt
  AND calendar_2.ca_dt < DATETIME(calendar.ca_dt, '5 day')
JOIN main.devices AS devices
  ON calendar_2.ca_dt = DATE(devices.de_purchase_ts, 'start of day')
WHERE
  CAST(STRFTIME('%Y', calendar.ca_dt) AS INTEGER) = 2024
GROUP BY
  calendar.ca_dt
ORDER BY
  n_purchases DESC,
  start_of_period
LIMIT 1
