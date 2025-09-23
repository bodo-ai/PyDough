SELECT
  COUNT(*) AS n_events
FROM events AS events
JOIN times AS times
  ON times.t_end_hour > CAST(STRFTIME('%H', events.ev_dt) AS INTEGER)
  AND times.t_name = 'Pre-Dawn'
  AND times.t_start_hour <= CAST(STRFTIME('%H', events.ev_dt) AS INTEGER)
