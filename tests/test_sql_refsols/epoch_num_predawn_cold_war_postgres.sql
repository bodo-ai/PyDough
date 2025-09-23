SELECT
  COUNT(*) AS n_events
FROM events AS events
JOIN times AS times
  ON times.t_end_hour > EXTRACT(HOUR FROM CAST(events.ev_dt AS TIMESTAMP))
  AND times.t_name = 'Pre-Dawn'
  AND times.t_start_hour <= EXTRACT(HOUR FROM CAST(events.ev_dt AS TIMESTAMP))
