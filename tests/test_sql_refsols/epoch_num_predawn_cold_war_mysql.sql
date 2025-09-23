SELECT
  COUNT(*) AS n_events
FROM EVENTS AS EVENTS
JOIN TIMES AS TIMES
  ON TIMES.t_end_hour > HOUR(EVENTS.ev_dt)
  AND TIMES.t_name = 'Pre-Dawn'
  AND TIMES.t_start_hour <= HOUR(EVENTS.ev_dt)
