WITH _s0 AS (
  SELECT
    events.ev_dt AS date_time,
    events.ev_key AS key
  FROM events AS events
)
SELECT
  COUNT(*) AS n_events
FROM _s0 AS _s0
JOIN times AS times
  ON times.t_end_hour > EXTRACT(HOUR FROM CAST(_s0.date_time AS DATETIME))
  AND times.t_name = 'Pre-Dawn'
  AND times.t_start_hour <= EXTRACT(HOUR FROM CAST(_s0.date_time AS DATETIME))
JOIN _s0 AS _s2
  ON _s0.key = _s2.key
JOIN eras AS eras
  ON eras.er_end_year > EXTRACT(YEAR FROM CAST(_s2.date_time AS DATETIME))
  AND eras.er_name = 'Cold War'
  AND eras.er_start_year <= EXTRACT(YEAR FROM CAST(_s2.date_time AS DATETIME))
