WITH _s0 AS (
  SELECT
    events.ev_dt AS date_time,
    events.ev_key AS key
  FROM events AS events
), _u_0 AS (
  SELECT
    _s2.key AS _u_1
  FROM _s0 AS _s2
  JOIN eras AS eras
    ON eras.er_end_year > CAST(STRFTIME('%Y', _s2.date_time) AS INTEGER)
    AND eras.er_name = 'Cold War'
    AND eras.er_start_year <= CAST(STRFTIME('%Y', _s2.date_time) AS INTEGER)
  GROUP BY
    _s2.key
)
SELECT
  COUNT(*) AS n_events
FROM _s0 AS _s0
JOIN times AS times
  ON times.t_end_hour > CAST(STRFTIME('%H', _s0.date_time) AS INTEGER)
  AND times.t_name = 'Pre-Dawn'
  AND times.t_start_hour <= CAST(STRFTIME('%H', _s0.date_time) AS INTEGER)
LEFT JOIN _u_0 AS _u_0
  ON _s0.key = _u_0._u_1
WHERE
  NOT _u_0._u_1 IS NULL
