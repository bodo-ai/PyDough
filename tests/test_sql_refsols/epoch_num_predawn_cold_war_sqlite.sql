WITH _s0 AS (
  SELECT
    ev_dt,
    ev_key
  FROM events
), _u_0 AS (
  SELECT
    _s2.ev_key AS _u_1
  FROM _s0 AS _s2
  JOIN eras AS eras
    ON eras.er_end_year > CAST(STRFTIME('%Y', _s2.ev_dt) AS INTEGER)
    AND eras.er_name = 'Cold War'
    AND eras.er_start_year <= CAST(STRFTIME('%Y', _s2.ev_dt) AS INTEGER)
  GROUP BY
    1
)
SELECT
  COUNT(DISTINCT _s0.ev_key) AS n_events
FROM _s0 AS _s0
JOIN times AS times
  ON times.t_end_hour > CAST(STRFTIME('%H', _s0.ev_dt) AS INTEGER)
  AND times.t_name = 'Pre-Dawn'
  AND times.t_start_hour <= CAST(STRFTIME('%H', _s0.ev_dt) AS INTEGER)
LEFT JOIN _u_0 AS _u_0
  ON _s0.ev_key = _u_0._u_1
WHERE
  NOT _u_0._u_1 IS NULL
