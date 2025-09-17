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
    ON eras.er_end_year > EXTRACT(YEAR FROM CAST(_s2.ev_dt AS TIMESTAMP))
    AND eras.er_name = 'Cold War'
    AND eras.er_start_year <= EXTRACT(YEAR FROM CAST(_s2.ev_dt AS TIMESTAMP))
  GROUP BY
    1
)
SELECT
  COUNT(*) AS n_events
FROM _s0 AS _s0
JOIN times AS times
  ON times.t_end_hour > EXTRACT(HOUR FROM CAST(_s0.ev_dt AS TIMESTAMP))
  AND times.t_name = 'Pre-Dawn'
  AND times.t_start_hour <= EXTRACT(HOUR FROM CAST(_s0.ev_dt AS TIMESTAMP))
LEFT JOIN _u_0 AS _u_0
  ON _s0.ev_key = _u_0._u_1
WHERE
  NOT _u_0._u_1 IS NULL
