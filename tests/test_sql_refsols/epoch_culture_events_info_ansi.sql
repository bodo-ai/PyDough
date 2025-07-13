WITH _s2 AS (
  SELECT
    ev_dt,
    ev_key
  FROM events
), _t0 AS (
  SELECT
    eras.er_name,
    events.ev_dt,
    events.ev_name,
    seasons.s_name,
    times.t_name
  FROM events AS events
  JOIN eras AS eras
    ON eras.er_end_year > EXTRACT(YEAR FROM CAST(events.ev_dt AS DATETIME))
    AND eras.er_start_year <= EXTRACT(YEAR FROM CAST(events.ev_dt AS DATETIME))
  JOIN _s2 AS _s2
    ON _s2.ev_key = events.ev_key
  JOIN seasons AS seasons
    ON seasons.s_month1 = EXTRACT(MONTH FROM CAST(_s2.ev_dt AS DATETIME))
    OR seasons.s_month2 = EXTRACT(MONTH FROM CAST(_s2.ev_dt AS DATETIME))
    OR seasons.s_month3 = EXTRACT(MONTH FROM CAST(_s2.ev_dt AS DATETIME))
  JOIN _s2 AS _s6
    ON _s6.ev_key = events.ev_key
  JOIN times AS times
    ON times.t_end_hour > EXTRACT(HOUR FROM CAST(_s6.ev_dt AS DATETIME))
    AND times.t_start_hour <= EXTRACT(HOUR FROM CAST(_s6.ev_dt AS DATETIME))
  WHERE
    events.ev_typ = 'culture'
  ORDER BY
    ev_dt
  LIMIT 6
)
SELECT
  ev_name AS event_name,
  er_name AS era_name,
  EXTRACT(YEAR FROM CAST(ev_dt AS DATETIME)) AS event_year,
  s_name AS season_name,
  t_name AS tod
FROM _t0
ORDER BY
  ev_dt
