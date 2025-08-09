WITH _s2 AS (
  SELECT
    ev_dt,
    ev_key
  FROM EVENTS
)
SELECT
  EVENTS.ev_name AS event_name,
  ERAS.er_name AS era_name,
  EXTRACT(YEAR FROM CAST(EVENTS.ev_dt AS DATETIME)) AS event_year,
  SEASONS.s_name AS season_name,
  TIMES.t_name AS tod
FROM EVENTS AS EVENTS
JOIN ERAS AS ERAS
  ON ERAS.er_end_year > EXTRACT(YEAR FROM CAST(EVENTS.ev_dt AS DATETIME))
  AND ERAS.er_start_year <= EXTRACT(YEAR FROM CAST(EVENTS.ev_dt AS DATETIME))
JOIN _s2 AS _s2
  ON EVENTS.ev_key = _s2.ev_key
JOIN SEASONS AS SEASONS
  ON SEASONS.s_month1 = EXTRACT(MONTH FROM CAST(_s2.ev_dt AS DATETIME))
  OR SEASONS.s_month2 = EXTRACT(MONTH FROM CAST(_s2.ev_dt AS DATETIME))
  OR SEASONS.s_month3 = EXTRACT(MONTH FROM CAST(_s2.ev_dt AS DATETIME))
JOIN _s2 AS _s6
  ON EVENTS.ev_key = _s6.ev_key
JOIN TIMES AS TIMES
  ON TIMES.t_end_hour > HOUR(_s6.ev_dt) AND TIMES.t_start_hour <= HOUR(_s6.ev_dt)
WHERE
  EVENTS.ev_typ = 'culture'
ORDER BY
  EVENTS.ev_dt
LIMIT 6
