WITH _s2 AS (
  SELECT
    ev_dt,
    ev_key
  FROM EVENTS
)
SELECT
  EVENTS.ev_name AS event_name,
  ERAS.er_name AS era_name,
  YEAR(EVENTS.ev_dt) AS event_year,
  SEASONS.s_name AS season_name,
  TIMES.t_name AS tod
FROM EVENTS AS EVENTS
JOIN ERAS AS ERAS
  ON ERAS.er_end_year > YEAR(EVENTS.ev_dt)
  AND ERAS.er_start_year <= YEAR(EVENTS.ev_dt)
JOIN _s2 AS _s2
  ON EVENTS.ev_key = _s2.ev_key
JOIN SEASONS AS SEASONS
  ON SEASONS.s_month1 = MONTH(_s2.ev_dt)
  OR SEASONS.s_month2 = MONTH(_s2.ev_dt)
  OR SEASONS.s_month3 = MONTH(_s2.ev_dt)
JOIN _s2 AS _s6
  ON EVENTS.ev_key = _s6.ev_key
JOIN TIMES AS TIMES
  ON TIMES.t_end_hour > HOUR(_s6.ev_dt) AND TIMES.t_start_hour <= HOUR(_s6.ev_dt)
WHERE
  EVENTS.ev_typ = 'culture'
ORDER BY
  EVENTS.ev_dt
LIMIT 6
