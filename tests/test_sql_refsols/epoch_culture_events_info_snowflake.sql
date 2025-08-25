WITH _s2 AS (
  SELECT
    ev_dt,
    ev_key
  FROM events
)
SELECT
  events.ev_name AS event_name,
  eras.er_name AS era_name,
  YEAR(CAST(events.ev_dt AS TIMESTAMP)) AS event_year,
  seasons.s_name AS season_name,
  times.t_name AS tod
FROM events AS events
JOIN eras AS eras
  ON eras.er_end_year > YEAR(CAST(events.ev_dt AS TIMESTAMP))
  AND eras.er_start_year <= YEAR(CAST(events.ev_dt AS TIMESTAMP))
JOIN _s2 AS _s2
  ON _s2.ev_key = events.ev_key
JOIN seasons AS seasons
  ON seasons.s_month1 = MONTH(CAST(_s2.ev_dt AS TIMESTAMP))
  OR seasons.s_month2 = MONTH(CAST(_s2.ev_dt AS TIMESTAMP))
  OR seasons.s_month3 = MONTH(CAST(_s2.ev_dt AS TIMESTAMP))
JOIN _s2 AS _s6
  ON _s6.ev_key = events.ev_key
JOIN times AS times
  ON times.t_end_hour > HOUR(CAST(_s6.ev_dt AS TIMESTAMP))
  AND times.t_start_hour <= HOUR(CAST(_s6.ev_dt AS TIMESTAMP))
WHERE
  events.ev_typ = 'culture'
ORDER BY
  events.ev_dt NULLS FIRST
LIMIT 6
