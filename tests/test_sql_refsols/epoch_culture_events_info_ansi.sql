WITH _t0 AS (
  SELECT
    events.ev_dt AS date_time,
    eras.er_name AS era_name,
    events.ev_name AS event_name,
    EXTRACT(YEAR FROM events.ev_dt) AS event_year,
    seasons.s_name AS season_name,
    times.t_name AS tod
  FROM events AS events
  JOIN eras AS eras
    ON eras.er_end_year > EXTRACT(YEAR FROM events.ev_dt)
    AND eras.er_start_year <= EXTRACT(YEAR FROM events.ev_dt)
  JOIN seasons AS seasons
    ON seasons.s_month1 = EXTRACT(MONTH FROM events.ev_dt)
    OR seasons.s_month2 = EXTRACT(MONTH FROM events.ev_dt)
    OR seasons.s_month3 = EXTRACT(MONTH FROM events.ev_dt)
  JOIN times AS times
    ON times.t_end_hour > EXTRACT(HOUR FROM events.ev_dt)
    AND times.t_start_hour <= EXTRACT(HOUR FROM events.ev_dt)
  WHERE
    events.ev_typ = 'culture'
  ORDER BY
    date_time
  LIMIT 6
)
SELECT
  event_name,
  era_name,
  event_year,
  season_name,
  tod
FROM _t0
ORDER BY
  date_time
