WITH _s2 AS (
  SELECT
    ev_dt AS date_time,
    ev_key AS key
  FROM events
), _s3 AS (
  SELECT
    er_end_year AS end_year,
    er_start_year AS start_year
  FROM eras
), _t0 AS (
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
  JOIN _s2 AS _s2
    ON _s2.key = events.ev_key
  JOIN _s3 AS _s3
    ON _s3.end_year > EXTRACT(YEAR FROM _s2.date_time)
    AND _s3.start_year <= EXTRACT(YEAR FROM _s2.date_time)
  JOIN seasons AS seasons
    ON seasons.s_month1 = EXTRACT(MONTH FROM _s2.date_time)
    OR seasons.s_month2 = EXTRACT(MONTH FROM _s2.date_time)
    OR seasons.s_month3 = EXTRACT(MONTH FROM _s2.date_time)
  JOIN _s2 AS _s8
    ON _s8.key = events.ev_key
  JOIN _s3 AS _s9
    ON _s9.end_year > EXTRACT(YEAR FROM _s8.date_time)
    AND _s9.start_year <= EXTRACT(YEAR FROM _s8.date_time)
  JOIN times AS times
    ON times.t_end_hour > EXTRACT(HOUR FROM _s8.date_time)
    AND times.t_start_hour <= EXTRACT(HOUR FROM _s8.date_time)
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
