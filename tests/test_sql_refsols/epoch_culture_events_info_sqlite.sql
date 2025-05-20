WITH _s2 AS (
  SELECT
    ev_dt AS date_time,
    ev_key AS key
  FROM events
), _t0 AS (
  SELECT
    events.ev_dt AS date_time,
    eras.er_name AS era_name,
    events.ev_name AS event_name,
    CAST(STRFTIME('%Y', events.ev_dt) AS INTEGER) AS event_year,
    seasons.s_name AS season_name,
    times.t_name AS tod
  FROM events AS events
  JOIN eras AS eras
    ON eras.er_end_year > CAST(STRFTIME('%Y', events.ev_dt) AS INTEGER)
    AND eras.er_start_year <= CAST(STRFTIME('%Y', events.ev_dt) AS INTEGER)
  JOIN _s2 AS _s2
    ON _s2.key = events.ev_key
  JOIN seasons AS seasons
    ON seasons.s_month1 = CAST(STRFTIME('%m', _s2.date_time) AS INTEGER)
    OR seasons.s_month2 = CAST(STRFTIME('%m', _s2.date_time) AS INTEGER)
    OR seasons.s_month3 = CAST(STRFTIME('%m', _s2.date_time) AS INTEGER)
  JOIN _s2 AS _s6
    ON _s6.key = events.ev_key
  JOIN times AS times
    ON times.t_end_hour > CAST(STRFTIME('%H', _s6.date_time) AS INTEGER)
    AND times.t_start_hour <= CAST(STRFTIME('%H', _s6.date_time) AS INTEGER)
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
