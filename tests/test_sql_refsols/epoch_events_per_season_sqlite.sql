WITH _t0 AS (
  SELECT
    COUNT(*) AS n_events,
    MAX(seasons.s_name) AS season_name
  FROM seasons AS seasons
  JOIN events AS events
    ON seasons.s_month1 = CAST(STRFTIME('%m', events.ev_dt) AS INTEGER)
    OR seasons.s_month2 = CAST(STRFTIME('%m', events.ev_dt) AS INTEGER)
    OR seasons.s_month3 = CAST(STRFTIME('%m', events.ev_dt) AS INTEGER)
  GROUP BY
    seasons.s_name
)
SELECT
  season_name,
  n_events
FROM _t0
ORDER BY
  n_events DESC,
  season_name
