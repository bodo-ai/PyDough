WITH _t0 AS (
  SELECT
    MAX(seasons.s_name) AS agg_2,
    COUNT() AS agg_0
  FROM seasons AS seasons
  JOIN events AS events
    ON seasons.s_month1 = CAST(STRFTIME('%m', events.ev_dt) AS INTEGER)
    OR seasons.s_month2 = CAST(STRFTIME('%m', events.ev_dt) AS INTEGER)
    OR seasons.s_month3 = CAST(STRFTIME('%m', events.ev_dt) AS INTEGER)
  GROUP BY
    seasons.s_name
)
SELECT
  agg_2 AS season_name,
  agg_0 AS n_events
FROM _t0
ORDER BY
  agg_0 DESC,
  agg_2
