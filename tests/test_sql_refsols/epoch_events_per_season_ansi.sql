WITH _t0 AS (
  SELECT
    ANY_VALUE(seasons.s_name) AS agg_2,
    COUNT() AS agg_0
  FROM seasons AS seasons
  JOIN events AS events
    ON seasons.s_month1 = EXTRACT(MONTH FROM events.ev_dt)
    OR seasons.s_month2 = EXTRACT(MONTH FROM events.ev_dt)
    OR seasons.s_month3 = EXTRACT(MONTH FROM events.ev_dt)
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
