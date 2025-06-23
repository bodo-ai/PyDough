WITH _t0 AS (
  SELECT
    COUNT(*) AS n_events,
    ANY_VALUE(seasons.s_name) AS season_name
  FROM seasons AS seasons
  JOIN events AS events
    ON seasons.s_month1 = EXTRACT(MONTH FROM CAST(events.ev_dt AS DATETIME))
    OR seasons.s_month2 = EXTRACT(MONTH FROM CAST(events.ev_dt AS DATETIME))
    OR seasons.s_month3 = EXTRACT(MONTH FROM CAST(events.ev_dt AS DATETIME))
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
