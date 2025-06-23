WITH _t0 AS (
  SELECT
    ANY_VALUE(seasons.s_name) AS anything_s_name,
    COUNT(*) AS n_events
  FROM seasons AS seasons
  JOIN events AS events
    ON seasons.s_month1 = EXTRACT(MONTH FROM CAST(events.ev_dt AS DATETIME))
    OR seasons.s_month2 = EXTRACT(MONTH FROM CAST(events.ev_dt AS DATETIME))
    OR seasons.s_month3 = EXTRACT(MONTH FROM CAST(events.ev_dt AS DATETIME))
  GROUP BY
    seasons.s_name
)
SELECT
  anything_s_name AS season_name,
  n_events
FROM _t0
ORDER BY
  n_events DESC,
  anything_s_name
