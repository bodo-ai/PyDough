SELECT
  seasons.s_name AS season_name,
  COUNT(*) AS n_events
FROM seasons AS seasons
JOIN events AS events
  ON seasons.s_month1 = MONTH(CAST(events.ev_dt AS TIMESTAMP))
  OR seasons.s_month2 = MONTH(CAST(events.ev_dt AS TIMESTAMP))
  OR seasons.s_month3 = MONTH(CAST(events.ev_dt AS TIMESTAMP))
GROUP BY
  1
ORDER BY
  2 DESC NULLS LAST,
  1 NULLS FIRST
