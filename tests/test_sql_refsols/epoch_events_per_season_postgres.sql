SELECT
  MAX(seasons.s_name) AS season_name,
  COUNT(*) AS n_events
FROM seasons AS seasons
JOIN events AS events
  ON seasons.s_month1 = EXTRACT(MONTH FROM CAST(events.ev_dt AS TIMESTAMP))
  OR seasons.s_month2 = EXTRACT(MONTH FROM CAST(events.ev_dt AS TIMESTAMP))
  OR seasons.s_month3 = EXTRACT(MONTH FROM CAST(events.ev_dt AS TIMESTAMP))
GROUP BY
  seasons.s_name
ORDER BY
  n_events DESC NULLS LAST,
  season_name NULLS FIRST
