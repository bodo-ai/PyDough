SELECT
  seasons.s_name COLLATE utf8mb4_bin AS season_name,
  COUNT(*) AS n_events
FROM seasons AS seasons
JOIN events AS events
  ON seasons.s_month1 = EXTRACT(MONTH FROM CAST(events.ev_dt AS DATETIME))
  OR seasons.s_month2 = EXTRACT(MONTH FROM CAST(events.ev_dt AS DATETIME))
  OR seasons.s_month3 = EXTRACT(MONTH FROM CAST(events.ev_dt AS DATETIME))
GROUP BY
  1
ORDER BY
  2 DESC,
  1
