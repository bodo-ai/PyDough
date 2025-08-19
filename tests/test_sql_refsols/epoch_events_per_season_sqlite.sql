SELECT
  seasons.s_name AS season_name,
  COUNT(*) AS n_events
FROM seasons AS seasons
JOIN events AS events
  ON seasons.s_month1 = CAST(STRFTIME('%m', events.ev_dt) AS INTEGER)
  OR seasons.s_month2 = CAST(STRFTIME('%m', events.ev_dt) AS INTEGER)
  OR seasons.s_month3 = CAST(STRFTIME('%m', events.ev_dt) AS INTEGER)
GROUP BY
  1
ORDER BY
  n_events DESC,
  seasons.s_name
