SELECT
  events.ev_typ AS event_type,
  COUNT() AS n_events
FROM seasons AS seasons
JOIN events AS events
  ON seasons.s_month1 = CAST(STRFTIME('%m', events.ev_dt) AS INTEGER)
  OR seasons.s_month2 = CAST(STRFTIME('%m', events.ev_dt) AS INTEGER)
  OR seasons.s_month3 = CAST(STRFTIME('%m', events.ev_dt) AS INTEGER)
WHERE
  seasons.s_name = 'Summer'
GROUP BY
  events.ev_typ
ORDER BY
  event_type
