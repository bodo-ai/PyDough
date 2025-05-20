SELECT
  events.ev_typ AS event_type,
  COUNT() AS n_events
FROM seasons AS seasons
JOIN events AS events
  ON seasons.s_month1 = EXTRACT(MONTH FROM events.ev_dt)
  OR seasons.s_month2 = EXTRACT(MONTH FROM events.ev_dt)
  OR seasons.s_month3 = EXTRACT(MONTH FROM events.ev_dt)
WHERE
  seasons.s_name = 'Summer'
GROUP BY
  events.ev_typ
ORDER BY
  event_type
