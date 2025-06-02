SELECT
  events.ev_typ AS event_type,
  COUNT() AS n_events
FROM events AS events
JOIN seasons AS seasons
  ON (
    seasons.s_month1 = EXTRACT(MONTH FROM events.ev_dt)
    OR seasons.s_month2 = EXTRACT(MONTH FROM events.ev_dt)
    OR seasons.s_month3 = EXTRACT(MONTH FROM events.ev_dt)
  )
  AND seasons.s_name = 'Summer'
GROUP BY
  events.ev_typ
ORDER BY
  event_type
