SELECT
  events.ev_typ AS event_type,
  COUNT(*) AS n_events
FROM events AS events
JOIN seasons AS seasons
  ON (
    seasons.s_month1 = CAST(STRFTIME('%m', events.ev_dt) AS INTEGER)
    OR seasons.s_month2 = CAST(STRFTIME('%m', events.ev_dt) AS INTEGER)
    OR seasons.s_month3 = CAST(STRFTIME('%m', events.ev_dt) AS INTEGER)
  )
  AND seasons.s_name = 'Summer'
GROUP BY
  events.ev_typ
ORDER BY
  event_type
