SELECT
  events.ev_typ AS event_type,
  COUNT(*) AS n_events
FROM events AS events
JOIN seasons AS seasons
  ON (
    seasons.s_month1 = MONTH(CAST(events.ev_dt AS TIMESTAMP))
    OR seasons.s_month2 = MONTH(CAST(events.ev_dt AS TIMESTAMP))
    OR seasons.s_month3 = MONTH(CAST(events.ev_dt AS TIMESTAMP))
  )
  AND seasons.s_name = 'Summer'
GROUP BY
  1
ORDER BY
  1 NULLS FIRST
