SELECT
  events.ev_typ AS event_type,
  COUNT(*) AS n_events
FROM events AS events
JOIN seasons AS seasons
  ON (
    seasons.s_month1 = EXTRACT(MONTH FROM CAST(events.ev_dt AS TIMESTAMP))
    OR seasons.s_month2 = EXTRACT(MONTH FROM CAST(events.ev_dt AS TIMESTAMP))
    OR seasons.s_month3 = EXTRACT(MONTH FROM CAST(events.ev_dt AS TIMESTAMP))
  )
  AND seasons.s_name = 'Summer'
GROUP BY
  events.ev_typ
ORDER BY
  events.ev_typ NULLS FIRST
