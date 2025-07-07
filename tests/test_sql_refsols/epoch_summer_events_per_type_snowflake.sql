SELECT
  EVENTS.ev_typ AS event_type,
  COUNT(*) AS n_events
FROM EVENTS AS EVENTS
JOIN SEASONS AS SEASONS
  ON (
    SEASONS.s_month1 = MONTH(EVENTS.ev_dt)
    OR SEASONS.s_month2 = MONTH(EVENTS.ev_dt)
    OR SEASONS.s_month3 = MONTH(EVENTS.ev_dt)
  )
  AND SEASONS.s_name = 'Summer'
GROUP BY
  EVENTS.ev_typ
ORDER BY
  EVENTS.ev_typ NULLS FIRST
