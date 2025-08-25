SELECT
  events.ev_typ COLLATE utf8mb4_bin AS event_type,
  COUNT(*) AS n_events
FROM events AS events
JOIN seasons AS seasons
  ON (
    seasons.s_month1 = EXTRACT(MONTH FROM CAST(events.ev_dt AS DATETIME))
    OR seasons.s_month2 = EXTRACT(MONTH FROM CAST(events.ev_dt AS DATETIME))
    OR seasons.s_month3 = EXTRACT(MONTH FROM CAST(events.ev_dt AS DATETIME))
  )
  AND seasons.s_name = 'Summer'
GROUP BY
  1
ORDER BY
  1
