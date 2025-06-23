WITH _t0 AS (
  SELECT
    COUNT(*) AS n_events,
    events.ev_typ
  FROM events AS events
  JOIN seasons AS seasons
    ON (
      seasons.s_month1 = EXTRACT(MONTH FROM CAST(events.ev_dt AS DATETIME))
      OR seasons.s_month2 = EXTRACT(MONTH FROM CAST(events.ev_dt AS DATETIME))
      OR seasons.s_month3 = EXTRACT(MONTH FROM CAST(events.ev_dt AS DATETIME))
    )
    AND seasons.s_name = 'Summer'
  GROUP BY
    events.ev_typ
)
SELECT
  ev_typ AS event_type,
  n_events
FROM _t0
ORDER BY
  ev_typ
