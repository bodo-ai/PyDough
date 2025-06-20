SELECT
  _s0.ev_typ AS event_type,
  COUNT(*) AS n_events
FROM events AS _s0
JOIN seasons AS _s1
  ON (
    _s1.s_month1 = EXTRACT(MONTH FROM _s0.ev_dt)
    OR _s1.s_month2 = EXTRACT(MONTH FROM _s0.ev_dt)
    OR _s1.s_month3 = EXTRACT(MONTH FROM _s0.ev_dt)
  )
  AND _s1.s_name = 'Summer'
GROUP BY
  _s0.ev_typ
ORDER BY
  event_type
