SELECT
  _s0.ev_typ AS event_type,
  COUNT(*) AS n_events
FROM events AS _s0
JOIN seasons AS _s1
  ON (
    _s1.s_month1 = CAST(STRFTIME('%m', _s0.ev_dt) AS INTEGER)
    OR _s1.s_month2 = CAST(STRFTIME('%m', _s0.ev_dt) AS INTEGER)
    OR _s1.s_month3 = CAST(STRFTIME('%m', _s0.ev_dt) AS INTEGER)
  )
  AND _s1.s_name = 'Summer'
GROUP BY
  _s0.ev_typ
ORDER BY
  event_type
