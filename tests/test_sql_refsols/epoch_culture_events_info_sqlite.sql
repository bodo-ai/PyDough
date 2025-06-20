SELECT
  _s0.ev_name AS event_name,
  _s1.er_name AS era_name,
  CAST(STRFTIME('%Y', _s0.ev_dt) AS INTEGER) AS event_year,
  _s5.s_name AS season_name,
  _s11.t_name AS tod
FROM events AS _s0
JOIN eras AS _s1
  ON _s1.er_end_year > CAST(STRFTIME('%Y', _s0.ev_dt) AS INTEGER)
  AND _s1.er_start_year <= CAST(STRFTIME('%Y', _s0.ev_dt) AS INTEGER)
JOIN events AS _s4
  ON _s0.ev_key = _s4.ev_key
JOIN seasons AS _s5
  ON _s5.s_month1 = CAST(STRFTIME('%m', _s4.ev_dt) AS INTEGER)
  OR _s5.s_month2 = CAST(STRFTIME('%m', _s4.ev_dt) AS INTEGER)
  OR _s5.s_month3 = CAST(STRFTIME('%m', _s4.ev_dt) AS INTEGER)
JOIN events AS _s10
  ON _s0.ev_key = _s10.ev_key
JOIN times AS _s11
  ON _s11.t_end_hour > CAST(STRFTIME('%H', _s10.ev_dt) AS INTEGER)
  AND _s11.t_start_hour <= CAST(STRFTIME('%H', _s10.ev_dt) AS INTEGER)
WHERE
  _s0.ev_typ = 'culture'
ORDER BY
  _s0.ev_dt
LIMIT 6
