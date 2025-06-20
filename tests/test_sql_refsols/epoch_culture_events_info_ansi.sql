SELECT
  _s0.ev_name AS event_name,
  _s1.er_name AS era_name,
  EXTRACT(YEAR FROM _s0.ev_dt) AS event_year,
  _s5.s_name AS season_name,
  _s11.t_name AS tod
FROM events AS _s0
JOIN eras AS _s1
  ON _s1.er_end_year > EXTRACT(YEAR FROM _s0.ev_dt)
  AND _s1.er_start_year <= EXTRACT(YEAR FROM _s0.ev_dt)
JOIN events AS _s4
  ON _s0.ev_key = _s4.ev_key
JOIN seasons AS _s5
  ON _s5.s_month1 = EXTRACT(MONTH FROM _s4.ev_dt)
  OR _s5.s_month2 = EXTRACT(MONTH FROM _s4.ev_dt)
  OR _s5.s_month3 = EXTRACT(MONTH FROM _s4.ev_dt)
JOIN events AS _s10
  ON _s0.ev_key = _s10.ev_key
JOIN times AS _s11
  ON _s11.t_end_hour > EXTRACT(HOUR FROM _s10.ev_dt)
  AND _s11.t_start_hour <= EXTRACT(HOUR FROM _s10.ev_dt)
WHERE
  _s0.ev_typ = 'culture'
ORDER BY
  _s0.ev_dt
LIMIT 6
