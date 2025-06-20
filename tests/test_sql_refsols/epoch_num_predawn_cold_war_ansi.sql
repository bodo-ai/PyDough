SELECT
  COUNT(*) AS n_events
FROM events AS _s0
JOIN times AS _s1
  ON _s1.t_end_hour > EXTRACT(HOUR FROM _s0.ev_dt)
  AND _s1.t_name = 'Pre-Dawn'
  AND _s1.t_start_hour <= EXTRACT(HOUR FROM _s0.ev_dt)
JOIN events AS _s4
  ON _s0.ev_key = _s4.ev_key
JOIN eras AS _s5
  ON _s5.er_end_year > EXTRACT(YEAR FROM _s4.ev_dt)
  AND _s5.er_name = 'Cold War'
  AND _s5.er_start_year <= EXTRACT(YEAR FROM _s4.ev_dt)
