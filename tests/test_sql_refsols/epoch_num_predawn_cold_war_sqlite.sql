SELECT
  COUNT(*) AS n_events
FROM events AS _s0
JOIN times AS _s1
  ON _s1.t_end_hour > CAST(STRFTIME('%H', _s0.ev_dt) AS INTEGER)
  AND _s1.t_name = 'Pre-Dawn'
  AND _s1.t_start_hour <= CAST(STRFTIME('%H', _s0.ev_dt) AS INTEGER)
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM events AS _s4
    JOIN eras AS _s5
      ON _s5.er_end_year > CAST(STRFTIME('%Y', _s4.ev_dt) AS INTEGER)
      AND _s5.er_name = 'Cold War'
      AND _s5.er_start_year <= CAST(STRFTIME('%Y', _s4.ev_dt) AS INTEGER)
    WHERE
      _s0.ev_key = _s4.ev_key
  )
