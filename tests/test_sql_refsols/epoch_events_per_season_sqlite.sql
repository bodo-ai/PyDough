WITH _t0 AS (
  SELECT
    COUNT(*) AS n_events,
    MAX(_s0.s_name) AS season_name
  FROM seasons AS _s0
  JOIN events AS _s1
    ON _s0.s_month1 = CAST(STRFTIME('%m', _s1.ev_dt) AS INTEGER)
    OR _s0.s_month2 = CAST(STRFTIME('%m', _s1.ev_dt) AS INTEGER)
    OR _s0.s_month3 = CAST(STRFTIME('%m', _s1.ev_dt) AS INTEGER)
  GROUP BY
    _s0.s_name
)
SELECT
  season_name,
  n_events
FROM _t0
ORDER BY
  n_events DESC,
  season_name
