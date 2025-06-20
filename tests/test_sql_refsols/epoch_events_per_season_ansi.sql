WITH _t0 AS (
  SELECT
    COUNT(*) AS n_events,
    ANY_VALUE(_s0.s_name) AS season_name
  FROM seasons AS _s0
  JOIN events AS _s1
    ON _s0.s_month1 = EXTRACT(MONTH FROM _s1.ev_dt)
    OR _s0.s_month2 = EXTRACT(MONTH FROM _s1.ev_dt)
    OR _s0.s_month3 = EXTRACT(MONTH FROM _s1.ev_dt)
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
