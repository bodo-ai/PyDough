WITH _t1 AS (
  SELECT
    _s0.er_name AS name,
    _s1.ev_name AS name_1,
    _s0.er_start_year AS start_year
  FROM eras AS _s0
  JOIN events AS _s1
    ON _s0.er_end_year > EXTRACT(YEAR FROM _s1.ev_dt)
    AND _s0.er_start_year <= EXTRACT(YEAR FROM _s1.ev_dt)
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY _s0.er_name ORDER BY _s1.ev_dt NULLS LAST) = 1
)
SELECT
  name AS era_name,
  name_1 AS event_name
FROM _t1
ORDER BY
  start_year
