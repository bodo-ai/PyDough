WITH _s0 AS (
  SELECT
    COUNT() AS agg_0
  FROM main.incidents
), _s1 AS (
  SELECT
    COUNT() AS agg_1
  FROM main.devices
)
SELECT
  ROUND(CAST(_s0.agg_0 AS REAL) / _s1.agg_1, 2) AS ir
FROM _s0 AS _s0
LEFT JOIN _s1 AS _s1
  ON TRUE
