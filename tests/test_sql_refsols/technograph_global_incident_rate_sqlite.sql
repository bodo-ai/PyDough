WITH _s2 AS (
  SELECT
    COUNT(*) AS agg_0
  FROM main.incidents
), _s3 AS (
  SELECT
    COUNT(*) AS agg_1
  FROM main.devices
)
SELECT
  ROUND(CAST(_s2.agg_0 AS REAL) / _s3.agg_1, 2) AS ir
FROM _s2 AS _s2
CROSS JOIN _s3 AS _s3
