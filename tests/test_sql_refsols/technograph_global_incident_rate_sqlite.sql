WITH _s0 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM main.incidents
), _s1 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM main.devices
)
SELECT
  ROUND(CAST(_s0.n_rows AS REAL) / _s1.n_rows, 2) AS ir
FROM _s0 AS _s0
CROSS JOIN _s1 AS _s1
