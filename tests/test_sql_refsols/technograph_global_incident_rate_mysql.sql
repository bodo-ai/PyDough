WITH _s0 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM main.INCIDENTS
), _s1 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM main.DEVICES
)
SELECT
  ROUND(_s0.n_rows / _s1.n_rows, 2) AS ir
FROM _s0 AS _s0
CROSS JOIN _s1 AS _s1
