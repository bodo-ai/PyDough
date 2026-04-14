WITH _s0 AS (
  SELECT
    r_name
  FROM tpch.region
)
SELECT
  _s0.r_name AS r1,
  _s1.r_name AS r2
FROM _s0 AS _s0
CROSS JOIN _s0 AS _s1
ORDER BY
  1,
  2
