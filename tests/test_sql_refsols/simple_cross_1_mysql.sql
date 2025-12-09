WITH _s0 AS (
  SELECT
    r_name
  FROM tpch.REGION
)
SELECT
  r1 COLLATE utf8mb4_bin AS r1,
  _s1.r_name COLLATE utf8mb4_bin AS r2
FROM _s0 AS _s0
CROSS JOIN _s0 AS _s1
ORDER BY
  1,
  2
