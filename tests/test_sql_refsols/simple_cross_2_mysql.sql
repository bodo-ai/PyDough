WITH _s0 AS (
  SELECT
    r_name
  FROM tpch.REGION
)
SELECT
  _s0.r_name COLLATE utf8mb4_bin AS r1,
  _s1.r_name COLLATE utf8mb4_bin AS r2
FROM _s0 AS _s0
JOIN _s0 AS _s1
  ON _s0.r_name <> _s1.r_name
ORDER BY
  1,
  2
