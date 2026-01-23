WITH _s0 AS (
  SELECT
    r_name,
    r_regionkey
  FROM tpch.REGION
), _s1 AS (
  SELECT
    n_name,
    n_regionkey
  FROM tpch.NATION
)
SELECT
  _s1.n_name COLLATE utf8mb4_bin AS n1,
  _s5.n_name COLLATE utf8mb4_bin AS n2
FROM _s0 AS _s0
JOIN _s1 AS _s1
  ON _s0.r_regionkey = _s1.n_regionkey
JOIN _s0 AS _s3
  ON _s0.r_name = _s3.r_name
JOIN _s1 AS _s5
  ON _s1.n_name <> _s5.n_name AND _s3.r_regionkey = _s5.n_regionkey
ORDER BY
  1,
  2
LIMIT 10
