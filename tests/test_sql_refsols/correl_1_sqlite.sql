WITH _s1 AS (
  SELECT
    SUBSTRING(n_name, 1, 1) AS expr_1,
    COUNT(*) AS n_rows,
    n_regionkey
  FROM tpch.nation
  GROUP BY
    SUBSTRING(n_name, 1, 1),
    n_regionkey
)
SELECT
  region.r_name AS region_name,
  COALESCE(_s1.n_rows, 0) AS n_prefix_nations
FROM tpch.region AS region
LEFT JOIN _s1 AS _s1
  ON _s1.expr_1 = SUBSTRING(region.r_name, 1, 1)
  AND _s1.n_regionkey = region.r_regionkey
ORDER BY
  region.r_name
