WITH _s1 AS (
  SELECT
    SUBSTRING(n_name, 1, 1) AS expr_1,
    n_regionkey,
    COUNT(*) AS n_rows
  FROM tpch.nation
  GROUP BY
    1,
    2
)
SELECT
  region.r_name AS name,
  _s1.n_rows AS n_prefix_nations
FROM tpch.region AS region
JOIN _s1 AS _s1
  ON _s1.expr_1 = SUBSTRING(region.r_name, 1, 1)
  AND _s1.n_regionkey = region.r_regionkey
