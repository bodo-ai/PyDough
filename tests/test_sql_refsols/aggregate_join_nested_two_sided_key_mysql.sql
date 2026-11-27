WITH _s2 AS (
  SELECT
    c_nationkey,
    COUNT(*) AS n_rows
  FROM tpch.CUSTOMER
  GROUP BY
    1
)
SELECT
  CONCAT_WS(' - ', NATION.n_name, REGION.r_name) COLLATE utf8mb4_bin AS label,
  SUM(_s2.n_rows) AS n_customers
FROM _s2 AS _s2
JOIN tpch.NATION AS NATION
  ON NATION.n_nationkey = _s2.c_nationkey
JOIN tpch.REGION AS REGION
  ON NATION.n_regionkey = REGION.r_regionkey
GROUP BY
  1
ORDER BY
  1
