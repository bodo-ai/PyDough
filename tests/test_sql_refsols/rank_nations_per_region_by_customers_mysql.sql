WITH _s1 AS (
  SELECT
    c_nationkey,
    COUNT(*) AS n_rows
  FROM tpch.CUSTOMER
  GROUP BY
    1
)
SELECT
  NATION.n_name AS nation_name,
  ROW_NUMBER() OVER (PARTITION BY NATION.n_regionkey ORDER BY CASE WHEN _s1.n_rows IS NULL THEN 1 ELSE 0 END DESC, _s1.n_rows DESC, CASE WHEN REGION.r_name COLLATE utf8mb4_bin IS NULL THEN 1 ELSE 0 END, REGION.r_name COLLATE utf8mb4_bin) AS `rank`
FROM tpch.NATION AS NATION
JOIN _s1 AS _s1
  ON NATION.n_nationkey = _s1.c_nationkey
JOIN tpch.REGION AS REGION
  ON NATION.n_regionkey = REGION.r_regionkey
ORDER BY
  2
LIMIT 5
