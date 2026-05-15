WITH _s1 AS (
  SELECT
    n_regionkey,
    COUNT(*) AS n_rows
  FROM tpch.NATION
  WHERE
    SUBSTRING(n_name, 1, 1) IN ('A', 'B', 'C')
  GROUP BY
    1
)
SELECT
  REGION.r_name COLLATE utf8mb4_bin AS region_name,
  'foo' AS x,
  COALESCE(_s1.n_rows, 0) AS n
FROM tpch.REGION AS REGION
LEFT JOIN _s1 AS _s1
  ON REGION.r_regionkey = _s1.n_regionkey
ORDER BY
  1
