WITH _s1 AS (
  SELECT
    n_regionkey,
    COUNT(*) AS n_rows
  FROM tpch.nation
  WHERE
    SUBSTRING(n_name FROM 1 FOR 1) IN ('A', 'B', 'C')
  GROUP BY
    1
)
SELECT
  region.r_name AS region_name,
  'foo' AS x,
  COALESCE(_s1.n_rows, 0) AS n
FROM tpch.region AS region
LEFT JOIN _s1 AS _s1
  ON _s1.n_regionkey = region.r_regionkey
ORDER BY
  1 NULLS FIRST
