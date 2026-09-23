WITH _s5 AS (
  SELECT
    NATION.n_regionkey,
    COUNT(*) AS n_rows
  FROM tpch.NATION AS NATION
  CROSS JOIN (VALUES
    ROW(NULL)) AS _q_1(_col_0)
  WHERE
    SUBSTRING(NATION.n_name, 1, 1) IN ('A', 'B', 'C')
  GROUP BY
    1
)
SELECT
  REGION.r_name COLLATE utf8mb4_bin AS region_name,
  'foo' AS x,
  COALESCE(_s5.n_rows, 0) AS n
FROM tpch.REGION AS REGION
CROSS JOIN (VALUES
  ROW(NULL)) AS _q_0(_col_0)
LEFT JOIN _s5 AS _s5
  ON REGION.r_regionkey = _s5.n_regionkey
ORDER BY
  1
