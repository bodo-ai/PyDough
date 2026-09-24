WITH _s5 AS (
  SELECT
    nation.n_regionkey,
    COUNT(*) AS n_rows
  FROM tpch.nation AS nation
  CROSS JOIN (VALUES
    (NULL)) AS _q_1(_col_0)
  WHERE
    SUBSTRING(nation.n_name, 1, 1) IN ('A', 'B', 'C')
  GROUP BY
    1
)
SELECT
  region.r_name AS region_name,
  'foo' AS x,
  COALESCE(_s5.n_rows, 0) AS n
FROM tpch.region AS region
CROSS JOIN (VALUES
  (NULL)) AS _q_0(_col_0)
LEFT JOIN _s5 AS _s5
  ON _s5.n_regionkey = region.r_regionkey
ORDER BY
  1
