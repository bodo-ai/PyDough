WITH _s0 AS (
  SELECT
    c_mktsegment,
    c_nationkey,
    COUNT(*) AS n_rows
  FROM tpch.CUSTOMER
  GROUP BY
    1,
    2
)
SELECT
  _s0.c_mktsegment COLLATE utf8mb4_bin AS market_segment,
  NATION.n_name COLLATE utf8mb4_bin AS nation_name,
  SUM(_s0.n_rows) AS n
FROM _s0 AS _s0
JOIN tpch.NATION AS NATION
  ON NATION.n_nationkey = _s0.c_nationkey
GROUP BY
  1,
  2
ORDER BY
  1,
  2
