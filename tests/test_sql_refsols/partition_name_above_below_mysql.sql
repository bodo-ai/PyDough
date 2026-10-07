WITH _s0 AS (
  SELECT
    c_mktsegment,
    c_nationkey,
    COUNT(*) AS n_rows
  FROM tpch.CUSTOMER
  GROUP BY
    1,
    2
), _s4 AS (
  SELECT
    _s0.c_mktsegment,
    NATION.n_name,
    SUM(_s0.n_rows) AS sum_n_rows
  FROM _s0 AS _s0
  JOIN tpch.NATION AS NATION
    ON NATION.n_nationkey = _s0.c_nationkey
  GROUP BY
    1,
    2
), _s5 AS (
  SELECT
    CUSTOMER.c_mktsegment,
    CUSTOMER.c_name,
    NATION.n_name,
    NATION.n_regionkey
  FROM tpch.CUSTOMER AS CUSTOMER
  JOIN tpch.NATION AS NATION
    ON CUSTOMER.c_nationkey = NATION.n_nationkey
)
SELECT
  REGION.r_name COLLATE utf8mb4_bin AS r_name,
  _s4.n_name COLLATE utf8mb4_bin AS n_name,
  _s5.c_mktsegment COLLATE utf8mb4_bin AS c_industry,
  _s5.c_name COLLATE utf8mb4_bin AS c_name,
  _s4.sum_n_rows AS n
FROM _s4 AS _s4
JOIN _s5 AS _s5
  ON _s4.c_mktsegment = _s5.c_mktsegment AND _s4.n_name = _s5.n_name
JOIN tpch.REGION AS REGION
  ON REGION.r_regionkey = _s5.n_regionkey
ORDER BY
  5 DESC,
  1,
  2,
  3,
  4
LIMIT 3
