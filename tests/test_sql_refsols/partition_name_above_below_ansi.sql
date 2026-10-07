WITH _s0 AS (
  SELECT
    c_mktsegment,
    c_nationkey,
    COUNT(*) AS n_rows
  FROM tpch.customer
  GROUP BY
    1,
    2
), _s4 AS (
  SELECT
    _s0.c_mktsegment,
    nation.n_name,
    SUM(_s0.n_rows) AS sum_n_rows
  FROM _s0 AS _s0
  JOIN tpch.nation AS nation
    ON _s0.c_nationkey = nation.n_nationkey
  GROUP BY
    1,
    2
), _s5 AS (
  SELECT
    customer.c_mktsegment,
    customer.c_name,
    nation.n_name,
    nation.n_regionkey
  FROM tpch.customer AS customer
  JOIN tpch.nation AS nation
    ON customer.c_nationkey = nation.n_nationkey
)
SELECT
  region.r_name,
  _s4.n_name,
  _s5.c_mktsegment AS c_industry,
  _s5.c_name,
  _s4.sum_n_rows AS n
FROM _s4 AS _s4
JOIN _s5 AS _s5
  ON _s4.c_mktsegment = _s5.c_mktsegment AND _s4.n_name = _s5.n_name
JOIN tpch.region AS region
  ON _s5.n_regionkey = region.r_regionkey
ORDER BY
  5 DESC,
  1,
  2,
  3,
  4
LIMIT 3
