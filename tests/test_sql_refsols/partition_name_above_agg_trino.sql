WITH _t3 AS (
  SELECT
    c_acctbal,
    c_mktsegment,
    c_nationkey
  FROM tpch.customer
  WHERE
    c_acctbal < 0
), _s0 AS (
  SELECT
    c_mktsegment,
    c_nationkey,
    COUNT(*) AS n_rows
  FROM _t3
  GROUP BY
    1,
    2
), _s1 AS (
  SELECT
    n_name,
    n_nationkey
  FROM tpch.nation
), _s4 AS (
  SELECT
    _s0.c_mktsegment,
    _s1.n_name,
    SUM(_s0.n_rows) AS sum_n_rows
  FROM _s0 AS _s0
  JOIN _s1 AS _s1
    ON _s0.c_nationkey = _s1.n_nationkey
  GROUP BY
    1,
    2
), _s5 AS (
  SELECT
    customer.c_mktsegment,
    customer.c_name,
    _s3.n_name,
    _s3.n_nationkey
  FROM tpch.customer AS customer
  JOIN _s1 AS _s3
    ON _s3.n_nationkey = customer.c_nationkey
  WHERE
    customer.c_acctbal < 0
), _s7 AS (
  SELECT
    c_mktsegment,
    c_nationkey,
    COUNT(*) AS n_rows
  FROM _t3
  GROUP BY
    1,
    2
)
SELECT
  _s5.n_name,
  _s5.c_mktsegment AS c_industry,
  _s5.c_name,
  _s4.sum_n_rows AS n,
  COALESCE(_s7.n_rows, 0) AS n_seg
FROM _s4 AS _s4
JOIN _s5 AS _s5
  ON _s4.c_mktsegment = _s5.c_mktsegment AND _s4.n_name = _s5.n_name
LEFT JOIN _s7 AS _s7
  ON _s5.c_mktsegment = _s7.c_mktsegment AND _s5.n_nationkey = _s7.c_nationkey
ORDER BY
  4 DESC,
  3 NULLS FIRST
LIMIT 3
