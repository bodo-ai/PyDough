WITH _t4 AS (
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
  FROM _t4
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
    c_nationkey
  FROM _t4
)
SELECT
  ANY_VALUE(_s5.n_name) AS n_name,
  _s5.c_mktsegment AS c_industry,
  ANY_VALUE(_s5.c_name) AS c_name,
  ANY_VALUE(_s4.sum_n_rows) AS n,
  COUNT(_s7.c_nationkey) AS n_seg
FROM _s4 AS _s4
JOIN _s5 AS _s5
  ON _s4.c_mktsegment = _s5.c_mktsegment AND _s4.n_name = _s5.n_name
LEFT JOIN _s7 AS _s7
  ON _s5.c_mktsegment = _s7.c_mktsegment AND _s5.n_nationkey = _s7.c_nationkey
GROUP BY
  _s5.n_nationkey,
  2
ORDER BY
  4 DESC,
  3
LIMIT 3
