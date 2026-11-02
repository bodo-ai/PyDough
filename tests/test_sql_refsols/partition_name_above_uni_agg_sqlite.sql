WITH _t5 AS (
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
  FROM _t5
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
), _t AS (
  SELECT
    _s5.c_mktsegment AS cmkt_7,
    _s5.n_name AS nnam_10,
    _s5.c_name,
    _s5.n_nationkey,
    _s4.sum_n_rows,
    ROW_NUMBER() OVER (PARTITION BY _s5.c_mktsegment, _s4.n_name ORDER BY _s5.c_name) AS _w
  FROM _s4 AS _s4
  JOIN _s5 AS _s5
    ON _s4.c_mktsegment = _s5.c_mktsegment AND _s4.n_name = _s5.n_name
), _s7 AS (
  SELECT
    c_mktsegment,
    c_nationkey,
    COUNT(*) AS n_rows
  FROM _t5
  GROUP BY
    1,
    2
)
SELECT
  _t.nnam_10 AS n_name,
  _t.cmkt_7 AS c_industry,
  _t.c_name,
  _t.sum_n_rows AS n,
  COALESCE(_s7.n_rows, 0) AS n_seg
FROM _t AS _t
LEFT JOIN _s7 AS _s7
  ON _s7.c_mktsegment = _t.cmkt_7 AND _s7.c_nationkey = _t.n_nationkey
WHERE
  _t._w = 1
ORDER BY
  4 DESC,
  3
LIMIT 3
