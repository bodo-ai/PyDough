WITH _s0 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(c_mktsegment = 'BUILDING') AS sum_expr,
    SUM(c_phone LIKE '11%') AS sum_expr_11,
    SUM(c_mktsegment = 'BUILDING' AND c_phone LIKE '11%') AS sum_expr_12
  FROM tpch.CUSTOMER
  WHERE
    c_acctbal <= 600 AND c_acctbal >= 500
), _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(c_phone LIKE '11%') AS sum_expr
  FROM tpch.CUSTOMER
  WHERE
    c_mktsegment = 'BUILDING'
)
SELECT
  _s0.n_rows AS n1,
  _s1.n_rows AS n2,
  _s0.sum_expr AS n3,
  _s0.sum_expr_11 AS n4,
  _s1.sum_expr AS n5,
  _s0.sum_expr_12 AS n6
FROM _s0 AS _s0
CROSS JOIN _s1 AS _s1
