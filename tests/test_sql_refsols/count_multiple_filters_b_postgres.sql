WITH _s0 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(CASE WHEN c_mktsegment = 'BUILDING' THEN 1 ELSE 0 END) AS sum_expr,
    SUM(CASE WHEN c_phone LIKE '11%' THEN 1 ELSE 0 END) AS sum_expr_11,
    SUM(CASE WHEN c_mktsegment = 'BUILDING' AND c_phone LIKE '11%' THEN 1 ELSE 0 END) AS sum_expr_12
  FROM tpch.customer
  WHERE
    c_acctbal <= 600 AND c_acctbal >= 500
), _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(CASE WHEN c_phone LIKE '11%' THEN 1 ELSE 0 END) AS sum_expr
  FROM tpch.customer
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
