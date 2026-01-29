WITH _s0 AS (
  SELECT
    COUNT(*) AS n_rows,
    COUNT_IF(STARTSWITH(c_phone, '11')) AS sum_c_phone_startswith_11,
    COUNT_IF(c_mktsegment = 'BUILDING') AS sum_expr,
    COUNT_IF(STARTSWITH(c_phone, '11') AND c_mktsegment = 'BUILDING') AS sum_expr_12
  FROM tpch.customer
  WHERE
    c_acctbal <= 600 AND c_acctbal >= 500
), _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    COUNT_IF(STARTSWITH(c_phone, '11')) AS sum_c_phone_startswith_11
  FROM tpch.customer
  WHERE
    c_mktsegment = 'BUILDING'
)
SELECT
  _s0.n_rows AS n1,
  _s1.n_rows AS n2,
  _s0.sum_expr AS n3,
  _s0.sum_c_phone_startswith_11 AS n4,
  _s1.sum_c_phone_startswith_11 AS n5,
  _s0.sum_expr_12 AS n6
FROM _s0 AS _s0
CROSS JOIN _s1 AS _s1
