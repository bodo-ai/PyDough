WITH _s3 AS (
  SELECT
    o_custkey,
    COUNT(*) AS n_rows,
    SUM(CASE WHEN o_orderpriority = '1-URGENT' THEN 1 ELSE 0 END) AS sum_expr,
    SUM(CASE WHEN o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS sum_expr_21,
    SUM(CASE WHEN o_orderpriority = '3-MEDIUM' THEN 1 ELSE 0 END) AS sum_expr_22
  FROM tpch.ORDERS
  GROUP BY
    1
), _s5 AS (
  SELECT
    NATION.n_regionkey,
    COUNT(*) AS n_rows,
    SUM(_s3.n_rows) AS sum_n_rows,
    SUM(_s3.sum_expr) AS sum_sum_expr,
    SUM(_s3.sum_expr_21) AS sum_sum_expr_21,
    SUM(_s3.sum_expr_22) AS sum_sum_expr_22
  FROM tpch.NATION AS NATION
  JOIN tpch.CUSTOMER AS CUSTOMER
    ON CUSTOMER.c_nationkey = NATION.n_nationkey
  LEFT JOIN _s3 AS _s3
    ON CUSTOMER.c_custkey = _s3.o_custkey
  GROUP BY
    1
)
SELECT
  REGION.r_name AS region_name,
  _s5.n_rows AS n1,
  COALESCE(_s5.sum_n_rows, 0) AS n2,
  COALESCE(_s5.sum_sum_expr, 0) AS n3,
  COALESCE(_s5.sum_sum_expr_21, 0) AS n4,
  COALESCE(_s5.sum_sum_expr_22, 0) AS n5
FROM tpch.REGION AS REGION
JOIN _s5 AS _s5
  ON REGION.r_regionkey = _s5.n_regionkey
