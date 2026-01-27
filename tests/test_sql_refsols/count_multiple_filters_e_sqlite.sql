WITH _s3 AS (
  SELECT
    o_custkey,
    COUNT(*) AS n_rows,
    SUM(IIF(o_orderpriority = '1-URGENT', 1, 0)) AS sum_expr,
    SUM(IIF(o_orderpriority = '2-HIGH', 1, 0)) AS sum_expr_21,
    SUM(IIF(o_orderpriority = '3-MEDIUM', 1, 0)) AS sum_expr_22
  FROM tpch.orders
  GROUP BY
    1
), _s5 AS (
  SELECT
    nation.n_regionkey,
    COUNT(*) AS n_rows,
    SUM(_s3.n_rows) AS sum_n_rows,
    SUM(_s3.sum_expr) AS sum_sum_expr,
    SUM(_s3.sum_expr_21) AS sum_sum_expr_21,
    SUM(_s3.sum_expr_22) AS sum_sum_expr_22
  FROM tpch.nation AS nation
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
  LEFT JOIN _s3 AS _s3
    ON _s3.o_custkey = customer.c_custkey
  GROUP BY
    1
)
SELECT
  region.r_name AS region_name,
  _s5.n_rows AS n1,
  COALESCE(_s5.sum_n_rows, 0) AS n2,
  COALESCE(_s5.sum_sum_expr, 0) AS n3,
  COALESCE(_s5.sum_sum_expr_21, 0) AS n4,
  COALESCE(_s5.sum_sum_expr_22, 0) AS n5
FROM tpch.region AS region
JOIN _s5 AS _s5
  ON _s5.n_regionkey = region.r_regionkey
