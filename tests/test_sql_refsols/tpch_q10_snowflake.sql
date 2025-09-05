WITH _s3 AS (
  SELECT
    SUM(lineitem.l_extendedprice * (
      1 - lineitem.l_discount
    )) AS sum_expr,
    orders.o_custkey
  FROM tpch.orders AS orders
  JOIN tpch.lineitem AS lineitem
    ON lineitem.l_orderkey = orders.o_orderkey AND lineitem.l_returnflag = 'R'
  WHERE
    MONTH(CAST(orders.o_orderdate AS TIMESTAMP)) IN (10, 11, 12)
    AND YEAR(CAST(orders.o_orderdate AS TIMESTAMP)) = 1993
  GROUP BY
    2
)
SELECT
  customer.c_custkey AS C_CUSTKEY,
  customer.c_name AS C_NAME,
  COALESCE(_s3.sum_expr, 0) AS REVENUE,
  customer.c_acctbal AS C_ACCTBAL,
  nation.n_name AS N_NAME,
  customer.c_address AS C_ADDRESS,
  customer.c_phone AS C_PHONE,
  customer.c_comment AS C_COMMENT
FROM tpch.customer AS customer
LEFT JOIN _s3 AS _s3
  ON _s3.o_custkey = customer.c_custkey
JOIN tpch.nation AS nation
  ON customer.c_nationkey = nation.n_nationkey
ORDER BY
  3 DESC NULLS LAST,
  1 NULLS FIRST
LIMIT 20
