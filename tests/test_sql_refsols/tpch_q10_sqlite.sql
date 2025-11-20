WITH _s3 AS (
  SELECT
    orders.o_custkey,
    SUM(lineitem.l_extendedprice * (
      1 - lineitem.l_discount
    )) AS sum_expr1
  FROM tpch.orders AS orders
  JOIN tpch.lineitem AS lineitem
    ON lineitem.l_orderkey = orders.o_orderkey AND lineitem.l_returnflag = 'R'
  WHERE
    CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER) = 1993
    AND CAST(STRFTIME('%m', orders.o_orderdate) AS INTEGER) IN (10, 11, 12)
  GROUP BY
    1
)
SELECT
  customer.c_custkey AS C_CUSTKEY,
  customer.c_name AS C_NAME,
  COALESCE(_s3.sum_expr1, 0) AS REVENUE,
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
  3 DESC,
  1
LIMIT 20
