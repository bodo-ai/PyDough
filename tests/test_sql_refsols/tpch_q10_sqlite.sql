WITH _s3 AS (
  SELECT
    SUM(lineitem.l_extendedprice * (
      1 - lineitem.l_discount
    )) AS sum_expr_1,
    orders.o_custkey
  FROM tpch.orders AS orders
  JOIN tpch.lineitem AS lineitem
    ON lineitem.l_orderkey = orders.o_orderkey AND lineitem.l_returnflag = 'R'
  WHERE
    CASE
      WHEN CAST(STRFTIME('%m', orders.o_orderdate) AS INTEGER) <= 3
      AND CAST(STRFTIME('%m', orders.o_orderdate) AS INTEGER) >= 1
      THEN 1
      WHEN CAST(STRFTIME('%m', orders.o_orderdate) AS INTEGER) <= 6
      AND CAST(STRFTIME('%m', orders.o_orderdate) AS INTEGER) >= 4
      THEN 2
      WHEN CAST(STRFTIME('%m', orders.o_orderdate) AS INTEGER) <= 9
      AND CAST(STRFTIME('%m', orders.o_orderdate) AS INTEGER) >= 7
      THEN 3
      WHEN CAST(STRFTIME('%m', orders.o_orderdate) AS INTEGER) <= 12
      AND CAST(STRFTIME('%m', orders.o_orderdate) AS INTEGER) >= 10
      THEN 4
    END = 4
    AND CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER) = 1993
  GROUP BY
    orders.o_custkey
)
SELECT
  customer.c_custkey AS C_CUSTKEY,
  customer.c_name AS C_NAME,
  COALESCE(_s3.sum_expr_1, 0) AS REVENUE,
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
  COALESCE(_s3.sum_expr_1, 0) DESC,
  c_custkey
LIMIT 20
