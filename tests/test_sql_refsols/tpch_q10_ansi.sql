WITH _t3_2 AS (
  SELECT
    SUM(lineitem.l_extendedprice * (
      1 - lineitem.l_discount
    )) AS agg_0,
    orders.o_custkey AS customer_key
  FROM tpch.orders AS orders
  JOIN tpch.lineitem AS lineitem
    ON lineitem.l_orderkey = orders.o_orderkey AND lineitem.l_returnflag = 'R'
  WHERE
    orders.o_orderdate < CAST('1994-01-01' AS DATE)
    AND orders.o_orderdate >= CAST('1993-10-01' AS DATE)
  GROUP BY
    orders.o_custkey
)
SELECT
  customer.c_custkey AS C_CUSTKEY,
  customer.c_name AS C_NAME,
  COALESCE(_t3.agg_0, 0) AS REVENUE,
  customer.c_acctbal AS C_ACCTBAL,
  nation.n_name AS N_NAME,
  customer.c_address AS C_ADDRESS,
  customer.c_phone AS C_PHONE,
  customer.c_comment AS C_COMMENT
FROM tpch.customer AS customer
LEFT JOIN _t3_2 AS _t3
  ON _t3.customer_key = customer.c_custkey
LEFT JOIN tpch.nation AS nation
  ON customer.c_nationkey = nation.n_nationkey
ORDER BY
  revenue DESC,
  c_custkey
LIMIT 20
