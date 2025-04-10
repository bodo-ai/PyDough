WITH _s1 AS (
  SELECT
    SUM(l_extendedprice * (
      1 - l_discount
    )) AS agg_0,
    l_orderkey AS order_key
  FROM tpch.lineitem
  WHERE
    l_returnflag = 'R'
  GROUP BY
    l_orderkey
), _s3 AS (
  SELECT
    SUM(_s1.agg_0) AS agg_0,
    orders.o_custkey AS customer_key
  FROM tpch.orders AS orders
  JOIN _s1 AS _s1
    ON _s1.order_key = orders.o_orderkey
  WHERE
    orders.o_orderdate < '1994-01-01' AND orders.o_orderdate >= '1993-10-01'
  GROUP BY
    orders.o_custkey
)
SELECT
  customer.c_custkey AS C_CUSTKEY,
  customer.c_name AS C_NAME,
  COALESCE(_s3.agg_0, 0) AS REVENUE,
  customer.c_acctbal AS C_ACCTBAL,
  nation.n_name AS N_NAME,
  customer.c_address AS C_ADDRESS,
  customer.c_phone AS C_PHONE,
  customer.c_comment AS C_COMMENT
FROM tpch.customer AS customer
LEFT JOIN _s3 AS _s3
  ON _s3.customer_key = customer.c_custkey
LEFT JOIN tpch.nation AS nation
  ON customer.c_nationkey = nation.n_nationkey
ORDER BY
  revenue DESC,
  c_custkey
LIMIT 20
