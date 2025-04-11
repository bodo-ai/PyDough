WITH _s3 AS (
  SELECT
    SUM(l_quantity) AS agg_0,
    l_orderkey AS order_key
  FROM tpch.lineitem
  GROUP BY
    l_orderkey
)
SELECT
  customer.c_name AS C_NAME,
  customer.c_custkey AS C_CUSTKEY,
  orders.o_orderkey AS O_ORDERKEY,
  orders.o_orderdate AS O_ORDERDATE,
  orders.o_totalprice AS O_TOTALPRICE,
  COALESCE(_s3.agg_0, 0) AS TOTAL_QUANTITY
FROM tpch.orders AS orders
LEFT JOIN tpch.customer AS customer
  ON customer.c_custkey = orders.o_custkey
LEFT JOIN _s3 AS _s3
  ON _s3.order_key = orders.o_orderkey
WHERE
  NOT _s3.agg_0 IS NULL AND _s3.agg_0 > 300
ORDER BY
  o_totalprice DESC,
  o_orderdate
LIMIT 10
