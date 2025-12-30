WITH _t1 AS (
  SELECT
    1 AS "_"
  FROM tpch.orders AS orders
  JOIN tpch.customer AS customer
    ON customer.c_custkey = orders.o_custkey AND customer.c_mktsegment = 'BUILDING'
  WHERE
    YEAR(CAST(orders.o_orderdate AS TIMESTAMP)) = 1995
  QUALIFY
    customer.c_acctbal < AVG(customer.c_acctbal) OVER ()
)
SELECT
  COUNT(*) AS n
FROM _t1
