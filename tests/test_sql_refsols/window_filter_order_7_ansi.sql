WITH _t1 AS (
  SELECT
    1 AS _
  FROM tpch.orders AS orders
  JOIN tpch.customer AS customer
    ON customer.c_custkey = orders.o_custkey AND customer.c_mktsegment = 'BUILDING'
  WHERE
    EXTRACT(YEAR FROM CAST(orders.o_orderdate AS DATETIME)) = 1995
  QUALIFY
    c_acctbal < AVG(c_acctbal) OVER ()
)
SELECT
  COUNT(*) AS n
FROM _t1
