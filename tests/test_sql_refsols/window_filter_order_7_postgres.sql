WITH _t AS (
  SELECT
    customer.c_acctbal,
    AVG(CAST(customer.c_acctbal AS DOUBLE PRECISION)) OVER () AS _w
  FROM tpch.orders AS orders
  JOIN tpch.customer AS customer
    ON customer.c_custkey = orders.o_custkey AND customer.c_mktsegment = 'BUILDING'
  WHERE
    EXTRACT(YEAR FROM CAST(orders.o_orderdate AS TIMESTAMP)) = 1995
)
SELECT
  COUNT(*) AS n
FROM _t
WHERE
  _w > c_acctbal
