WITH _t AS (
  SELECT
    CUSTOMER.c_acctbal,
    AVG(CUSTOMER.c_acctbal) OVER () AS _w
  FROM tpch.ORDERS AS ORDERS
  JOIN tpch.CUSTOMER AS CUSTOMER
    ON CUSTOMER.c_custkey = ORDERS.o_custkey AND CUSTOMER.c_mktsegment = 'BUILDING'
  WHERE
    EXTRACT(YEAR FROM CAST(ORDERS.o_orderdate AS DATETIME)) = 1995
)
SELECT
  COUNT(*) AS n
FROM _t
WHERE
  _w > c_acctbal
