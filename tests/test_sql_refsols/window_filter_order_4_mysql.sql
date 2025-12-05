WITH _t2 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM tpch.CUSTOMER AS CUSTOMER
  JOIN tpch.NATION AS NATION
    ON CUSTOMER.c_nationkey = NATION.n_nationkey AND NATION.n_name = 'GERMANY'
  JOIN tpch.ORDERS AS ORDERS
    ON CUSTOMER.c_custkey = ORDERS.o_custkey
    AND EXTRACT(YEAR FROM CAST(ORDERS.o_orderdate AS DATETIME)) = 1992
  GROUP BY
    ORDERS.o_custkey
), _t AS (
  SELECT
    n_rows,
    AVG(n_rows) OVER () AS _w
  FROM _t2
)
SELECT
  COUNT(*) AS n
FROM _t
WHERE
  _w > n_rows
