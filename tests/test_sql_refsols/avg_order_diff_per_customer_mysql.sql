WITH _t1 AS (
  SELECT
    CUSTOMER.c_name,
    ORDERS.o_custkey,
    DATEDIFF(
      ORDERS.o_orderdate,
      LAG(ORDERS.o_orderdate, 1) OVER (PARTITION BY ORDERS.o_custkey ORDER BY CASE WHEN ORDERS.o_orderdate IS NULL THEN 1 ELSE 0 END, ORDERS.o_orderdate)
    ) AS day_diff
  FROM tpch.CUSTOMER AS CUSTOMER
  JOIN tpch.NATION AS NATION
    ON CUSTOMER.c_nationkey = NATION.n_nationkey AND NATION.n_name = 'JAPAN'
  JOIN tpch.ORDERS AS ORDERS
    ON CUSTOMER.c_custkey = ORDERS.o_custkey AND ORDERS.o_orderpriority = '1-URGENT'
)
SELECT
  ANY_VALUE(c_name) AS name,
  AVG(day_diff) AS avg_diff
FROM _t1
GROUP BY
  o_custkey
ORDER BY
  2 DESC
LIMIT 5
