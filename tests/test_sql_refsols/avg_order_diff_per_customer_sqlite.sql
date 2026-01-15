WITH _t1 AS (
  SELECT
    customer.c_name,
    orders.o_custkey,
    CAST((
      JULIANDAY(DATE(orders.o_orderdate, 'start of day')) - JULIANDAY(
        DATE(
          LAG(orders.o_orderdate, 1) OVER (PARTITION BY orders.o_custkey ORDER BY orders.o_orderdate),
          'start of day'
        )
      )
    ) AS INTEGER) AS day_diff
  FROM tpch.customer AS customer
  JOIN tpch.nation AS nation
    ON customer.c_nationkey = nation.n_nationkey AND nation.n_name = 'JAPAN'
  JOIN tpch.orders AS orders
    ON customer.c_custkey = orders.o_custkey AND orders.o_orderpriority = '1-URGENT'
)
SELECT
  MAX(c_name) AS name,
  AVG(day_diff) AS avg_diff
FROM _t1
GROUP BY
  o_custkey
ORDER BY
  2 DESC
LIMIT 5
