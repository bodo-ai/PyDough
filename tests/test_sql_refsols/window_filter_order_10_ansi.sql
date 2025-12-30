WITH _t1 AS (
  SELECT
    1 AS "_"
  FROM tpch.orders AS orders
  JOIN tpch.customer AS customer
    ON customer.c_custkey = orders.o_custkey AND customer.c_mktsegment = 'BUILDING'
  WHERE
    orders.o_clerk = 'Clerk#000000001'
  QUALIFY
    o_totalprice < (
      0.05 * AVG(CAST(NULL AS INT)) OVER ()
    )
)
SELECT
  COUNT(*) AS n
FROM _t1
