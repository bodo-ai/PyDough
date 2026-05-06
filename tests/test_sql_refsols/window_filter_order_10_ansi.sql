WITH _s1 AS (
  SELECT DISTINCT
    c_custkey
  FROM tpch.customer
  WHERE
    c_mktsegment = 'BUILDING'
), _t1 AS (
  SELECT
    1 AS "_"
  FROM tpch.orders AS orders
  JOIN _s1 AS _s1
    ON _s1.c_custkey = orders.o_custkey
  WHERE
    orders.o_clerk = 'Clerk#000000001'
  QUALIFY
    orders.o_totalprice < (
      0.05 * AVG(CAST(NULL AS INT)) OVER ()
    )
)
SELECT
  COUNT(*) AS n
FROM _t1
