WITH _s1 AS (
  SELECT
    c_custkey
  FROM tpch.customer
  WHERE
    c_mktsegment = 'BUILDING'
), _t1 AS (
  SELECT
    1 AS _
  FROM tpch.orders AS orders
  LEFT JOIN _s1 AS _s1
    ON _s1.c_custkey = orders.o_custkey
  WHERE
    YEAR(CAST(orders.o_orderdate AS TIMESTAMP)) = 1995
  QUALIFY
    NOT expr_0 IS NULL AND c_acctbal < AVG(COALESCE(c_acctbal, 0)) OVER ()
)
SELECT
  COUNT(*) AS n
FROM _t1
