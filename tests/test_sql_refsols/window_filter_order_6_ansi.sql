WITH _s1 AS (
  SELECT
    1 AS expr_0,
    c_acctbal,
    c_custkey
  FROM tpch.customer
  WHERE
    c_mktsegment = 'BUILDING'
), _t1 AS (
  SELECT
    1 AS "_"
  FROM tpch.orders AS orders
  LEFT JOIN _s1 AS _s1
    ON _s1.c_custkey = orders.o_custkey
  WHERE
    EXTRACT(YEAR FROM CAST(orders.o_orderdate AS DATETIME)) = 1995
  QUALIFY
    NOT _s1.expr_0 IS NULL
    AND _s1.c_acctbal < AVG(CAST(COALESCE(_s1.c_acctbal, 0) AS DOUBLE)) OVER ()
)
SELECT
  COUNT(*) AS n
FROM _t1
