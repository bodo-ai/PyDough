WITH _s1 AS (
  SELECT
    1 AS expr_0,
    c_acctbal,
    c_custkey
  FROM tpch.customer
  WHERE
    c_mktsegment = 'BUILDING'
), _t AS (
  SELECT
    _s1.c_acctbal,
    _s1.expr_0,
    AVG(COALESCE(_s1.c_acctbal, 0)) OVER () AS _w
  FROM tpch.orders AS orders
  LEFT JOIN _s1 AS _s1
    ON _s1.c_custkey = orders.o_custkey
  WHERE
    CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER) = 1995
)
SELECT
  COUNT(*) AS n
FROM _t
WHERE
  NOT expr_0 IS NULL AND _w > c_acctbal
