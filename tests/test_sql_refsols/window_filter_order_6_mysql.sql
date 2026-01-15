WITH _s1 AS (
  SELECT
    1 AS expr_0,
    c_acctbal,
    c_custkey
  FROM tpch.CUSTOMER
  WHERE
    c_mktsegment = 'BUILDING'
), _t AS (
  SELECT
    _s1.c_acctbal,
    _s1.expr_0,
    AVG(CAST(COALESCE(_s1.c_acctbal, 0) AS DOUBLE)) OVER () AS _w
  FROM tpch.ORDERS AS ORDERS
  LEFT JOIN _s1 AS _s1
    ON ORDERS.o_custkey = _s1.c_custkey
  WHERE
    EXTRACT(YEAR FROM CAST(ORDERS.o_orderdate AS DATETIME)) = 1995
)
SELECT
  COUNT(*) AS n
FROM _t
WHERE
  NOT expr_0 IS NULL AND _w > c_acctbal
