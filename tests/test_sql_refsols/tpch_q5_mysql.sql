WITH _s11 AS (
  SELECT
    NATION.n_name,
    SUPPLIER.s_suppkey
  FROM tpch.SUPPLIER AS SUPPLIER
  JOIN tpch.NATION AS NATION
    ON NATION.n_nationkey = SUPPLIER.s_nationkey
)
SELECT
  ANY_VALUE(NATION.n_name) AS N_NAME,
  COALESCE(SUM(LINEITEM.l_extendedprice * (
    1 - LINEITEM.l_discount
  )), 0) AS REVENUE
FROM tpch.NATION AS NATION
JOIN tpch.REGION AS REGION
  ON NATION.n_regionkey = REGION.r_regionkey AND REGION.r_name = 'ASIA'
JOIN tpch.CUSTOMER AS CUSTOMER
  ON CUSTOMER.c_nationkey = NATION.n_nationkey
JOIN tpch.ORDERS AS ORDERS
  ON CUSTOMER.c_custkey = ORDERS.o_custkey
  AND ORDERS.o_orderdate < CAST('1995-01-01' AS DATE)
  AND ORDERS.o_orderdate >= CAST('1994-01-01' AS DATE)
JOIN tpch.LINEITEM AS LINEITEM
  ON LINEITEM.l_orderkey = ORDERS.o_orderkey
JOIN _s11 AS _s11
  ON LINEITEM.l_suppkey = _s11.s_suppkey AND NATION.n_name = _s11.n_name
GROUP BY
  NATION.n_nationkey
ORDER BY
  2 DESC
