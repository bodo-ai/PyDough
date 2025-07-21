WITH _s1 AS (
  SELECT
    n_name,
    n_nationkey
  FROM tpch.NATION
  WHERE
    n_name = 'FRANCE' OR n_name = 'GERMANY'
), _s9 AS (
  SELECT
    _s7.n_name,
    ORDERS.o_orderkey
  FROM tpch.ORDERS AS ORDERS
  JOIN tpch.CUSTOMER AS CUSTOMER
    ON CUSTOMER.c_custkey = ORDERS.o_custkey
  JOIN _s1 AS _s7
    ON CUSTOMER.c_nationkey = _s7.n_nationkey
)
SELECT
  _s1.n_name AS SUPP_NATION,
  _s9.n_name AS CUST_NATION,
  YEAR(LINEITEM.l_shipdate) AS L_YEAR,
  COALESCE(SUM(LINEITEM.l_extendedprice * (
    1 - LINEITEM.l_discount
  )), 0) AS REVENUE
FROM tpch.LINEITEM AS LINEITEM
JOIN tpch.SUPPLIER AS SUPPLIER
  ON LINEITEM.l_suppkey = SUPPLIER.s_suppkey
JOIN _s1 AS _s1
  ON SUPPLIER.s_nationkey = _s1.n_nationkey
JOIN _s9 AS _s9
  ON LINEITEM.l_orderkey = _s9.o_orderkey
  AND (
    _s1.n_name = 'FRANCE' OR _s9.n_name = 'FRANCE'
  )
  AND (
    _s1.n_name = 'GERMANY' OR _s9.n_name = 'GERMANY'
  )
WHERE
  YEAR(LINEITEM.l_shipdate) IN (1995, 1996)
GROUP BY
  _s9.n_name,
  YEAR(LINEITEM.l_shipdate),
  _s1.n_name
ORDER BY
  _s1.n_name,
  _s9.n_name,
  YEAR(LINEITEM.l_shipdate)
