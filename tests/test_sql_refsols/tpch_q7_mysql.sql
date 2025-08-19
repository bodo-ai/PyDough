WITH _s9 AS (
  SELECT
    NATION.n_name,
    ORDERS.o_orderkey
  FROM tpch.ORDERS AS ORDERS
  JOIN tpch.CUSTOMER AS CUSTOMER
    ON CUSTOMER.c_custkey = ORDERS.o_custkey
  JOIN tpch.NATION AS NATION
    ON CUSTOMER.c_nationkey = NATION.n_nationkey
    AND (
      NATION.n_name = 'FRANCE' OR NATION.n_name = 'GERMANY'
    )
)
SELECT
  NATION.n_name COLLATE utf8mb4_bin AS SUPP_NATION,
  _s9.n_name COLLATE utf8mb4_bin AS CUST_NATION,
  EXTRACT(YEAR FROM CAST(LINEITEM.l_shipdate AS DATETIME)) AS L_YEAR,
  COALESCE(SUM(LINEITEM.l_extendedprice * (
    1 - LINEITEM.l_discount
  )), 0) AS REVENUE
FROM tpch.LINEITEM AS LINEITEM
JOIN tpch.SUPPLIER AS SUPPLIER
  ON LINEITEM.l_suppkey = SUPPLIER.s_suppkey
JOIN tpch.NATION AS NATION
  ON NATION.n_nationkey = SUPPLIER.s_nationkey
JOIN _s9 AS _s9
  ON LINEITEM.l_orderkey = _s9.o_orderkey
  AND (
    NATION.n_name = 'FRANCE' OR NATION.n_name = 'GERMANY'
  )
  AND (
    NATION.n_name = 'FRANCE' OR _s9.n_name = 'FRANCE'
  )
  AND (
    NATION.n_name = 'GERMANY' OR _s9.n_name = 'GERMANY'
  )
WHERE
  EXTRACT(YEAR FROM CAST(LINEITEM.l_shipdate AS DATETIME)) IN (1995, 1996)
GROUP BY
  1,
  2,
  3
ORDER BY
  1,
  2,
  3
