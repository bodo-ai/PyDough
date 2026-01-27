WITH _s1 AS (
  SELECT
    n_name,
    n_nationkey,
    n_regionkey
  FROM tpch.NATION
)
SELECT
  ANY_VALUE(_s1.n_name) AS supplier_nation,
  ANY_VALUE(_s5.n_name) AS customer_nation,
  COUNT(*) AS nation_combinations
FROM tpch.REGION AS REGION
JOIN _s1 AS _s1
  ON REGION.r_regionkey = _s1.n_regionkey
JOIN tpch.REGION AS REGION_2
  ON REGION_2.r_name = 'AMERICA'
JOIN _s1 AS _s5
  ON REGION_2.r_regionkey = _s5.n_regionkey
JOIN tpch.CUSTOMER AS CUSTOMER
  ON CUSTOMER.c_acctbal < 0 AND CUSTOMER.c_nationkey = _s5.n_nationkey
JOIN tpch.ORDERS AS ORDERS
  ON CUSTOMER.c_custkey = ORDERS.o_custkey
  AND EXTRACT(MONTH FROM CAST(ORDERS.o_orderdate AS DATETIME)) = 4
  AND EXTRACT(YEAR FROM CAST(ORDERS.o_orderdate AS DATETIME)) = 1992
JOIN tpch.LINEITEM AS LINEITEM
  ON LINEITEM.l_orderkey = ORDERS.o_orderkey AND LINEITEM.l_shipmode = 'SHIP'
JOIN tpch.SUPPLIER AS SUPPLIER
  ON LINEITEM.l_suppkey = SUPPLIER.s_suppkey
  AND SUPPLIER.s_nationkey = _s1.n_nationkey
WHERE
  REGION.r_name = 'ASIA'
GROUP BY
  _s1.n_nationkey,
  _s5.n_nationkey,
  REGION.r_regionkey,
  REGION_2.r_regionkey
