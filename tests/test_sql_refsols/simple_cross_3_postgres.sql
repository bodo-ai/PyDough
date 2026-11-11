WITH _s1 AS (
  SELECT
    n_name,
    n_nationkey,
    n_regionkey
  FROM main.nation
)
SELECT
  ANY_VALUE(_s1.n_name) AS supplier_nation,
  ANY_VALUE(_s5.n_name) AS customer_nation,
  COUNT(*) AS nation_combinations
FROM main.region AS region
JOIN _s1 AS _s1
  ON _s1.n_regionkey = region.r_regionkey
JOIN main.region AS region_2
  ON region_2.r_name = 'AMERICA'
JOIN _s1 AS _s5
  ON _s5.n_regionkey = region_2.r_regionkey
JOIN main.customer AS customer
  ON _s5.n_nationkey = customer.c_nationkey AND customer.c_acctbal < 0
JOIN main.orders AS orders
  ON EXTRACT(MONTH FROM CAST(orders.o_orderdate AS TIMESTAMP)) = 4
  AND EXTRACT(YEAR FROM CAST(orders.o_orderdate AS TIMESTAMP)) = 1992
  AND customer.c_custkey = orders.o_custkey
JOIN main.lineitem AS lineitem
  ON lineitem.l_orderkey = orders.o_orderkey AND lineitem.l_shipmode = 'SHIP'
JOIN main.supplier AS supplier
  ON _s1.n_nationkey = supplier.s_nationkey
  AND lineitem.l_suppkey = supplier.s_suppkey
WHERE
  region.r_name = 'ASIA'
GROUP BY
  _s1.n_nationkey,
  _s5.n_nationkey,
  region.r_regionkey,
  region_2.r_regionkey
