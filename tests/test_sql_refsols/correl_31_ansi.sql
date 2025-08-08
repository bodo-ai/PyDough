SELECT
  ANY_VALUE(nation.n_name) AS nation_name,
  AVG(lineitem.l_extendedprice * (
    1 - lineitem.l_discount
  )) AS mean_rev,
  MEDIAN(lineitem.l_extendedprice * (
    1 - lineitem.l_discount
  )) AS median_rev
FROM tpch.nation AS nation
JOIN tpch.region AS region
  ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'EUROPE'
JOIN tpch.customer AS customer
  ON customer.c_nationkey = nation.n_nationkey
JOIN tpch.orders AS orders
  ON EXTRACT(MONTH FROM CAST(orders.o_orderdate AS DATETIME)) = 1
  AND EXTRACT(YEAR FROM CAST(orders.o_orderdate AS DATETIME)) = 1996
  AND customer.c_custkey = orders.o_custkey
  AND orders.o_orderpriority = '1-URGENT'
JOIN tpch.lineitem AS lineitem
  ON lineitem.l_orderkey = orders.o_orderkey
  AND lineitem.l_shipmode = 'TRUCK'
  AND lineitem.l_tax < 0.05
JOIN tpch.supplier AS supplier
  ON lineitem.l_suppkey = supplier.s_suppkey
  AND nation.n_nationkey = supplier.s_nationkey
GROUP BY
  nation.n_nationkey
ORDER BY
  nation_name
