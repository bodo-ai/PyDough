SELECT
  CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER) AS O_YEAR,
  CAST(COALESCE(
    SUM(
      IIF(
        nation_2.n_name = 'BRAZIL',
        lineitem.l_extendedprice * (
          1 - lineitem.l_discount
        ),
        0
      )
    ),
    0
  ) AS REAL) / COALESCE(SUM(lineitem.l_extendedprice * (
    1 - lineitem.l_discount
  )), 0) AS MKT_SHARE
FROM tpch.lineitem AS lineitem
JOIN tpch.part AS part
  ON lineitem.l_partkey = part.p_partkey AND part.p_type = 'ECONOMY ANODIZED STEEL'
JOIN tpch.orders AS orders
  ON CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER) IN (1995, 1996)
  AND lineitem.l_orderkey = orders.o_orderkey
JOIN tpch.customer AS customer
  ON customer.c_custkey = orders.o_custkey
JOIN tpch.nation AS nation
  ON customer.c_nationkey = nation.n_nationkey
JOIN tpch.region AS region
  ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'AMERICA'
JOIN tpch.supplier AS supplier
  ON lineitem.l_suppkey = supplier.s_suppkey
JOIN tpch.nation AS nation_2
  ON nation_2.n_nationkey = supplier.s_nationkey
GROUP BY
  CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER)
