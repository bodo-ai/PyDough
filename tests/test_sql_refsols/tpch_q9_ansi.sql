SELECT
  nation.n_name AS NATION,
  EXTRACT(YEAR FROM CAST(orders.o_orderdate AS DATETIME)) AS O_YEAR,
  COALESCE(
    SUM(
      lineitem.l_extendedprice * (
        1 - lineitem.l_discount
      ) - partsupp.ps_supplycost * lineitem.l_quantity
    ),
    0
  ) AS AMOUNT
FROM tpch.lineitem AS lineitem
JOIN tpch.part AS part
  ON lineitem.l_partkey = part.p_partkey AND part.p_name LIKE '%green%'
JOIN tpch.supplier AS supplier
  ON lineitem.l_suppkey = supplier.s_suppkey
JOIN tpch.nation AS nation
  ON nation.n_nationkey = supplier.s_nationkey
JOIN tpch.orders AS orders
  ON lineitem.l_orderkey = orders.o_orderkey
JOIN tpch.partsupp AS partsupp
  ON lineitem.l_partkey = partsupp.ps_partkey
  AND lineitem.l_suppkey = partsupp.ps_suppkey
GROUP BY
  nation.n_name,
  EXTRACT(YEAR FROM CAST(orders.o_orderdate AS DATETIME))
ORDER BY
  nation.n_name,
  o_year DESC
LIMIT 10
