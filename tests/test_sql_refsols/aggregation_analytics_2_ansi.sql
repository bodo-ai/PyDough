WITH _t0 AS (
  SELECT
    SUM(
      lineitem.l_extendedprice * (
        1 - lineitem.l_discount
      ) * (
        1 - lineitem.l_tax
      ) - lineitem.l_quantity * partsupp.ps_supplycost
    ) AS agg_0,
    ANY_VALUE(partsupp.ps_partkey) AS agg_3,
    ANY_VALUE(partsupp.ps_suppkey) AS agg_4
  FROM tpch.partsupp AS partsupp
  JOIN tpch.lineitem AS lineitem
    ON lineitem.l_partkey = partsupp.ps_partkey
    AND lineitem.l_suppkey = partsupp.ps_suppkey
  JOIN tpch.orders AS orders
    ON EXTRACT(YEAR FROM orders.o_orderdate) IN (1995, 1996)
    AND lineitem.l_orderkey = orders.o_orderkey
  GROUP BY
    partsupp.ps_partkey,
    partsupp.ps_suppkey
)
SELECT
  part.p_name AS part_name,
  ROUND(COALESCE(_t0.agg_0, 0), 2) AS revenue_generated
FROM _t0 AS _t0
JOIN tpch.supplier AS supplier
  ON _t0.agg_4 = supplier.s_suppkey AND supplier.s_name = 'Supplier#000000182'
JOIN tpch.part AS part
  ON _t0.agg_3 = part.p_partkey AND part.p_container LIKE 'SM%'
ORDER BY
  revenue_generated,
  part_name
LIMIT 4
