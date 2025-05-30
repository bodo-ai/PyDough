WITH _t0 AS (
  SELECT
    SUM(
      lineitem.l_extendedprice * (
        1 - lineitem.l_discount
      ) * (
        1 - lineitem.l_tax
      ) - lineitem.l_quantity * partsupp.ps_supplycost
    ) AS agg_0,
    SUM(lineitem.l_quantity) AS agg_1,
    ANY_VALUE(partsupp.ps_partkey) AS agg_4,
    ANY_VALUE(partsupp.ps_suppkey) AS agg_5
  FROM tpch.partsupp AS partsupp
  JOIN tpch.lineitem AS lineitem
    ON lineitem.l_partkey = partsupp.ps_partkey
    AND lineitem.l_suppkey = partsupp.ps_suppkey
  JOIN tpch.orders AS orders
    ON EXTRACT(YEAR FROM orders.o_orderdate) = 1994
    AND lineitem.l_orderkey = orders.o_orderkey
  GROUP BY
    partsupp.ps_partkey,
    partsupp.ps_suppkey
)
SELECT
  part.p_name AS part_name,
  ROUND(COALESCE(_t0.agg_0, 0) / COALESCE(_t0.agg_1, 0), 2) AS revenue_ratio
FROM _t0 AS _t0
JOIN tpch.supplier AS supplier
  ON _t0.agg_5 = supplier.s_suppkey AND supplier.s_name = 'Supplier#000000182'
JOIN tpch.part AS part
  ON _t0.agg_4 = part.p_partkey AND part.p_container LIKE 'MED%'
ORDER BY
  revenue_ratio,
  part_name
LIMIT 3
