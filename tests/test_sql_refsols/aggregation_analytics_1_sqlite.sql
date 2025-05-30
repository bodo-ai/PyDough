WITH _s6 AS (
  SELECT
    SUM(
      lineitem.l_extendedprice * (
        1 - lineitem.l_discount
      ) * (
        1 - lineitem.l_tax
      ) - lineitem.l_quantity * partsupp.ps_supplycost
    ) AS agg_0,
    lineitem.l_orderkey AS order_key,
    partsupp.ps_partkey AS part_key,
    partsupp.ps_suppkey AS supplier_key
  FROM tpch.partsupp AS partsupp
  JOIN tpch.lineitem AS lineitem
    ON lineitem.l_partkey = partsupp.ps_partkey
    AND lineitem.l_suppkey = partsupp.ps_suppkey
  GROUP BY
    lineitem.l_orderkey,
    partsupp.ps_partkey,
    partsupp.ps_suppkey
), _s9 AS (
  SELECT
    SUM(_s6.agg_0) AS agg_0,
    _s6.part_key,
    _s6.supplier_key
  FROM _s6 AS _s6
  JOIN tpch.orders AS orders
    ON CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER) IN (1995, 1996)
    AND _s6.order_key = orders.o_orderkey
  GROUP BY
    _s6.part_key,
    _s6.supplier_key
)
SELECT
  part.p_name AS part_name,
  ROUND(COALESCE(_s9.agg_0, 0), 2) AS revenue_generated
FROM tpch.partsupp AS partsupp
JOIN tpch.supplier AS supplier
  ON partsupp.ps_suppkey = supplier.s_suppkey
  AND supplier.s_name = 'Supplier#000009450'
JOIN tpch.part AS part
  ON part.p_container LIKE 'LG%' AND part.p_partkey = partsupp.ps_partkey
LEFT JOIN _s9 AS _s9
  ON _s9.part_key = partsupp.ps_partkey AND _s9.supplier_key = partsupp.ps_suppkey
ORDER BY
  revenue_generated,
  part_name
LIMIT 8
