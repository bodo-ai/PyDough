WITH _t2 AS (
  SELECT
    s_suppkey AS key,
    s_name AS name
  FROM tpch.supplier
  WHERE
    s_name = 'Supplier#000009450'
), _s10 AS (
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
  JOIN _t2 AS _t7
    ON _t7.key = partsupp.ps_suppkey
  JOIN tpch.part AS part
    ON part.p_container LIKE 'LG%' AND part.p_partkey = partsupp.ps_partkey
  JOIN tpch.lineitem AS lineitem
    ON lineitem.l_partkey = partsupp.ps_partkey
    AND lineitem.l_suppkey = partsupp.ps_suppkey
  GROUP BY
    lineitem.l_orderkey,
    partsupp.ps_partkey,
    partsupp.ps_suppkey
), _s13 AS (
  SELECT
    SUM(_s10.agg_0) AS agg_0,
    _s10.part_key,
    _s10.supplier_key
  FROM _s10 AS _s10
  JOIN tpch.orders AS orders
    ON EXTRACT(YEAR FROM orders.o_orderdate) IN (1995, 1996)
    AND _s10.order_key = orders.o_orderkey
  GROUP BY
    _s10.part_key,
    _s10.supplier_key
)
SELECT
  part.p_name AS part_name,
  ROUND(COALESCE(_s13.agg_0, 0), 2) AS revenue_generated
FROM tpch.partsupp AS partsupp
JOIN _t2 AS _t2
  ON _t2.key = partsupp.ps_suppkey
JOIN tpch.part AS part
  ON part.p_container LIKE 'LG%' AND part.p_partkey = partsupp.ps_partkey
LEFT JOIN _s13 AS _s13
  ON _s13.part_key = partsupp.ps_partkey AND _s13.supplier_key = partsupp.ps_suppkey
ORDER BY
  revenue_generated,
  part_name
LIMIT 8
