WITH _s1 AS (
  SELECT
    COUNT() AS agg_0,
    l_partkey AS part_key,
    l_suppkey AS supplier_key
  FROM tpch.lineitem
  WHERE
    CAST(STRFTIME('%Y', l_shipdate) AS INTEGER) = 1994
  GROUP BY
    l_partkey,
    l_suppkey
), _t AS (
  SELECT
    COALESCE(_s1.agg_0, 0) AS n_orders,
    part.p_name AS part_name,
    partsupp.ps_suppkey AS supplier_key,
    ROW_NUMBER() OVER (PARTITION BY partsupp.ps_suppkey ORDER BY COALESCE(_s1.agg_0, 0) DESC, part.p_name) AS _w
  FROM tpch.partsupp AS partsupp
  LEFT JOIN _s1 AS _s1
    ON _s1.part_key = partsupp.ps_partkey AND _s1.supplier_key = partsupp.ps_suppkey
  JOIN tpch.part AS part
    ON part.p_partkey = partsupp.ps_partkey
), _s5 AS (
  SELECT
    n_orders,
    part_name,
    supplier_key
  FROM _t
  WHERE
    _w = 1
)
SELECT
  supplier.s_name AS supplier_name,
  _s5.part_name,
  _s5.n_orders
FROM tpch.supplier AS supplier
LEFT JOIN _s5 AS _s5
  ON _s5.supplier_key = supplier.s_suppkey
WHERE
  supplier.s_nationkey = 20
ORDER BY
  n_orders DESC,
  supplier_name
LIMIT 5
