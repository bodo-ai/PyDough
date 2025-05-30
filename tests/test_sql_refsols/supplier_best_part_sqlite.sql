WITH _t3 AS (
  SELECT
    COUNT() AS agg_0,
    SUM(l_quantity) AS agg_1,
    l_partkey AS part_key,
    l_suppkey AS supplier_key
  FROM tpch.lineitem
  WHERE
    CAST(STRFTIME('%Y', l_shipdate) AS INTEGER) = 1994 AND l_tax = 0
  GROUP BY
    l_partkey,
    l_suppkey
), _t AS (
  SELECT
    _t3.agg_0 AS n_shipments,
    part.p_name AS part_name,
    COALESCE(_t3.agg_1, 0) AS quantity,
    partsupp.ps_suppkey AS supplier_key,
    ROW_NUMBER() OVER (PARTITION BY partsupp.ps_suppkey ORDER BY COALESCE(_t3.agg_1, 0) DESC) AS _w
  FROM tpch.partsupp AS partsupp
  JOIN _t3 AS _t3
    ON _t3.part_key = partsupp.ps_partkey AND _t3.supplier_key = partsupp.ps_suppkey
  JOIN tpch.part AS part
    ON part.p_partkey = partsupp.ps_partkey
)
SELECT
  supplier.s_name AS supplier_name,
  _t.part_name,
  _t.quantity AS total_quantity,
  _t.n_shipments
FROM tpch.supplier AS supplier
JOIN tpch.nation AS nation
  ON nation.n_name = 'FRANCE' AND nation.n_nationkey = supplier.s_nationkey
JOIN _t AS _t
  ON _t._w = 1 AND _t.supplier_key = supplier.s_suppkey
ORDER BY
  total_quantity DESC,
  supplier_name
LIMIT 3
