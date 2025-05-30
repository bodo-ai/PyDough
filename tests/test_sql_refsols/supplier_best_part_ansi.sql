WITH _t3 AS (
  SELECT
    COUNT() AS agg_0,
    SUM(l_quantity) AS agg_1,
    l_partkey AS part_key,
    l_suppkey AS supplier_key
  FROM tpch.lineitem
  WHERE
    EXTRACT(YEAR FROM l_shipdate) = 1994 AND l_tax = 0
  GROUP BY
    l_partkey,
    l_suppkey
), _t1 AS (
  SELECT
    _t3.agg_0 AS n_shipments,
    part.p_name AS part_name,
    COALESCE(_t3.agg_1, 0) AS quantity,
    partsupp.ps_suppkey AS supplier_key
  FROM tpch.partsupp AS partsupp
  JOIN _t3 AS _t3
    ON _t3.part_key = partsupp.ps_partkey AND _t3.supplier_key = partsupp.ps_suppkey
  JOIN tpch.part AS part
    ON part.p_partkey = partsupp.ps_partkey
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY partsupp.ps_suppkey ORDER BY COALESCE(_t3.agg_1, 0) DESC NULLS FIRST) = 1
)
SELECT
  supplier.s_name AS supplier_name,
  _t1.part_name,
  _t1.quantity AS total_quantity,
  _t1.n_shipments
FROM tpch.supplier AS supplier
JOIN tpch.nation AS nation
  ON nation.n_name = 'FRANCE' AND nation.n_nationkey = supplier.s_nationkey
JOIN _t1 AS _t1
  ON _t1.supplier_key = supplier.s_suppkey
ORDER BY
  total_quantity DESC,
  supplier_name
LIMIT 3
