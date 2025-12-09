WITH _s2 AS (
  SELECT
    l_linenumber,
    l_orderkey,
    l_partkey,
    l_suppkey
  FROM tpch.lineitem
  ORDER BY
    2 NULLS FIRST,
    1 NULLS FIRST
  LIMIT 7
), _s0 AS (
  SELECT
    ps_partkey,
    ps_suppkey
  FROM tpch.partsupp
)
SELECT
  _s2.l_orderkey AS order_key,
  _s2.l_linenumber AS line_number,
  part.p_size AS part_size,
  supplier.s_nationkey AS supplier_nation
FROM _s2 AS _s2
JOIN _s0 AS _s0
  ON _s0.ps_partkey = _s2.l_partkey AND _s0.ps_suppkey = _s2.l_suppkey
JOIN tpch.part AS part
  ON _s0.ps_partkey = part.p_partkey
JOIN _s0 AS _s4
  ON _s2.l_partkey = _s4.ps_partkey AND _s2.l_suppkey = _s4.ps_suppkey
JOIN tpch.supplier AS supplier
  ON _s4.ps_suppkey = supplier.s_suppkey
ORDER BY
  1 NULLS FIRST,
  2 NULLS FIRST
