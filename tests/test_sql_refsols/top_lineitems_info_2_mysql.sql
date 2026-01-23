WITH _s1 AS (
  SELECT
    ps_partkey,
    ps_suppkey
  FROM tpch.PARTSUPP
)
SELECT
  LINEITEM.l_orderkey AS order_key,
  LINEITEM.l_linenumber AS line_number,
  PART.p_size AS part_size,
  NATION.n_nationkey AS supplier_nation
FROM tpch.PART AS PART
JOIN _s1 AS _s1
  ON PART.p_partkey = _s1.ps_partkey
CROSS JOIN tpch.NATION AS NATION
JOIN tpch.SUPPLIER AS SUPPLIER
  ON NATION.n_nationkey = SUPPLIER.s_nationkey
JOIN _s1 AS _s7
  ON SUPPLIER.s_suppkey = _s7.ps_suppkey
JOIN tpch.LINEITEM AS LINEITEM
  ON LINEITEM.l_partkey = PART.p_partkey
  AND LINEITEM.l_partkey = _s7.ps_partkey
  AND LINEITEM.l_suppkey = _s1.ps_suppkey
  AND LINEITEM.l_suppkey = _s7.ps_suppkey
ORDER BY
  1,
  2
LIMIT 7
