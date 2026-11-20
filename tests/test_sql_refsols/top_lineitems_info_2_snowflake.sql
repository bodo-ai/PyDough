WITH _s1 AS (
  SELECT
    ps_partkey,
    ps_suppkey
  FROM snowflake_sample_data.tpch_sf1.partsupp
)
SELECT
  lineitem.l_orderkey AS order_key,
  lineitem.l_linenumber AS line_number,
  part.p_size AS part_size,
  nation.n_nationkey AS supplier_nation
FROM snowflake_sample_data.tpch_sf1.part AS part
JOIN _s1 AS _s1
  ON _s1.ps_partkey = part.p_partkey
CROSS JOIN snowflake_sample_data.tpch_sf1.nation AS nation
JOIN snowflake_sample_data.tpch_sf1.supplier AS supplier
  ON nation.n_nationkey = supplier.s_nationkey
JOIN _s1 AS _s7
  ON _s7.ps_suppkey = supplier.s_suppkey
JOIN snowflake_sample_data.tpch_sf1.lineitem AS lineitem
  ON _s1.ps_suppkey = lineitem.l_suppkey
  AND _s7.ps_partkey = lineitem.l_partkey
  AND _s7.ps_suppkey = lineitem.l_suppkey
  AND lineitem.l_partkey = part.p_partkey
ORDER BY
  1 NULLS FIRST,
  2 NULLS FIRST
LIMIT 7
