SELECT
  part.p_partkey AS key,
  region.r_name AS region,
  DENSE_RANK() OVER (PARTITION BY nation.n_regionkey ORDER BY part.p_size DESC, part.p_container DESC, part.p_type DESC) AS rank
FROM tpch.region AS region
JOIN tpch.nation AS nation
  ON nation.n_regionkey = region.r_regionkey
JOIN tpch.supplier AS supplier
  ON nation.n_nationkey = supplier.s_nationkey
JOIN tpch.partsupp AS partsupp
  ON partsupp.ps_suppkey = supplier.s_suppkey
JOIN tpch.part AS part
  ON part.p_partkey = partsupp.ps_partkey
ORDER BY
  1,
  2
LIMIT 15
