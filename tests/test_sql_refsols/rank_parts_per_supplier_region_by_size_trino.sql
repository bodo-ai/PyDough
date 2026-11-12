SELECT
  part.p_partkey AS key,
  region.r_name AS region,
  DENSE_RANK() OVER (
    PARTITION BY nation.n_regionkey
    ORDER BY part.p_size DESC NULLS FIRST, part.p_container DESC NULLS FIRST, part.p_type DESC NULLS FIRST
  ) AS rank
FROM mongo.tpch.region AS region
JOIN cassandra.tpch.nation AS nation
  ON nation.n_regionkey = region.r_regionkey
JOIN mongo.tpch.supplier AS supplier
  ON nation.n_nationkey = supplier.s_nationkey
JOIN cassandra.tpch.partsupp AS partsupp
  ON partsupp.ps_suppkey = supplier.s_suppkey
JOIN mysql.tpch.part AS part
  ON part.p_partkey = partsupp.ps_partkey
ORDER BY
  1 NULLS FIRST,
  2 NULLS FIRST
LIMIT 15
