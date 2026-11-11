SELECT
  part.p_partkey AS key,
  region.r_name AS region,
  DENSE_RANK() OVER (
    PARTITION BY nation.n_regionkey
    ORDER BY part.p_size DESC, part.p_container DESC, part.p_type DESC
  ) AS rank
FROM main.region AS region
JOIN main.nation AS nation
  ON nation.n_regionkey = region.r_regionkey
JOIN main.supplier AS supplier
  ON nation.n_nationkey = supplier.s_nationkey
JOIN main.partsupp AS partsupp
  ON partsupp.ps_suppkey = supplier.s_suppkey
JOIN main.part AS part
  ON part.p_partkey = partsupp.ps_partkey
ORDER BY
  1 NULLS FIRST,
  2 NULLS FIRST
LIMIT 15
