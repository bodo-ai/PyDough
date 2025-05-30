WITH _t0 AS (
  SELECT
    part.p_partkey AS key_13,
    DENSE_RANK() OVER (PARTITION BY region.r_regionkey ORDER BY part.p_size DESC, part.p_container DESC, part.p_type DESC) AS rank,
    region.r_name AS region
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
    part.p_partkey
  LIMIT 15
)
SELECT
  key_13 AS key,
  region,
  rank
FROM _t0
ORDER BY
  key_13
