WITH _s5 AS (
  SELECT
    COUNT() AS agg_0,
    ps_suppkey AS supplier_key
  FROM tpch.partsupp
  GROUP BY
    ps_suppkey
), _t0 AS (
  SELECT
    supplier.s_name AS name_8
  FROM tpch.region AS region
  JOIN tpch.nation AS nation
    ON nation.n_regionkey = region.r_regionkey
  JOIN tpch.supplier AS supplier
    ON nation.n_nationkey = supplier.s_nationkey
  JOIN _s5 AS _s5
    ON _s5.supplier_key = supplier.s_suppkey
  QUALIFY
    NTILE(1000) OVER (PARTITION BY region.r_regionkey ORDER BY _s5.agg_0 NULLS LAST, supplier.s_name NULLS LAST) = 1000
)
SELECT
  name_8 AS name
FROM _t0
