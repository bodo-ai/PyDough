WITH _s3 AS (
  SELECT
    nation.n_nationkey
  FROM tpch.nation AS nation
  JOIN tpch.region AS region
    ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'AFRICA'
)
SELECT
  COUNT(*) AS n
FROM tpch.supplier AS supplier
ANTI JOIN _s3 AS _s3
  ON _s3.n_nationkey = supplier.s_nationkey
