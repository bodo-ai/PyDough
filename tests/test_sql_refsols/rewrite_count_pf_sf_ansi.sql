WITH _s3 AS (
  SELECT
    c_nationkey
  FROM tpch.customer
  WHERE
    c_mktsegment = 'BUILDING'
)
SELECT
  COUNT(*) AS n
FROM tpch.nation AS nation
JOIN tpch.region AS region
  ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'EUROPE'
SEMI JOIN _s3 AS _s3
  ON _s3.c_nationkey = nation.n_nationkey
