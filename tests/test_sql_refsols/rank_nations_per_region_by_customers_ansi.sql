WITH _s3 AS (
  SELECT
    COUNT() AS agg_0,
    c_nationkey AS nation_key
  FROM tpch.customer
  GROUP BY
    c_nationkey
)
SELECT
  nation.n_name AS name,
  ROW_NUMBER() OVER (PARTITION BY region.r_regionkey ORDER BY _s3.agg_0 DESC NULLS FIRST) AS rank
FROM tpch.region AS region
JOIN tpch.nation AS nation
  ON nation.n_regionkey = region.r_regionkey
JOIN _s3 AS _s3
  ON _s3.nation_key = nation.n_nationkey
ORDER BY
  rank
LIMIT 5
