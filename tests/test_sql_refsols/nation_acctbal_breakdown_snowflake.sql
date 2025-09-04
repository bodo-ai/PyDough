WITH _s3 AS (
  SELECT
    MEDIAN(CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END) AS agg_0,
    MEDIAN(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END) AS agg_2,
    COUNT(CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END) AS agg_3,
    COUNT(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END) AS agg_4,
    MEDIAN(c_acctbal) AS median_c_acctbal,
    c_nationkey
  FROM tpch.customer
  GROUP BY
    6
)
SELECT
  nation.n_name AS nation_name,
  _s3.agg_4 AS n_red_acctbal,
  _s3.agg_3 AS n_black_acctbal,
  _s3.agg_2 AS median_red_acctbal,
  _s3.agg_0 AS median_black_acctbal,
  _s3.median_c_acctbal AS median_overall_acctbal
FROM tpch.nation AS nation
JOIN tpch.region AS region
  ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'AMERICA'
JOIN _s3 AS _s3
  ON _s3.c_nationkey = nation.n_nationkey
ORDER BY
  1 NULLS FIRST
