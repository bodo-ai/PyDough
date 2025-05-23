WITH _s3 AS (
  SELECT
    MEDIAN(CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END) AS agg_0,
    MEDIAN(c_acctbal) AS agg_1,
    MEDIAN(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END) AS agg_2,
    COUNT(CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END) AS agg_3,
    COUNT(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END) AS agg_4,
    c_nationkey AS nation_key
  FROM tpch.customer
  GROUP BY
    c_nationkey
)
SELECT
  nation.n_name AS nation_name,
  COALESCE(_s3.agg_4, 0) AS n_red_acctbal,
  COALESCE(_s3.agg_3, 0) AS n_black_acctbal,
  _s3.agg_2 AS median_red_acctbal,
  _s3.agg_0 AS median_black_acctbal,
  _s3.agg_1 AS median_overall_acctbal
FROM tpch.nation AS nation
JOIN tpch.region AS region
  ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'AMERICA'
LEFT JOIN _s3 AS _s3
  ON _s3.nation_key = nation.n_nationkey
ORDER BY
  nation_name
