WITH _s3 AS (
  SELECT
    c_nationkey,
    COUNT(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END) AS count_negativeacctbal,
    COUNT(CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END) AS count_nonnegativeacctbal,
    MEDIAN(c_acctbal) AS median_cacctbal,
    MEDIAN(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END) AS median_negativeacctbal,
    MEDIAN(CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END) AS median_nonnegativeacctbal
  FROM tpch.customer
  GROUP BY
    1
)
SELECT
  nation.n_name AS nation_name,
  _s3.count_negativeacctbal AS n_red_acctbal,
  _s3.count_nonnegativeacctbal AS n_black_acctbal,
  _s3.median_negativeacctbal AS median_red_acctbal,
  _s3.median_nonnegativeacctbal AS median_black_acctbal,
  _s3.median_cacctbal AS median_overall_acctbal
FROM tpch.nation AS nation
JOIN tpch.region AS region
  ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'AMERICA'
JOIN _s3 AS _s3
  ON _s3.c_nationkey = nation.n_nationkey
ORDER BY
  1 NULLS FIRST
