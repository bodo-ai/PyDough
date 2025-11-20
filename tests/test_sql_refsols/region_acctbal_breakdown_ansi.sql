WITH _s3 AS (
  SELECT
    nation.n_regionkey,
    COUNT(CASE WHEN customer.c_acctbal < 0 THEN customer.c_acctbal ELSE NULL END) AS count_negativeacctbal,
    COUNT(CASE WHEN customer.c_acctbal >= 0 THEN customer.c_acctbal ELSE NULL END) AS count_nonnegativeacctbal,
    MEDIAN(customer.c_acctbal) AS median_cacctbal,
    MEDIAN(CASE WHEN customer.c_acctbal < 0 THEN customer.c_acctbal ELSE NULL END) AS median_negativeacctbal,
    MEDIAN(CASE WHEN customer.c_acctbal >= 0 THEN customer.c_acctbal ELSE NULL END) AS median_nonnegativeacctbal
  FROM tpch.nation AS nation
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
  GROUP BY
    1
)
SELECT
  region.r_name AS region_name,
  _s3.count_negativeacctbal AS n_red_acctbal,
  _s3.count_nonnegativeacctbal AS n_black_acctbal,
  _s3.median_negativeacctbal AS median_red_acctbal,
  _s3.median_nonnegativeacctbal AS median_black_acctbal,
  _s3.median_cacctbal AS median_overall_acctbal
FROM tpch.region AS region
JOIN _s3 AS _s3
  ON _s3.n_regionkey = region.r_regionkey
ORDER BY
  1
