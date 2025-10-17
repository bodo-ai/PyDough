WITH _s3 AS (
  SELECT
    nation.n_regionkey,
    COUNT(CASE WHEN customer.c_acctbal < 0 THEN customer.c_acctbal ELSE NULL END) AS count_negative_acctbal,
    COUNT(CASE WHEN customer.c_acctbal >= 0 THEN customer.c_acctbal ELSE NULL END) AS count_non_negative_acctbal,
    MEDIAN(customer.c_acctbal) AS median_c_acctbal,
    MEDIAN(CASE WHEN customer.c_acctbal < 0 THEN customer.c_acctbal ELSE NULL END) AS median_negative_acctbal,
    MEDIAN(CASE WHEN customer.c_acctbal >= 0 THEN customer.c_acctbal ELSE NULL END) AS median_non_negative_acctbal
  FROM tpch.nation AS nation
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
  GROUP BY
    1
)
SELECT
  region.r_name AS region_name,
  _s3.count_negative_acctbal AS n_red_acctbal,
  _s3.count_non_negative_acctbal AS n_black_acctbal,
  _s3.median_negative_acctbal AS median_red_acctbal,
  _s3.median_non_negative_acctbal AS median_black_acctbal,
  _s3.median_c_acctbal AS median_overall_acctbal
FROM tpch.region AS region
JOIN _s3 AS _s3
  ON _s3.n_regionkey = region.r_regionkey
ORDER BY
  1 NULLS FIRST
