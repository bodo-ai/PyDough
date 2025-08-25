WITH _s3 AS (
  SELECT
    MEDIAN(CASE WHEN customer.c_acctbal >= 0 THEN customer.c_acctbal ELSE NULL END) AS median_black_acctbal,
    MEDIAN(customer.c_acctbal) AS median_overall_acctbal,
    MEDIAN(CASE WHEN customer.c_acctbal < 0 THEN customer.c_acctbal ELSE NULL END) AS median_red_acctbal,
    COUNT(CASE WHEN customer.c_acctbal >= 0 THEN customer.c_acctbal ELSE NULL END) AS n_black_acctbal,
    COUNT(CASE WHEN customer.c_acctbal < 0 THEN customer.c_acctbal ELSE NULL END) AS n_red_acctbal,
    nation.n_regionkey
  FROM tpch.nation AS nation
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
  GROUP BY
    6
)
SELECT
  region.r_name AS region_name,
  _s3.n_red_acctbal,
  _s3.n_black_acctbal,
  _s3.median_red_acctbal,
  _s3.median_black_acctbal,
  _s3.median_overall_acctbal
FROM tpch.region AS region
JOIN _s3 AS _s3
  ON _s3.n_regionkey = region.r_regionkey
ORDER BY
  1 NULLS FIRST
