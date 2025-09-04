WITH _s3 AS (
  SELECT
    MEDIAN(CASE WHEN customer.c_acctbal >= 0 THEN customer.c_acctbal ELSE NULL END) AS agg_0,
    MEDIAN(CASE WHEN customer.c_acctbal < 0 THEN customer.c_acctbal ELSE NULL END) AS agg_2,
    COUNT(CASE WHEN customer.c_acctbal >= 0 THEN customer.c_acctbal ELSE NULL END) AS agg_3,
    COUNT(CASE WHEN customer.c_acctbal < 0 THEN customer.c_acctbal ELSE NULL END) AS agg_4,
    MEDIAN(customer.c_acctbal) AS median_c_acctbal,
    nation.n_regionkey
  FROM tpch.nation AS nation
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
  GROUP BY
    6
)
SELECT
  region.r_name AS region_name,
  _s3.agg_4 AS n_red_acctbal,
  _s3.agg_3 AS n_black_acctbal,
  _s3.agg_2 AS median_red_acctbal,
  _s3.agg_0 AS median_black_acctbal,
  _s3.median_c_acctbal AS median_overall_acctbal
FROM tpch.region AS region
JOIN _s3 AS _s3
  ON _s3.n_regionkey = region.r_regionkey
ORDER BY
  1
