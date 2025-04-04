WITH _t3_2 AS (
  SELECT
    COUNT(CASE WHEN customer.c_acctbal < 0 THEN customer.c_acctbal ELSE NULL END) AS agg_4,
    COUNT(CASE WHEN customer.c_acctbal >= 0 THEN customer.c_acctbal ELSE NULL END) AS agg_3,
    MEDIAN(customer.c_acctbal) AS agg_1,
    MEDIAN(CASE WHEN customer.c_acctbal < 0 THEN customer.c_acctbal ELSE NULL END) AS agg_2,
    MEDIAN(CASE WHEN customer.c_acctbal >= 0 THEN customer.c_acctbal ELSE NULL END) AS agg_0,
    nation.n_regionkey AS region_key
  FROM tpch.nation AS nation
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
  GROUP BY
    nation.n_regionkey
)
SELECT
  region.r_name AS region_name,
  COALESCE(_t3.agg_4, 0) AS n_red_acctbal,
  COALESCE(_t3.agg_3, 0) AS n_black_acctbal,
  _t3.agg_2 AS median_red_acctbal,
  _t3.agg_0 AS median_black_acctbal,
  _t3.agg_1 AS median_overall_acctbal
FROM tpch.region AS region
LEFT JOIN _t3_2 AS _t3
  ON _t3.region_key = region.r_regionkey
ORDER BY
  region.r_name
