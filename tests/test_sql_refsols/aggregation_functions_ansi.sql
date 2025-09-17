WITH _s1 AS (
  SELECT
    o_custkey,
    COUNT(*) AS n_rows
  FROM tpch.orders
  GROUP BY
    1
), _t1 AS (
  SELECT
    PERCENTILE_DISC(0.8) WITHIN GROUP (ORDER BY
      customer.c_acctbal NULLS LAST) AS agg_7,
    ANY_VALUE(customer.c_acctbal) AS anything_c_acctbal,
    AVG(customer.c_acctbal) AS avg_c_acctbal,
    COUNT(customer.c_acctbal) AS count_c_acctbal,
    MAX(customer.c_acctbal) AS max_c_acctbal,
    MEDIAN(customer.c_acctbal) AS median_c_acctbal,
    MIN(customer.c_acctbal) AS min_c_acctbal,
    COUNT(DISTINCT customer.c_acctbal) AS ndistinct_c_acctbal,
    STDDEV_POP(customer.c_acctbal) AS population_std_c_acctbal,
    VARIANCE_POP(customer.c_acctbal) AS population_var_c_acctbal,
    STDDEV(customer.c_acctbal) AS sample_std_c_acctbal,
    VARIANCE(customer.c_acctbal) AS sample_var_c_acctbal,
    SUM(customer.c_acctbal) AS sum_c_acctbal,
    SUM(_s1.n_rows) AS sum_n_rows
  FROM tpch.customer AS customer
  LEFT JOIN _s1 AS _s1
    ON _s1.o_custkey = customer.c_custkey
  GROUP BY
    customer.c_nationkey
)
SELECT
  COALESCE(sum_c_acctbal, 0) AS sum_value,
  avg_c_acctbal AS avg_value,
  median_c_acctbal AS median_value,
  min_c_acctbal AS min_value,
  max_c_acctbal AS max_value,
  agg_7 AS quantile_value,
  anything_c_acctbal AS anything_value,
  count_c_acctbal AS count_value,
  ndistinct_c_acctbal AS count_distinct_value,
  sample_var_c_acctbal AS variance_s_value,
  population_var_c_acctbal AS variance_p_value,
  sample_std_c_acctbal AS stddev_s_value,
  population_std_c_acctbal AS stddev_p_value
FROM _t1
WHERE
  sum_n_rows = 0 OR sum_n_rows IS NULL
