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
      customer.c_acctbal) AS agg_7,
    MAX(customer.c_acctbal) AS anything_cacctbal,
    AVG(CAST(customer.c_acctbal AS DECIMAL)) AS avg_cacctbal,
    COUNT(customer.c_acctbal) AS count_cacctbal,
    MAX(customer.c_acctbal) AS max_cacctbal,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY
      customer.c_acctbal) AS median_cacctbal,
    MIN(customer.c_acctbal) AS min_cacctbal,
    COUNT(DISTINCT customer.c_acctbal) AS ndistinct_cacctbal,
    STDDEV_POP(customer.c_acctbal) AS population_std_cacctbal,
    VAR_POP(customer.c_acctbal) AS population_var_cacctbal,
    STDDEV(customer.c_acctbal) AS sample_std_cacctbal,
    VAR_SAMP(customer.c_acctbal) AS sample_var_cacctbal,
    SUM(customer.c_acctbal) AS sum_cacctbal,
    SUM(_s1.n_rows) AS sum_nrows
  FROM tpch.customer AS customer
  LEFT JOIN _s1 AS _s1
    ON _s1.o_custkey = customer.c_custkey
  GROUP BY
    customer.c_nationkey
)
SELECT
  COALESCE(sum_cacctbal, 0) AS sum_value,
  avg_cacctbal AS avg_value,
  median_cacctbal AS median_value,
  min_cacctbal AS min_value,
  max_cacctbal AS max_value,
  agg_7 AS quantile_value,
  anything_cacctbal AS anything_value,
  count_cacctbal AS count_value,
  ndistinct_cacctbal AS count_distinct_value,
  sample_var_cacctbal AS variance_s_value,
  population_var_cacctbal AS variance_p_value,
  sample_std_cacctbal AS stddev_s_value,
  population_std_cacctbal AS stddev_p_value
FROM _t1
WHERE
  sum_nrows = 0 OR sum_nrows IS NULL
