WITH _s1 AS (
  SELECT
    o_custkey,
    COUNT(*) AS n_rows
  FROM tpch.orders
  GROUP BY
    1
), _t1 AS (
  SELECT
    customer.c_nationkey,
    PERCENTILE_DISC(0.8) WITHIN GROUP (ORDER BY
      customer.c_acctbal NULLS LAST) AS agg_7,
    ANY_VALUE(customer.c_acctbal) AS anything_c_acctbal,
    AVG(customer.c_acctbal) AS avg_c_acctbal,
    COUNT(customer.c_acctbal) AS count_c_acctbal,
    MAX(customer.c_acctbal) AS max_c_acctbal,
    MEDIAN(customer.c_acctbal) AS median_c_acctbal,
    MIN(customer.c_acctbal) AS min_c_acctbal,
    COUNT(DISTINCT customer.c_acctbal) AS ndistinct_c_acctbal,
    STDDEV(customer.c_acctbal) AS sample_std_c_acctbal,
    VARIANCE(customer.c_acctbal) AS sample_variance_c_acctbal,
    SUM(customer.c_acctbal) AS sum_c_acctbal,
    SUM(_s1.n_rows) AS sum_n_rows
  FROM tpch.customer AS customer
  LEFT JOIN _s1 AS _s1
    ON _s1.o_custkey = customer.c_custkey
  GROUP BY
    1
)
SELECT
  COALESCE(_t1.sum_c_acctbal, 0) AS sum_value,
  _t1.avg_c_acctbal AS avg_value,
  _t1.median_c_acctbal AS median_value,
  _t1.min_c_acctbal AS min_value,
  _t1.max_c_acctbal AS max_value,
  _t1.agg_7 AS quantile_value,
  _t1.anything_c_acctbal AS anything_value,
  _t1.count_c_acctbal AS count_value,
  _t1.ndistinct_c_acctbal AS count_distinct_value,
  _t1.sample_variance_c_acctbal AS variance_value,
  _t1.sample_std_c_acctbal AS stddev_value
FROM tpch.nation AS nation
JOIN _t1 AS _t1
  ON _t1.c_nationkey = nation.n_nationkey
  AND (
    _t1.sum_n_rows = 0 OR _t1.sum_n_rows IS NULL
  )
