WITH _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    o_custkey
  FROM tpch.orders
  GROUP BY
    o_custkey
), _t2 AS (
  SELECT
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY customer.c_nationkey ORDER BY customer.c_acctbal DESC) - 1.0
        ) - (
          CAST((
            COUNT(customer.c_acctbal) OVER (PARTITION BY customer.c_nationkey) - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN customer.c_acctbal
      ELSE NULL
    END AS expr_17,
    CASE
      WHEN CAST(0.19999999999999996 * COUNT(customer.c_acctbal) OVER (PARTITION BY customer.c_nationkey) AS INTEGER) < ROW_NUMBER() OVER (PARTITION BY customer.c_nationkey ORDER BY customer.c_acctbal DESC)
      THEN customer.c_acctbal
      ELSE NULL
    END AS expr_18,
    customer.c_acctbal,
    customer.c_nationkey,
    _s1.n_rows
  FROM tpch.customer AS customer
  LEFT JOIN _s1 AS _s1
    ON _s1.o_custkey = customer.c_custkey
), _t1 AS (
  SELECT
    MAX(c_acctbal) AS anything_c_acctbal,
    AVG(c_acctbal) AS avg_c_acctbal,
    AVG(expr_17) AS avg_expr_17,
    COUNT(c_acctbal) AS count_c_acctbal,
    MAX(c_acctbal) AS max_c_acctbal,
    MAX(expr_18) AS max_expr_18,
    MIN(c_acctbal) AS min_c_acctbal,
    COUNT(DISTINCT c_acctbal) AS ndistinct_c_acctbal,
    POWER(
      (
        CAST((
          SUM((
            POWER(c_acctbal, 2)
          )) - (
            CAST((
              POWER(SUM(c_acctbal), 2)
            ) AS REAL) / COUNT(c_acctbal)
          )
        ) AS REAL) / COUNT(c_acctbal)
      ),
      0.5
    ) AS population_std_c_acctbal,
    CAST((
      SUM((
        POWER(c_acctbal, 2)
      )) - (
        CAST((
          POWER(SUM(c_acctbal), 2)
        ) AS REAL) / COUNT(c_acctbal)
      )
    ) AS REAL) / COUNT(c_acctbal) AS population_var_c_acctbal,
    POWER(
      (
        CAST((
          SUM((
            POWER(c_acctbal, 2)
          )) - (
            CAST((
              POWER(SUM(c_acctbal), 2)
            ) AS REAL) / COUNT(c_acctbal)
          )
        ) AS REAL) / (
          COUNT(c_acctbal) - 1
        )
      ),
      0.5
    ) AS sample_std_c_acctbal,
    CAST((
      SUM((
        POWER(c_acctbal, 2)
      )) - (
        CAST((
          POWER(SUM(c_acctbal), 2)
        ) AS REAL) / COUNT(c_acctbal)
      )
    ) AS REAL) / (
      COUNT(c_acctbal) - 1
    ) AS sample_var_c_acctbal,
    SUM(c_acctbal) AS sum_c_acctbal,
    SUM(n_rows) AS sum_n_rows,
    c_nationkey
  FROM _t2
  GROUP BY
    c_nationkey
)
SELECT
  COALESCE(_t1.sum_c_acctbal, 0) AS sum_value,
  _t1.avg_c_acctbal AS avg_value,
  _t1.avg_expr_17 AS median_value,
  _t1.min_c_acctbal AS min_value,
  _t1.max_c_acctbal AS max_value,
  _t1.max_expr_18 AS quantile_value,
  _t1.anything_c_acctbal AS anything_value,
  _t1.count_c_acctbal AS count_value,
  _t1.ndistinct_c_acctbal AS count_distinct_value,
  _t1.sample_var_c_acctbal AS variance_s_value,
  _t1.population_var_c_acctbal AS variance_p_value,
  _t1.sample_std_c_acctbal AS stddev_s_value,
  _t1.population_std_c_acctbal AS stddev_p_value
FROM tpch.nation AS nation
JOIN _t1 AS _t1
  ON _t1.c_nationkey = nation.n_nationkey
  AND (
    _t1.sum_n_rows = 0 OR _t1.sum_n_rows IS NULL
  )
