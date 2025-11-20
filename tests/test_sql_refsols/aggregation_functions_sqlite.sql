WITH _s1 AS (
  SELECT
    o_custkey,
    COUNT(*) AS n_rows
  FROM tpch.orders
  GROUP BY
    1
), _t2 AS (
  SELECT
    customer.c_acctbal,
    customer.c_nationkey,
    _s1.n_rows,
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
    END AS expr_18
  FROM tpch.customer AS customer
  LEFT JOIN _s1 AS _s1
    ON _s1.o_custkey = customer.c_custkey
), _t1 AS (
  SELECT
    MAX(c_acctbal) AS anything_cacctbal,
    AVG(c_acctbal) AS avg_cacctbal,
    AVG(expr_17) AS avg_expr17,
    COUNT(c_acctbal) AS count_cacctbal,
    MAX(c_acctbal) AS max_cacctbal,
    MAX(expr_18) AS max_expr18,
    MIN(c_acctbal) AS min_cacctbal,
    COUNT(DISTINCT c_acctbal) AS ndistinct_cacctbal,
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
    ) AS population_std_cacctbal,
    CAST((
      SUM((
        POWER(c_acctbal, 2)
      )) - (
        CAST((
          POWER(SUM(c_acctbal), 2)
        ) AS REAL) / COUNT(c_acctbal)
      )
    ) AS REAL) / COUNT(c_acctbal) AS population_var_cacctbal,
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
    ) AS sample_std_cacctbal,
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
    ) AS sample_var_cacctbal,
    SUM(c_acctbal) AS sum_cacctbal,
    SUM(n_rows) AS sum_nrows
  FROM _t2
  GROUP BY
    c_nationkey
)
SELECT
  COALESCE(sum_cacctbal, 0) AS sum_value,
  avg_cacctbal AS avg_value,
  avg_expr17 AS median_value,
  min_cacctbal AS min_value,
  max_cacctbal AS max_value,
  max_expr18 AS quantile_value,
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
