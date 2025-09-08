WITH _t1 AS (
  SELECT
    customer.c_acctbal,
    nation.n_regionkey,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY nation.n_regionkey ORDER BY CASE WHEN customer.c_acctbal >= 0 THEN customer.c_acctbal ELSE NULL END DESC) - 1.0
        ) - (
          CAST((
            COUNT(CASE WHEN customer.c_acctbal >= 0 THEN customer.c_acctbal ELSE NULL END) OVER (PARTITION BY nation.n_regionkey) - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN CASE WHEN customer.c_acctbal >= 0 THEN customer.c_acctbal ELSE NULL END
      ELSE NULL
    END AS expr_5,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY nation.n_regionkey ORDER BY customer.c_acctbal DESC) - 1.0
        ) - (
          CAST((
            COUNT(customer.c_acctbal) OVER (PARTITION BY nation.n_regionkey) - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN customer.c_acctbal
      ELSE NULL
    END AS expr_6,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY nation.n_regionkey ORDER BY CASE WHEN customer.c_acctbal < 0 THEN customer.c_acctbal ELSE NULL END DESC) - 1.0
        ) - (
          CAST((
            COUNT(CASE WHEN customer.c_acctbal < 0 THEN customer.c_acctbal ELSE NULL END) OVER (PARTITION BY nation.n_regionkey) - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN CASE WHEN customer.c_acctbal < 0 THEN customer.c_acctbal ELSE NULL END
      ELSE NULL
    END AS expr_7
  FROM tpch.nation AS nation
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
), _s3 AS (
  SELECT
    n_regionkey,
    AVG(expr_5) AS avg_expr_5,
    AVG(expr_6) AS avg_expr_6,
    AVG(expr_7) AS avg_expr_7,
    COUNT(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END) AS count_negative_acctbal,
    COUNT(CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END) AS count_non_negative_acctbal
  FROM _t1
  GROUP BY
    1
)
SELECT
  region.r_name AS region_name,
  _s3.count_negative_acctbal AS n_red_acctbal,
  _s3.count_non_negative_acctbal AS n_black_acctbal,
  _s3.avg_expr_7 AS median_red_acctbal,
  _s3.avg_expr_5 AS median_black_acctbal,
  _s3.avg_expr_6 AS median_overall_acctbal
FROM tpch.region AS region
JOIN _s3 AS _s3
  ON _s3.n_regionkey = region.r_regionkey
ORDER BY
  1
