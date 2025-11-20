WITH _t2 AS (
  SELECT
    c_acctbal,
    c_nationkey,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY c_nationkey ORDER BY CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END DESC) - 1.0
        ) - (
          (
            COUNT(CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END) OVER (PARTITION BY c_nationkey) - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END
      ELSE NULL
    END AS expr_5,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY c_nationkey ORDER BY c_acctbal DESC) - 1.0
        ) - (
          (
            COUNT(c_acctbal) OVER (PARTITION BY c_nationkey) - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN c_acctbal
      ELSE NULL
    END AS expr_6,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY c_nationkey ORDER BY CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END DESC) - 1.0
        ) - (
          (
            COUNT(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END) OVER (PARTITION BY c_nationkey) - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END
      ELSE NULL
    END AS expr_7
  FROM tpch.CUSTOMER
), _s3 AS (
  SELECT
    c_nationkey,
    AVG(expr_5) AS avg_expr5,
    AVG(expr_6) AS avg_expr6,
    AVG(expr_7) AS avg_expr7,
    COUNT(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END) AS count_negativeacctbal,
    COUNT(CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END) AS count_nonnegativeacctbal
  FROM _t2
  GROUP BY
    1
)
SELECT
  NATION.n_name COLLATE utf8mb4_bin AS nation_name,
  _s3.count_negativeacctbal AS n_red_acctbal,
  _s3.count_nonnegativeacctbal AS n_black_acctbal,
  _s3.avg_expr7 AS median_red_acctbal,
  _s3.avg_expr5 AS median_black_acctbal,
  _s3.avg_expr6 AS median_overall_acctbal
FROM tpch.NATION AS NATION
JOIN tpch.REGION AS REGION
  ON NATION.n_regionkey = REGION.r_regionkey AND REGION.r_name = 'AMERICA'
JOIN _s3 AS _s3
  ON NATION.n_nationkey = _s3.c_nationkey
ORDER BY
  1
