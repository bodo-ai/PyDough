WITH _t0 AS (
  SELECT
    c_acctbal,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (ORDER BY CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END DESC NULLS LAST) - 1.0
        ) - (
          CAST((
            COUNT(CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END) OVER () - 1.0
          ) AS DOUBLE PRECISION) / 2.0
        )
      ) < 1.0
      THEN CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END
      ELSE NULL
    END AS expr_5,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (ORDER BY c_acctbal DESC NULLS LAST) - 1.0
        ) - (
          CAST((
            COUNT(c_acctbal) OVER () - 1.0
          ) AS DOUBLE PRECISION) / 2.0
        )
      ) < 1.0
      THEN c_acctbal
      ELSE NULL
    END AS expr_6,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (ORDER BY CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END DESC NULLS LAST) - 1.0
        ) - (
          CAST((
            COUNT(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END) OVER () - 1.0
          ) AS DOUBLE PRECISION) / 2.0
        )
      ) < 1.0
      THEN CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END
      ELSE NULL
    END AS expr_7
  FROM tpch.customer
)
SELECT
  COUNT(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END) AS n_red_acctbal,
  COUNT(CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END) AS n_black_acctbal,
  AVG(CAST(expr_7 AS DECIMAL)) AS median_red_acctbal,
  AVG(CAST(expr_5 AS DECIMAL)) AS median_black_acctbal,
  AVG(CAST(expr_6 AS DECIMAL)) AS median_overall_acctbal
FROM _t0
