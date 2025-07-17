WITH _t0 AS (
  SELECT
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (ORDER BY CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END DESC) - 1.0
        ) - (
          (
            COUNT(CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END) OVER () - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END
      ELSE NULL
    END AS expr_5,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (ORDER BY c_acctbal DESC) - 1.0
        ) - (
          (
            COUNT(c_acctbal) OVER () - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN c_acctbal
      ELSE NULL
    END AS expr_6,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (ORDER BY CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END DESC) - 1.0
        ) - (
          (
            COUNT(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END) OVER () - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END
      ELSE NULL
    END AS expr_7,
    CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END AS negative_acctbal,
    CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END AS non_negative_acctbal
  FROM tpch.CUSTOMER
)
SELECT
  COUNT(negative_acctbal) AS n_red_acctbal,
  COUNT(non_negative_acctbal) AS n_black_acctbal,
  AVG(expr_7) AS median_red_acctbal,
  AVG(expr_5) AS median_black_acctbal,
  AVG(expr_6) AS median_overall_acctbal
FROM _t0
