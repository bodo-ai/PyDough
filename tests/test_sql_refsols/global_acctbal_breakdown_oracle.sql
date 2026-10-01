WITH "_t0" AS (
  SELECT
    C_ACCTBAL,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (ORDER BY CASE WHEN C_ACCTBAL >= 0 THEN C_ACCTBAL ELSE NULL END DESC NULLS LAST) - 1.0
        ) - (
          (
            COUNT(CASE WHEN C_ACCTBAL >= 0 THEN C_ACCTBAL ELSE NULL END) OVER () - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN CASE WHEN C_ACCTBAL >= 0 THEN C_ACCTBAL ELSE NULL END
      ELSE NULL
    END AS EXPR_5,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (ORDER BY C_ACCTBAL DESC NULLS LAST) - 1.0
        ) - (
          (
            COUNT(C_ACCTBAL) OVER () - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN C_ACCTBAL
      ELSE NULL
    END AS EXPR_6,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (ORDER BY CASE WHEN C_ACCTBAL < 0 THEN C_ACCTBAL ELSE NULL END DESC NULLS LAST) - 1.0
        ) - (
          (
            COUNT(CASE WHEN C_ACCTBAL < 0 THEN C_ACCTBAL ELSE NULL END) OVER () - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN CASE WHEN C_ACCTBAL < 0 THEN C_ACCTBAL ELSE NULL END
      ELSE NULL
    END AS EXPR_7
  FROM TPCH.CUSTOMER
)
SELECT
  COUNT(CASE WHEN C_ACCTBAL < 0 THEN C_ACCTBAL ELSE NULL END) AS n_red_acctbal,
  COUNT(CASE WHEN C_ACCTBAL >= 0 THEN C_ACCTBAL ELSE NULL END) AS n_black_acctbal,
  AVG(EXPR_7) AS median_red_acctbal,
  AVG(EXPR_5) AS median_black_acctbal,
  AVG(EXPR_6) AS median_overall_acctbal
FROM "_t0"
