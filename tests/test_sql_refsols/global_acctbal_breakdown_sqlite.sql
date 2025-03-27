SELECT
  COUNT(negative_acctbal) AS n_red_acctbal,
  COUNT(non_negative_acctbal) AS n_black_acctbal,
  AVG(expr_7) AS median_red_acctbal,
  AVG(expr_5) AS median_black_acctbal,
  AVG(expr_6) AS median_overall_acctbal
FROM (
  SELECT
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (ORDER BY acctbal DESC) - 1.0
        ) - (
          CAST((
            COUNT(acctbal) OVER () - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN acctbal
      ELSE NULL
    END AS expr_6,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (ORDER BY negative_acctbal DESC) - 1.0
        ) - (
          CAST((
            COUNT(negative_acctbal) OVER () - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN negative_acctbal
      ELSE NULL
    END AS expr_7,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (ORDER BY non_negative_acctbal DESC) - 1.0
        ) - (
          CAST((
            COUNT(non_negative_acctbal) OVER () - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN non_negative_acctbal
      ELSE NULL
    END AS expr_5,
    negative_acctbal,
    non_negative_acctbal
  FROM (
    SELECT
      CASE WHEN acctbal >= 0 THEN acctbal ELSE NULL END AS non_negative_acctbal,
      CASE WHEN acctbal < 0 THEN acctbal ELSE NULL END AS negative_acctbal,
      acctbal
    FROM (
      SELECT
        c_acctbal AS acctbal
      FROM tpch.CUSTOMER
    )
  )
)
