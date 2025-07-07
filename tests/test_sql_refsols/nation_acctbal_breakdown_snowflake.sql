WITH _T2 AS (
  SELECT
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY c_nationkey ORDER BY CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END DESC NULLS LAST) - 1.0
        ) - (
          (
            COUNT(CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END) OVER (PARTITION BY c_nationkey) - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END
      ELSE NULL
    END AS EXPR_5,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY c_nationkey ORDER BY c_acctbal DESC NULLS LAST) - 1.0
        ) - (
          (
            COUNT(c_acctbal) OVER (PARTITION BY c_nationkey) - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN c_acctbal
      ELSE NULL
    END AS EXPR_6,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY c_nationkey ORDER BY CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END DESC NULLS LAST) - 1.0
        ) - (
          (
            COUNT(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END) OVER (PARTITION BY c_nationkey) - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END
      ELSE NULL
    END AS EXPR_7,
    c_nationkey AS C_NATIONKEY,
    CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END AS NEGATIVE_ACCTBAL,
    CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END AS NON_NEGATIVE_ACCTBAL
  FROM TPCH.CUSTOMER
), _S3 AS (
  SELECT
    AVG(EXPR_5) AS MEDIAN_BLACK_ACCTBAL,
    AVG(EXPR_6) AS MEDIAN_OVERALL_ACCTBAL,
    AVG(EXPR_7) AS MEDIAN_RED_ACCTBAL,
    COUNT(NON_NEGATIVE_ACCTBAL) AS N_BLACK_ACCTBAL,
    COUNT(NEGATIVE_ACCTBAL) AS N_RED_ACCTBAL,
    C_NATIONKEY
  FROM _T2
  GROUP BY
    C_NATIONKEY
)
SELECT
  NATION.n_name AS nation_name,
  _S3.N_RED_ACCTBAL AS n_red_acctbal,
  _S3.N_BLACK_ACCTBAL AS n_black_acctbal,
  _S3.MEDIAN_RED_ACCTBAL AS median_red_acctbal,
  _S3.MEDIAN_BLACK_ACCTBAL AS median_black_acctbal,
  _S3.MEDIAN_OVERALL_ACCTBAL AS median_overall_acctbal
FROM TPCH.NATION AS NATION
JOIN TPCH.REGION AS REGION
  ON NATION.n_regionkey = REGION.r_regionkey AND REGION.r_name = 'AMERICA'
JOIN _S3 AS _S3
  ON NATION.n_nationkey = _S3.C_NATIONKEY
ORDER BY
  NATION.n_name NULLS FIRST
