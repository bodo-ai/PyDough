WITH _T1 AS (
  SELECT
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY NATION.n_regionkey ORDER BY CASE WHEN CUSTOMER.c_acctbal >= 0 THEN CUSTOMER.c_acctbal ELSE NULL END DESC NULLS LAST) - 1.0
        ) - (
          (
            COUNT(CASE WHEN CUSTOMER.c_acctbal >= 0 THEN CUSTOMER.c_acctbal ELSE NULL END) OVER (PARTITION BY NATION.n_regionkey) - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN CASE WHEN CUSTOMER.c_acctbal >= 0 THEN CUSTOMER.c_acctbal ELSE NULL END
      ELSE NULL
    END AS EXPR_5,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY NATION.n_regionkey ORDER BY CUSTOMER.c_acctbal DESC NULLS LAST) - 1.0
        ) - (
          (
            COUNT(CUSTOMER.c_acctbal) OVER (PARTITION BY NATION.n_regionkey) - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN CUSTOMER.c_acctbal
      ELSE NULL
    END AS EXPR_6,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY NATION.n_regionkey ORDER BY CASE WHEN CUSTOMER.c_acctbal < 0 THEN CUSTOMER.c_acctbal ELSE NULL END DESC NULLS LAST) - 1.0
        ) - (
          (
            COUNT(CASE WHEN CUSTOMER.c_acctbal < 0 THEN CUSTOMER.c_acctbal ELSE NULL END) OVER (PARTITION BY NATION.n_regionkey) - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN CASE WHEN CUSTOMER.c_acctbal < 0 THEN CUSTOMER.c_acctbal ELSE NULL END
      ELSE NULL
    END AS EXPR_7,
    NATION.n_regionkey AS N_REGIONKEY,
    CASE WHEN CUSTOMER.c_acctbal < 0 THEN CUSTOMER.c_acctbal ELSE NULL END AS NEGATIVE_ACCTBAL,
    CASE WHEN CUSTOMER.c_acctbal >= 0 THEN CUSTOMER.c_acctbal ELSE NULL END AS NON_NEGATIVE_ACCTBAL
  FROM TPCH.NATION AS NATION
  JOIN TPCH.CUSTOMER AS CUSTOMER
    ON CUSTOMER.c_nationkey = NATION.n_nationkey
), _S3 AS (
  SELECT
    AVG(EXPR_5) AS MEDIAN_BLACK_ACCTBAL,
    AVG(EXPR_6) AS MEDIAN_OVERALL_ACCTBAL,
    AVG(EXPR_7) AS MEDIAN_RED_ACCTBAL,
    COUNT(NON_NEGATIVE_ACCTBAL) AS N_BLACK_ACCTBAL,
    COUNT(NEGATIVE_ACCTBAL) AS N_RED_ACCTBAL,
    N_REGIONKEY
  FROM _T1
  GROUP BY
    N_REGIONKEY
)
SELECT
  REGION.r_name AS region_name,
  _S3.N_RED_ACCTBAL AS n_red_acctbal,
  _S3.N_BLACK_ACCTBAL AS n_black_acctbal,
  _S3.MEDIAN_RED_ACCTBAL AS median_red_acctbal,
  _S3.MEDIAN_BLACK_ACCTBAL AS median_black_acctbal,
  _S3.MEDIAN_OVERALL_ACCTBAL AS median_overall_acctbal
FROM TPCH.REGION AS REGION
JOIN _S3 AS _S3
  ON REGION.r_regionkey = _S3.N_REGIONKEY
ORDER BY
  REGION.r_name NULLS FIRST
