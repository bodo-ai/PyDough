WITH "_T1" AS (
  SELECT
    CUSTOMER.c_acctbal AS C_ACCTBAL,
    NATION.n_regionkey AS N_REGIONKEY,
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
    END AS EXPR_7
  FROM TPCH.NATION NATION
  JOIN TPCH.CUSTOMER CUSTOMER
    ON CUSTOMER.c_nationkey = NATION.n_nationkey
), "_S3" AS (
  SELECT
    N_REGIONKEY,
    AVG(EXPR_5) AS AVG_EXPR_5,
    AVG(EXPR_6) AS AVG_EXPR_6,
    AVG(EXPR_7) AS AVG_EXPR_7,
    COUNT(CASE WHEN C_ACCTBAL < 0 THEN C_ACCTBAL ELSE NULL END) AS COUNT_NEGATIVE_ACCTBAL,
    COUNT(CASE WHEN C_ACCTBAL >= 0 THEN C_ACCTBAL ELSE NULL END) AS COUNT_NON_NEGATIVE_ACCTBAL
  FROM "_T1"
  GROUP BY
    N_REGIONKEY
)
SELECT
  REGION.r_name AS region_name,
  "_S3".COUNT_NEGATIVE_ACCTBAL AS n_red_acctbal,
  "_S3".COUNT_NON_NEGATIVE_ACCTBAL AS n_black_acctbal,
  "_S3".AVG_EXPR_7 AS median_red_acctbal,
  "_S3".AVG_EXPR_5 AS median_black_acctbal,
  "_S3".AVG_EXPR_6 AS median_overall_acctbal
FROM TPCH.REGION REGION
JOIN "_S3" "_S3"
  ON REGION.r_regionkey = "_S3".N_REGIONKEY
ORDER BY
  1 NULLS FIRST
