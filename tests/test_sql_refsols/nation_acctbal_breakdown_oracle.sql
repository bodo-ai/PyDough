WITH "_S3" AS (
  SELECT
    c_acctbal AS C_ACCTBAL,
    c_nationkey AS C_NATIONKEY,
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
    END AS EXPR_7
  FROM TPCH.CUSTOMER
)
SELECT
  ANY_VALUE(NATION.n_name) AS nation_name,
  COUNT(CASE WHEN "_S3".C_ACCTBAL < 0 THEN "_S3".C_ACCTBAL ELSE NULL END) AS n_red_acctbal,
  COUNT(CASE WHEN "_S3".C_ACCTBAL >= 0 THEN "_S3".C_ACCTBAL ELSE NULL END) AS n_black_acctbal,
  AVG("_S3".EXPR_7) AS median_red_acctbal,
  AVG("_S3".EXPR_5) AS median_black_acctbal,
  AVG("_S3".EXPR_6) AS median_overall_acctbal
FROM TPCH.NATION NATION
JOIN TPCH.REGION REGION
  ON NATION.n_regionkey = REGION.r_regionkey AND REGION.r_name = 'AMERICA'
JOIN "_S3" "_S3"
  ON NATION.n_nationkey = "_S3".C_NATIONKEY
GROUP BY
  "_S3".C_NATIONKEY
ORDER BY
  1 NULLS FIRST
