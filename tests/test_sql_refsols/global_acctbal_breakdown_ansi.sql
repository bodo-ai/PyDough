SELECT
  COUNT(negative_acctbal) AS n_red_acctbal,
  COUNT(non_negative_acctbal) AS n_black_acctbal,
  MEDIAN(negative_acctbal) AS median_red_acctbal,
  MEDIAN(non_negative_acctbal) AS median_black_acctbal,
  MEDIAN(acctbal) AS median_overall_acctbal
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
