SELECT
  COUNT(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END) AS n_red_acctbal,
  COUNT(CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END) AS n_black_acctbal,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY
    CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END) AS median_red_acctbal,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY
    CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END) AS median_black_acctbal,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY
    c_acctbal) AS median_overall_acctbal
FROM tpch.customer
