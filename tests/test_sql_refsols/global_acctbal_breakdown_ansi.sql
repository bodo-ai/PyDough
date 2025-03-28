SELECT
  COUNT(CASE WHEN customer.c_acctbal < 0 THEN customer.c_acctbal ELSE NULL END) AS n_red_acctbal,
  COUNT(CASE WHEN customer.c_acctbal >= 0 THEN customer.c_acctbal ELSE NULL END) AS n_black_acctbal,
  MEDIAN(CASE WHEN customer.c_acctbal < 0 THEN customer.c_acctbal ELSE NULL END) AS median_red_acctbal,
  MEDIAN(CASE WHEN customer.c_acctbal >= 0 THEN customer.c_acctbal ELSE NULL END) AS median_black_acctbal,
  MEDIAN(customer.c_acctbal) AS median_overall_acctbal
FROM tpch.customer AS customer
