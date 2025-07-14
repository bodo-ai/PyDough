SELECT
  ABS(c_acctbal) AS abs_value,
  ROUND(c_acctbal, 2) AS round_value,
  CAST(c_acctbal AS INTEGER) + CASE WHEN c_acctbal > CAST(c_acctbal AS INTEGER) THEN 1 ELSE 0 END AS ceil_value,
  CAST(c_acctbal AS INTEGER) - CASE WHEN c_acctbal < CAST(c_acctbal AS INTEGER) THEN 1 ELSE 0 END AS floor_value,
  POWER(c_acctbal, 2) AS power_value,
  POWER(c_acctbal, 0.5) AS sqrt_value,
  CASE WHEN c_acctbal = 0 THEN 0 ELSE CASE WHEN c_acctbal < 0 THEN -1 ELSE 1 END END AS sign_value,
  MIN(c_acctbal, 0) AS smallest_value,
  MAX(c_acctbal, 0) AS largest_value
FROM tpch.customer
