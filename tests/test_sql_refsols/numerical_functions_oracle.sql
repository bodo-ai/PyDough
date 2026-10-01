SELECT
  ABS(C_ACCTBAL) AS abs_value,
  ROUND(C_ACCTBAL, 2) AS round_value,
  CEIL(C_ACCTBAL) AS ceil_value,
  FLOOR(C_ACCTBAL) AS floor_value,
  POWER(C_ACCTBAL, 2) AS power_value,
  POWER(C_ACCTBAL, 0.5) AS sqrt_value,
  CASE WHEN C_ACCTBAL = 0 THEN 0 ELSE CASE WHEN C_ACCTBAL < 0 THEN -1 ELSE 1 END END AS sign_value,
  LEAST(C_ACCTBAL, 0) AS smallest_value,
  GREATEST(C_ACCTBAL, 0) AS largest_value
FROM TPCH.CUSTOMER
