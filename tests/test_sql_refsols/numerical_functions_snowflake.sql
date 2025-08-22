SELECT
  ABS(c_acctbal) AS abs_value,
  ROUND(c_acctbal, 2) AS round_value,
  CEIL(c_acctbal) AS ceil_value,
  FLOOR(c_acctbal) AS floor_value,
  POWER(c_acctbal, 2) AS power_value,
  POWER(c_acctbal, 0.5) AS sqrt_value,
  SIGN(c_acctbal) AS sign_value,
  LEAST(c_acctbal, 0) AS smallest_value,
  GREATEST(c_acctbal, 0) AS largest_value
FROM TPCH.CUSTOMER
