SELECT
  SUBSTRING(c_phone, 1, 3) AS country_code,
  SUBSTRING(c_name, 2) AS name_without_first_char,
  SUBSTRING(c_phone, 0) AS last_digit,
  SUBSTRING(c_name, 2, -2) AS name_without_start_and_end_char,
  SUBSTRING(c_phone, 1, -5) AS phone_without_last_5_chars,
  SUBSTRING(c_name, -1, 1) AS name_second_to_last_char,
  c_acctbal >= 0 AS is_not_in_debt
FROM tpch.CUSTOMER
