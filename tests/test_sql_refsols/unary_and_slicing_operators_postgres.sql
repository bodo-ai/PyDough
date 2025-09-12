SELECT
  SUBSTRING(c_phone FROM 1 FOR 3) AS country_code,
  SUBSTRING(c_name FROM 2) AS name_without_first_char,
  SUBSTRING(c_phone FROM GREATEST(LENGTH(c_phone) + 0, 1)) AS last_digit,
  SUBSTRING(c_name FROM 2 FOR GREATEST(LENGTH(c_name) + -1 - 1, 0)) AS name_without_start_and_end_char,
  SUBSTRING(c_phone FROM 1 FOR GREATEST(LENGTH(c_phone) + -5, 0)) AS phone_without_last_5_chars,
  SUBSTRING(c_name FROM GREATEST(LENGTH(c_name) + -1, 1) FOR GREATEST(LENGTH(c_name) + -1 - GREATEST(LENGTH(c_name) + -2, 0), 0)) AS name_second_to_last_char,
  c_acctbal >= 0 AS is_not_in_debt
FROM tpch.customer
