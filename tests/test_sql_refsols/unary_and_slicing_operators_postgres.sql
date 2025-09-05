SELECT
  SUBSTRING(c_phone FROM 1 FOR 3) AS country_code,
  SUBSTRING(c_name FROM 2) AS name_without_first_char,
  SUBSTRING(c_phone FROM CASE WHEN ABS(-1) < LENGTH(c_phone) THEN LENGTH(c_phone) + 0 ELSE 1 END) AS last_digit,
  SUBSTRING(c_name FROM 2 FOR GREATEST(LENGTH(c_name) + -1 - 1, 0)) AS name_without_start_and_end_char,
  SUBSTRING(c_phone FROM 1 FOR LENGTH(c_phone) + -5) AS phone_without_last_5_chars,
  SUBSTRING(c_name FROM CASE WHEN ABS(-2) < LENGTH(c_name) THEN LENGTH(c_name) + -1 ELSE 1 END FOR CASE WHEN ABS(-2) > LENGTH(c_name) THEN LENGTH(c_name) + -1 ELSE GREATEST(1, 0) END) AS name_second_to_last_char,
  c_acctbal >= 0 AS is_not_in_debt
FROM tpch.customer
