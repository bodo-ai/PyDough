SELECT
  SUBSTRING(c_phone, 1, 3) AS country_code,
  SUBSTRING(c_name, 2) AS name_without_first_char,
  SUBSTRING(c_phone, CASE WHEN ABS(-1) < CHAR_LENGTH(c_phone) THEN -1 ELSE 1 END) AS last_digit,
  SUBSTRING(c_name, 2, GREATEST(CHAR_LENGTH(c_name) + -1 - 1, 0)) AS name_without_start_and_end_char,
  SUBSTRING(c_phone, 1, CHAR_LENGTH(c_phone) + -5) AS phone_without_last_5_chars,
  SUBSTRING(
    c_name,
    CASE WHEN ABS(-2) < CHAR_LENGTH(c_name) THEN -2 ELSE 1 END,
    CASE
      WHEN ABS(-2) > CHAR_LENGTH(c_name)
      THEN CHAR_LENGTH(c_name) + -1
      ELSE GREATEST(1, 0)
    END
  ) AS name_second_to_last_char,
  c_acctbal >= 0 AS is_not_in_debt
FROM tpch.customer
