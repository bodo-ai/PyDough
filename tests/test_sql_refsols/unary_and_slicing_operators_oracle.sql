SELECT
  SUBSTR(C_PHONE, 1, 3) AS country_code,
  SUBSTR(C_NAME, 2) AS name_without_first_char,
  SUBSTR(C_PHONE, CASE WHEN ABS(-1) < LENGTH(C_PHONE) THEN -1 ELSE 1 END) AS last_digit,
  SUBSTR(C_NAME, 2, GREATEST(LENGTH(C_NAME) + -1 - 1, 0)) AS name_without_start_and_end_char,
  SUBSTR(C_PHONE, 1, LENGTH(C_PHONE) + -5) AS phone_without_last_5_chars,
  SUBSTR(
    C_NAME,
    CASE WHEN ABS(-2) < LENGTH(C_NAME) THEN -2 ELSE 1 END,
    CASE WHEN ABS(-2) > LENGTH(C_NAME) THEN LENGTH(C_NAME) + -1 ELSE GREATEST(1, 0) END
  ) AS name_second_to_last_char,
  C_ACCTBAL >= 0 AS is_not_in_debt
FROM TPCH.CUSTOMER
