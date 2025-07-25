SELECT
  SUBSTRING(c_phone FROM 1 FOR 3) AS country_code,
  SUBSTRING(c_name FROM 2) AS name_without_first_char,
  SUBSTRING(c_phone FROM CASE WHEN (
    LENGTH(c_phone) + 0
  ) < 1 THEN 1 ELSE (
    LENGTH(c_phone) + 0
  ) END) AS last_digit,
  SUBSTRING(c_name FROM 2 FOR CASE
    WHEN (
      LENGTH(c_name) + 0
    ) < 1
    THEN 0
    ELSE CASE
      WHEN (
        (
          LENGTH(c_name) + 0
        ) - 2
      ) <= 0
      THEN ''
      ELSE (
        LENGTH(c_name) + 0
      ) - 2
    END
  END) AS name_without_start_and_end_char,
  SUBSTRING(c_phone FROM 1 FOR CASE WHEN (
    LENGTH(c_phone) + -5
  ) < 0 THEN 0 ELSE (
    LENGTH(c_phone) + -5
  ) END) AS phone_without_last_5_chars,
  SUBSTRING(c_name FROM CASE WHEN (
    LENGTH(c_name) + -1
  ) < 1 THEN 1 ELSE (
    LENGTH(c_name) + -1
  ) END FOR CASE
    WHEN (
      LENGTH(c_name) + 0
    ) < 1
    THEN 0
    ELSE (
      LENGTH(c_name) + 0
    ) - CASE WHEN (
      LENGTH(c_name) + -1
    ) < 1 THEN 1 ELSE (
      LENGTH(c_name) + -1
    ) END
  END) AS name_second_to_last_char,
  c_acctbal >= 0 AS is_not_in_debt
FROM tpch.customer
