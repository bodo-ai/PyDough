SELECT
  SUBSTRING(c_phone, 1, 3) AS country_code,
  SUBSTRING(c_name, 2) AS name_without_first_char,
  SUBSTRING(
    c_phone,
    CASE WHEN (
      LENGTH(c_phone) + 0
    ) < 1 THEN 1 ELSE (
      LENGTH(c_phone) + 0
    ) END
  ) AS last_digit,
  SUBSTRING(
    c_name,
    2,
    CASE
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
    END
  ) AS name_without_start_and_end_char,
  SUBSTRING(
    c_phone,
    1,
    CASE WHEN (
      LENGTH(c_phone) + -5
    ) < 0 THEN 0 ELSE (
      LENGTH(c_phone) + -5
    ) END
  ) AS phone_without_last_5_chars,
  SUBSTRING(
    c_name,
    CASE WHEN (
      LENGTH(c_name) + -1
    ) < 1 THEN 1 ELSE (
      LENGTH(c_name) + -1
    ) END,
    CASE
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
    END
  ) AS name_second_to_last_char,
  0 - c_acctbal AS complement_balance
FROM tpch.customer
