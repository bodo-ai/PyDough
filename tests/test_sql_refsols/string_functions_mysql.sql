SELECT
  LOWER(CUSTOMER.c_name) AS lowercase_name,
  UPPER(CUSTOMER.c_name) AS uppercase_name,
  CHAR_LENGTH(CUSTOMER.c_name) AS name_length,
  CUSTOMER.c_name LIKE 'A%' AS starts_with_A,
  CUSTOMER.c_name LIKE '%z' AS ends_with_z,
  CUSTOMER.c_name LIKE '%sub%' AS contains_sub,
  CUSTOMER.c_name LIKE '%test%' AS matches_like,
  CONCAT_WS('::', CUSTOMER.c_name, NATION.n_name) AS joined_string,
  LPAD(CUSTOMER.c_name, 20, '*') AS lpad_name,
  RPAD(CUSTOMER.c_name, 20, '-') AS rpad_name,
  TRIM('\n\t ' FROM CUSTOMER.c_name) AS stripped,
  TRIM('aeiou' FROM CUSTOMER.c_name) AS stripped_vowels,
  REPLACE(CUSTOMER.c_name, 'Corp', 'Inc') AS replaced_name,
  REPLACE(CUSTOMER.c_name, 'Ltd', '') AS removed_substr,
  CASE
    WHEN CHAR_LENGTH('e') = 0
    THEN 0
    ELSE CAST(CHAR_LENGTH(CUSTOMER.c_name) - CHAR_LENGTH(REPLACE(CUSTOMER.c_name, 'e', '')) / CHAR_LENGTH('e') AS SIGNED)
  END AS count_e,
  LOCATE('Alex', CUSTOMER.c_name) - 1 AS idx_Alex
FROM tpch.CUSTOMER AS CUSTOMER
JOIN tpch.NATION AS NATION
  ON CUSTOMER.c_nationkey = NATION.n_nationkey
