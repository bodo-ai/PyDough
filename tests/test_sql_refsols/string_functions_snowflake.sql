SELECT
  LOWER(CUSTOMER.c_name) AS lowercase_name,
  UPPER(CUSTOMER.c_name) AS uppercase_name,
  LENGTH(CUSTOMER.c_name) AS name_length,
  STARTSWITH(CUSTOMER.c_name, 'A') AS starts_with_A,
  ENDSWITH(CUSTOMER.c_name, 'z') AS ends_with_z,
  CONTAINS(CUSTOMER.c_name, 'sub') AS contains_sub,
  CUSTOMER.c_name LIKE '%test%' AS matches_like,
  CONCAT_WS('::', CUSTOMER.c_name, NATION.n_name) AS joined_string,
  LPAD(CUSTOMER.c_name, 20, '*') AS lpad_name,
  RPAD(CUSTOMER.c_name, 20, '-') AS rpad_name,
  TRIM(CUSTOMER.c_name, '\n\t ') AS stripped,
  TRIM(CUSTOMER.c_name, 'aeiou') AS stripped_vowels,
  REPLACE(CUSTOMER.c_name, 'Corp', 'Inc') AS replaced_name,
  REPLACE(CUSTOMER.c_name, 'Ltd', '') AS removed_substr,
  REGEXP_COUNT(CUSTOMER.c_name, 'e') AS count_e,
  CHARINDEX('Alex', CUSTOMER.c_name) - 1 AS idx_Alex
FROM TPCH.CUSTOMER AS CUSTOMER
JOIN TPCH.NATION AS NATION
  ON CUSTOMER.c_nationkey = NATION.n_nationkey
