SELECT
  LOWER(CUSTOMER.c_name) AS lowercase_name,
  UPPER(CUSTOMER.c_name) AS uppercase_name,
  LENGTH(CUSTOMER.c_name) AS name_length,
  CUSTOMER.c_name LIKE 'A%' AS starts_with_A,
  CUSTOMER.c_name LIKE '%z' AS ends_with_z,
  CUSTOMER.c_name LIKE '%sub%' AS contains_sub,
  CUSTOMER.c_name LIKE '%test%' AS matches_like,
  NVL(CUSTOMER.c_name, '') || '::' || NVL(NATION.n_name, '') AS joined_string,
  NULL AS join_nulls,
  LPAD(CUSTOMER.c_name, 20, '*') AS lpad_name,
  RPAD(CUSTOMER.c_name, 20, '-') AS rpad_name,
  RTRIM(
    LTRIM(CUSTOMER.c_name, CONCAT(CHR(10), CHR(9), CHR(13), ' ')),
    CONCAT(CHR(10), CHR(9), CHR(13), ' ')
  ) AS stripped,
  RTRIM(LTRIM(CUSTOMER.c_name, 'aeiou'), 'aeiou') AS stripped_vowels,
  REPLACE(CUSTOMER.c_name, 'Corp', 'Inc') AS replaced_name,
  REPLACE(CUSTOMER.c_name, 'Ltd', '') AS removed_substr,
  CASE
    WHEN LENGTH('e') IS NULL OR LENGTH(CUSTOMER.c_name) IS NULL
    THEN 0
    ELSE CAST((
      LENGTH(CUSTOMER.c_name) - NVL(LENGTH(REPLACE(CUSTOMER.c_name, 'e', '')), 0)
    ) / LENGTH('e') AS INT)
  END AS count_e,
  INSTR(CUSTOMER.c_name, 'Alex') - 1 AS idx_Alex
FROM TPCH.CUSTOMER CUSTOMER
JOIN TPCH.NATION NATION
  ON CUSTOMER.c_nationkey = NATION.n_nationkey
