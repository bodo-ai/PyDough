SELECT
  LOWER(CUSTOMER.C_NAME) AS lowercase_name,
  UPPER(CUSTOMER.C_NAME) AS uppercase_name,
  LENGTH(CUSTOMER.C_NAME) AS name_length,
  CUSTOMER.C_NAME LIKE 'A%' AS starts_with_A,
  CUSTOMER.C_NAME LIKE '%z' AS ends_with_z,
  CUSTOMER.C_NAME LIKE '%sub%' AS contains_sub,
  CUSTOMER.C_NAME LIKE '%test%' AS matches_like,
  NVL(CUSTOMER.C_NAME, '') || '::' || NVL(NATION.N_NAME, '') AS joined_string,
  NULL AS join_nulls,
  LPAD(CUSTOMER.C_NAME, 20, '*') AS lpad_name,
  RPAD(CUSTOMER.C_NAME, 20, '-') AS rpad_name,
  RTRIM(
    LTRIM(CUSTOMER.C_NAME, CONCAT(CHR(  10), CHR(  9), CHR(  13), ' ')),
    CONCAT(CHR(  10), CHR(  9), CHR(  13), ' ')
  ) AS stripped,
  RTRIM(LTRIM(CUSTOMER.C_NAME, 'aeiou'), 'aeiou') AS stripped_vowels,
  REPLACE(CUSTOMER.C_NAME, 'Corp', 'Inc') AS replaced_name,
  REPLACE(CUSTOMER.C_NAME, 'Ltd', '') AS removed_substr,
  CASE
    WHEN LENGTH(CUSTOMER.C_NAME) = 0 OR LENGTH(CUSTOMER.C_NAME) IS NULL
    THEN 0
    ELSE CAST((
      LENGTH(CUSTOMER.C_NAME) - NVL(LENGTH(REPLACE(CUSTOMER.C_NAME, 'e', '')), 0)
    ) AS INT)
  END AS count_e,
  INSTR(CUSTOMER.C_NAME, 'Alex') - 1 AS idx_Alex
FROM TPCH.CUSTOMER CUSTOMER
JOIN TPCH.NATION NATION
  ON CUSTOMER.C_NATIONKEY = NATION.N_NATIONKEY
