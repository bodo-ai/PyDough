SELECT
  LOWER(CUSTOMER.c_name) AS lowercase_name,
  UPPER(CUSTOMER.c_name) AS uppercase_name,
  LENGTH(CUSTOMER.c_name) AS name_length,
  CUSTOMER.c_name LIKE 'A%' AS starts_with_A,
  CUSTOMER.c_name LIKE '%z' AS ends_with_z,
  CUSTOMER.c_name LIKE '%sub%' AS contains_sub,
  CUSTOMER.c_name LIKE '%test%' AS matches_like,
  LTRIM(
    NVL2(CUSTOMER.c_name, '::' || CUSTOMER.c_name, NULL) || NVL2(NATION.n_name, '::' || NATION.n_name, NULL),
    '::'
  ) AS joined_string,
  CASE
    WHEN LENGTH(CUSTOMER.c_name) >= 20
    THEN SUBSTR(CUSTOMER.c_name, 1, 20)
    ELSE SUBSTR(CONCAT('********************', CUSTOMER.c_name), -20)
  END AS lpad_name,
  SUBSTR(CONCAT(CUSTOMER.c_name, '--------------------'), 1, 20) AS rpad_name,
  TRIM(CUSTOMER.c_name) AS stripped,
  TRIM('aeiou' FROM CUSTOMER.c_name) AS stripped_vowels,
  REPLACE(CUSTOMER.c_name, 'Corp', 'Inc') AS replaced_name,
  REPLACE(CUSTOMER.c_name, 'Ltd', '') AS removed_substr,
  CASE
    WHEN LENGTH('e') = 0
    THEN 0
    ELSE CAST((
      LENGTH(CUSTOMER.c_name) - LENGTH(REPLACE(CUSTOMER.c_name, 'e', ''))
    ) / LENGTH('e') AS INT)
  END AS count_e,
  INSTR(CUSTOMER.c_name, 'Alex') AS idx_Alex
FROM TPCH.CUSTOMER CUSTOMER
JOIN TPCH.NATION NATION
  ON CUSTOMER.c_nationkey = NATION.n_nationkey
