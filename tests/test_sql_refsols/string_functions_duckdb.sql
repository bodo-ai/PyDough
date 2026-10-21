SELECT
  LOWER(customer.c_name) AS lowercase_name,
  UPPER(customer.c_name) AS uppercase_name,
  LENGTH(customer.c_name) AS name_length,
  customer.c_name LIKE 'A%' AS starts_with_A,
  customer.c_name LIKE '%z' AS ends_with_z,
  customer.c_name LIKE '%sub%' AS contains_sub,
  customer.c_name LIKE '%test%' AS matches_like,
  CASE
    WHEN customer.c_name IS NULL OR nation.n_name IS NULL
    THEN NULL
    ELSE CONCAT_WS('::', customer.c_name, nation.n_name)
  END AS joined_string,
  NULL AS join_nulls,
  CASE
    WHEN LENGTH(customer.c_name) >= 20
    THEN SUBSTRING(customer.c_name, 1, 20)
    ELSE SUBSTRING('********************' || customer.c_name, -20)
  END AS lpad_name,
  SUBSTRING(customer.c_name || '--------------------', 1, 20) AS rpad_name,
  TRIM(customer.c_name, CHR(10) || CHR(9) || CHR(13) || ' ') AS stripped,
  TRIM(customer.c_name, 'aeiou') AS stripped_vowels,
  REPLACE(customer.c_name, 'Corp', 'Inc') AS replaced_name,
  REPLACE(customer.c_name, 'Ltd', '') AS removed_substr,
  CAST((
    LENGTH(customer.c_name) - LENGTH(REPLACE(customer.c_name, 'e', ''))
  ) AS BIGINT) AS count_e,
  STRPOS(customer.c_name, 'Alex') - 1 AS idx_Alex
FROM tpch.customer AS customer
JOIN tpch.nation AS nation
  ON customer.c_nationkey = nation.n_nationkey
