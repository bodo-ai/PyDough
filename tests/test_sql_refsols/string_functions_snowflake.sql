SELECT
  LOWER(customer.c_name) AS lowercase_name,
  UPPER(customer.c_name) AS uppercase_name,
  LENGTH(customer.c_name) AS name_length,
  STARTSWITH(customer.c_name, 'A') AS starts_with_A,
  ENDSWITH(customer.c_name, 'z') AS ends_with_z,
  CONTAINS(customer.c_name, 'sub') AS contains_sub,
  customer.c_name LIKE '%test%' AS matches_like,
  CONCAT_WS('::', customer.c_name, nation.n_name) AS joined_string,
  LPAD(customer.c_name, 20, '*') AS lpad_name,
  RPAD(customer.c_name, 20, '-') AS rpad_name,
  TRIM(customer.c_name, '\n\t ') AS stripped,
  TRIM(customer.c_name, 'aeiou') AS stripped_vowels,
  REPLACE(customer.c_name, 'Corp', 'Inc') AS replaced_name,
  REPLACE(customer.c_name, 'Ltd', '') AS removed_substr,
  CASE
    WHEN LENGTH('e') = 0
    THEN 0
    ELSE CAST((
      LENGTH(customer.c_name) - LENGTH(REPLACE(customer.c_name, 'e', ''))
    ) / LENGTH('e') AS BIGINT)
  END AS count_e,
  CHARINDEX('Alex', customer.c_name) - 1 AS idx_Alex
FROM tpch.customer AS customer
JOIN tpch.nation AS nation
  ON customer.c_nationkey = nation.n_nationkey
