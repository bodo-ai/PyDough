SELECT
  LOWER(customer.c_name) AS lowercase_name,
  UPPER(customer.c_name) AS uppercase_name,
  LENGTH(customer.c_name) AS name_length,
  customer.c_name LIKE 'A%' AS starts_with_A,
  customer.c_name LIKE '%z' AS ends_with_z,
  customer.c_name LIKE '%sub%' AS contains_sub,
  customer.c_name LIKE '%test%' AS matches_like,
  CONCAT_WS('::', customer.c_name, nation.n_name) AS joined_string,
  CASE
    WHEN LENGTH(customer.c_name) >= 20
    THEN SUBSTRING(customer.c_name, 1, 20)
    ELSE SUBSTRING('********************' || customer.c_name, -20)
  END AS lpad_name,
  SUBSTRING(customer.c_name || '--------------------', 1, 20) AS rpad_name,
  TRIM(customer.c_name, '
	 ') AS stripped,
  TRIM(customer.c_name, 'aeiou') AS stripped_vowels,
  REPLACE(customer.c_name, 'Corp', 'Inc') AS replaced_name,
  REPLACE(customer.c_name, 'Ltd', '') AS removed_substr,
  CASE
    WHEN LENGTH('e') = 0
    THEN 0
    ELSE CAST(CAST(LENGTH(customer.c_name) - LENGTH(REPLACE(customer.c_name, 'e', '')) AS REAL) / LENGTH('e') AS INTEGER)
  END AS count_e,
  INSTR(customer.c_name, 'Alex') - 1 AS idx_Alex,
  INSTR(customer.c_name, 'Rodriguez') - 1 AS idx_Rodriguez,
  INSTR(customer.c_name, 'bob') - 1 AS idx_bob,
  INSTR(customer.c_name, 'e') - 1 AS idx_e,
  INSTR(customer.c_name, ' ') - 1 AS idx_space,
  INSTR(customer.c_name, 'R') - 1 AS idx_of_R,
  INSTR(customer.c_name, 'Alex Rodriguez') - 1 AS idx_of_full
FROM tpch.customer AS customer
JOIN tpch.nation AS nation
  ON customer.c_nationkey = nation.n_nationkey
