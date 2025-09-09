SELECT
  LOWER(customer.c_name) AS lowercase_name,
  UPPER(customer.c_name) AS uppercase_name,
  LENGTH(customer.c_name) AS name_length,
  customer.c_name LIKE 'A%' AS starts_with_A,
  customer.c_name LIKE '%z' AS ends_with_z,
  customer.c_name LIKE '%sub%' AS contains_sub,
  customer.c_name LIKE '%test%' AS matches_like,
  CONCAT_WS('::', customer.c_name, nation.n_name) AS joined_string,
  LPAD(CAST(customer.c_name AS TEXT), 20, '*') AS lpad_name,
  RPAD(CAST(customer.c_name AS TEXT), 20, '-') AS rpad_name,
  TRIM('
	 ' FROM customer.c_name) AS stripped,
  TRIM('aeiou' FROM customer.c_name) AS stripped_vowels,
  REPLACE(customer.c_name, 'Corp', 'Inc') AS replaced_name,
  REPLACE(customer.c_name, 'Ltd', '') AS removed_substr,
  CASE
    WHEN LENGTH('e') = 0
    THEN 0
    ELSE CAST(CAST((
      LENGTH(customer.c_name) - LENGTH(REPLACE(customer.c_name, 'e', ''))
    ) AS DOUBLE PRECISION) / LENGTH('e') AS BIGINT)
  END AS count_e,
  POSITION('Alex' IN customer.c_name) - 1 AS idx_Alex
FROM tpch.customer AS customer
JOIN tpch.nation AS nation
  ON customer.c_nationkey = nation.n_nationkey
