SELECT
  LOWER(customer.c_name) AS lowercase_name,
  UPPER(customer.c_name) AS uppercase_name,
  CHAR_LENGTH(customer.c_name) AS name_length,
  customer.c_name LIKE 'A%' AS starts_with_A,
  customer.c_name LIKE '%z' AS ends_with_z,
  customer.c_name LIKE '%sub%' AS contains_sub,
  customer.c_name LIKE '%test%' AS matches_like,
  CONCAT_WS('::', customer.c_name, nation.n_name) AS joined_string,
  LPAD(customer.c_name, 20, '*') AS lpad_name,
  RPAD(customer.c_name, 20, '-') AS rpad_name,
  CASE
    WHEN customer.c_name = '\\s'
    THEN ''
    ELSE REGEXP_REPLACE(
      CAST(customer.c_name AS CHAR) COLLATE utf8mb4_bin,
      CONCAT(
        '^[',
        REGEXP_REPLACE('\\s', '([]\\[\\^\\-])', '\\\\\\1'),
        ']+|[',
        REGEXP_REPLACE('\\s', '([]\\[\\^\\-])', '\\\\\\1'),
        ']+$'
      ),
      ''
    )
  END AS stripped,
  CASE
    WHEN customer.c_name = 'aeiou'
    THEN ''
    ELSE REGEXP_REPLACE(
      CAST(customer.c_name AS CHAR) COLLATE utf8mb4_bin,
      CONCAT(
        '^[',
        REGEXP_REPLACE('aeiou', '([]\\[\\^\\-])', '\\\\\\1'),
        ']+|[',
        REGEXP_REPLACE('aeiou', '([]\\[\\^\\-])', '\\\\\\1'),
        ']+$'
      ),
      ''
    )
  END AS stripped_vowels,
  REPLACE(customer.c_name, 'Corp', 'Inc') AS replaced_name,
  REPLACE(customer.c_name, 'Ltd', '') AS removed_substr,
  CASE
    WHEN CHAR_LENGTH('e') = 0
    THEN 0
    ELSE CAST((
      CHAR_LENGTH(customer.c_name) - CHAR_LENGTH(REPLACE(customer.c_name, 'e', ''))
    ) / CHAR_LENGTH('e') AS SIGNED)
  END AS count_e,
  LOCATE('Alex', customer.c_name) - 1 AS idx_Alex
FROM tpch.customer AS customer
JOIN tpch.nation AS nation
  ON customer.c_nationkey = nation.n_nationkey
