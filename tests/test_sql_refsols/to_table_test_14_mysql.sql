SELECT
  `key`,
  name COLLATE utf8mb4_bin AS name
FROM sorted_nations_t14
ORDER BY
  2 DESC
LIMIT 5
