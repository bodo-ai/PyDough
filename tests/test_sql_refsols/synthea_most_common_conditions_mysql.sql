SELECT
  description COLLATE utf8mb4_bin AS condition_description
FROM synthea.conditions
GROUP BY
  1
ORDER BY
  COUNT(*) DESC,
  1
LIMIT 1
