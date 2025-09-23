SELECT
  description AS condition_description
FROM synthea.conditions
GROUP BY
  1
ORDER BY
  COUNT(*) DESC NULLS LAST,
  1 NULLS FIRST
LIMIT 1
