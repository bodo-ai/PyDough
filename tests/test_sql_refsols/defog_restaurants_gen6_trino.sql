SELECT
  street_name
FROM mongo.defog.location
GROUP BY
  1
ORDER BY
  COUNT(*) DESC
LIMIT 1
