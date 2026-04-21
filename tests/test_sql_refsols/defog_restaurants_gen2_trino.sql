SELECT
  city_name,
  COUNT(*) AS total_count
FROM mongo.defog.location
GROUP BY
  1
