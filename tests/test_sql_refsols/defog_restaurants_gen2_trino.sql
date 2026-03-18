SELECT
  city_name,
  COUNT(*) AS total_count
FROM postgres.location
GROUP BY
  1
