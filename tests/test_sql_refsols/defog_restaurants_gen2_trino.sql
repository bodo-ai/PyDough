SELECT
  city_name,
  COUNT(*) AS total_count
FROM postgres.main.location
GROUP BY
  1
