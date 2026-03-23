SELECT
  CAST(COUNT_IF(rating > 4.0) AS DOUBLE) / NULLIF(COUNT_IF(rating < 4.0), 0) AS ratio
FROM postgres.main.restaurant
WHERE
  LOWER(city_name) = 'new york'
