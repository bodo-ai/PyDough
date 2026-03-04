SELECT
  CAST(COALESCE(SUM(rating > 4.0), 0) AS DOUBLE) / NULLIF(SUM(rating < 4.0), 0) AS ratio
FROM main.restaurant
WHERE
  LOWER(city_name) = 'new york'
