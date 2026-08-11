SELECT
  CAST(COALESCE(SUM(IIF(rating > 4.0, 1, 0)), 0) AS REAL) / NULLIF(SUM(IIF(rating < 4.0, 1, 0)), 0) AS ratio
FROM main.restaurant
WHERE
  LOWER(city_name) = 'new york'
