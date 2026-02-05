SELECT
  NVL(SUM(rating > 4.0), 0) / NULLIF(SUM(rating < 4.0), 0) AS ratio
FROM MAIN.RESTAURANT
WHERE
  LOWER(city_name) = 'new york'
