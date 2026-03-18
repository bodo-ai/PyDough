SELECT
  CAST(COUNT_IF(LOWER(food_type) = 'italian') AS DOUBLE) / NULLIF(COUNT(*), 0) AS ratio
FROM postgres.restaurant
WHERE
  LOWER(city_name) = 'los angeles'
