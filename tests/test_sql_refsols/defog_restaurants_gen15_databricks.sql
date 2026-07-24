SELECT
  COUNT_IF(LOWER(food_type) = 'italian') / NULLIF(COUNT(*), 0) AS ratio
FROM defog.restaurants.restaurant
WHERE
  LOWER(city_name) = 'los angeles'
