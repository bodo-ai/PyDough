SELECT
  COALESCE(COUNT_IF(LOWER(food_type) = 'italian'), 0) / CASE WHEN COUNT(*) <> 0 THEN COUNT(*) ELSE NULL END AS ratio
FROM main.restaurant
WHERE
  LOWER(city_name) = 'los angeles'
