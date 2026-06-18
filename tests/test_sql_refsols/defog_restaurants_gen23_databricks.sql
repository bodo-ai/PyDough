SELECT
  restaurant.name,
  restaurant.food_type
FROM main.location AS location
JOIN main.restaurant AS restaurant
  ON location.restaurant_id = restaurant.id
WHERE
  LOWER(location.city_name) = 'san francisco'
  AND LOWER(location.street_name) = 'market st'
