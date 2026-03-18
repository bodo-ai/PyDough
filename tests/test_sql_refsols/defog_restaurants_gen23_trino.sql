SELECT
  restaurant.name,
  restaurant.food_type
FROM postgres.location AS location
JOIN postgres.restaurant AS restaurant
  ON location.restaurant_id = restaurant.id
WHERE
  LOWER(location.city_name) = 'san francisco'
  AND LOWER(location.street_name) = 'market st'
