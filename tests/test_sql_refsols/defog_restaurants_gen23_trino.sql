SELECT
  restaurant.name,
  restaurant.food_type
FROM mongo.defog.location AS location
JOIN cassandra.defog.restaurant AS restaurant
  ON location.restaurant_id = restaurant.id
WHERE
  LOWER(location.city_name) = 'san francisco'
  AND LOWER(location.street_name) = 'market st'
