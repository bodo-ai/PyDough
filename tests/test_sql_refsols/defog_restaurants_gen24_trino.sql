SELECT
  name
FROM cassandra.defog.restaurant
WHERE
  LOWER(food_type) = 'italian'
