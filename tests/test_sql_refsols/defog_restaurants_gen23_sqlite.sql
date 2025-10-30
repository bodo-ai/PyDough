WITH _s1 AS (
  SELECT
    food_type,
    id,
    name
  FROM main.restaurant
)
SELECT
  _s1.name,
  _s1.food_type
FROM main.location AS location
LEFT JOIN _s1 AS _s1
  ON _s1.id = location.restaurant_id
WHERE
  LOWER(location.street_name) = 'market st'
