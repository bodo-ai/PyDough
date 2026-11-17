SELECT
  city_name,
  AVG(CAST(rating AS DOUBLE)) AS avg_rating
FROM cassandra.defog.restaurant
WHERE
  LOWER(food_type) = 'mexican'
GROUP BY
  1
