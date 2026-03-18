SELECT
  city_name,
  AVG(rating) AS avg_rating
FROM postgres.restaurant
WHERE
  LOWER(food_type) = 'mexican'
GROUP BY
  1
