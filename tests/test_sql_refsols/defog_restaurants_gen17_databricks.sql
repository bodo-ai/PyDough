SELECT
  city_name,
  AVG(rating) AS avg_rating
FROM defog.restaurants.restaurant
WHERE
  LOWER(food_type) = 'mexican'
GROUP BY
  1
