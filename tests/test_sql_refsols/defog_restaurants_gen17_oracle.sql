SELECT
  city_name,
  AVG(rating) AS avg_rating
FROM MAIN.RESTAURANT
WHERE
  LOWER(food_type) = 'mexican'
GROUP BY
  city_name
