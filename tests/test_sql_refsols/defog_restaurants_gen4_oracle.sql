SELECT
  city_name,
  COUNT(*) AS num_restaurants
FROM MAIN.RESTAURANT
WHERE
  LOWER(food_type) = 'italian'
GROUP BY
  city_name
ORDER BY
  2 DESC NULLS LAST,
  1 DESC NULLS LAST
