SELECT
  food_type,
  AVG(CAST(rating AS DECIMAL)) AS avg_rating
FROM main.restaurant
GROUP BY
  1
ORDER BY
  2 DESC NULLS LAST,
  1 DESC NULLS LAST
