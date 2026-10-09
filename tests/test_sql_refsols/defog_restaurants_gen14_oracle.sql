SELECT
  SUM(LOWER(FOOD_TYPE) = 'vegan') / NULLIF(SUM(LOWER(FOOD_TYPE) <> 'vegan'), 0) AS ratio
FROM MAIN.RESTAURANT
WHERE
  LOWER(CITY_NAME) = 'san francisco'
  AND (
    LOWER(FOOD_TYPE) <> 'vegan' OR LOWER(FOOD_TYPE) = 'vegan'
  )
