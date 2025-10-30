WITH _s1 AS (
  SELECT
    city_name,
    region
  FROM main.geographic
)
SELECT
  _s1.region COLLATE utf8mb4_bin AS rest_region,
  COUNT(*) AS n_restaurants
FROM main.restaurant AS restaurant
LEFT JOIN _s1 AS _s1
  ON _s1.city_name = restaurant.city_name
GROUP BY
  1
ORDER BY
  2 DESC,
  1
