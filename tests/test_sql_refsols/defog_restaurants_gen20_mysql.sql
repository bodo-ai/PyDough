WITH _s0 AS (
  SELECT
    city_name,
    COUNT(*) AS n_rows
  FROM restaurants.restaurant
  GROUP BY
    1
)
SELECT
  geographic.region COLLATE utf8mb4_bin AS rest_region,
  SUM(_s0.n_rows) AS n_restaurants
FROM _s0 AS _s0
JOIN restaurants.geographic AS geographic
  ON _s0.city_name = geographic.city_name
GROUP BY
  1
ORDER BY
  2 DESC,
  1
