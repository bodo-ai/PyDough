WITH _s1 AS (
  SELECT
    city_name,
    region
  FROM restaurants.geographic
), _s6 AS (
  SELECT DISTINCT
    _s1.region
  FROM restaurants.location AS location
  LEFT JOIN _s1 AS _s1
    ON _s1.city_name = location.city_name
), _s7 AS (
  SELECT
    _s3.region,
    SUM(CASE WHEN NOT restaurant.rating IS NULL THEN 1 ELSE 0 END) AS sum_expr,
    SUM(restaurant.rating) AS sum_rating
  FROM restaurants.location AS location
  LEFT JOIN _s1 AS _s3
    ON _s3.city_name = location.city_name
  JOIN restaurants.restaurant AS restaurant
    ON location.restaurant_id = restaurant.id
  GROUP BY
    1
)
SELECT
  _s6.region COLLATE utf8mb4_bin AS region_name,
  _s7.sum_rating / _s7.sum_expr AS avg_rating
FROM _s6 AS _s6
JOIN _s7 AS _s7
  ON _s6.region = _s7.region
ORDER BY
  1,
  2 DESC
