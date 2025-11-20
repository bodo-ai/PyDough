WITH _s1 AS (
  SELECT
    city_name,
    region
  FROM main.geographic
), _s6 AS (
  SELECT DISTINCT
    _s1.region
  FROM main.location AS location
  LEFT JOIN _s1 AS _s1
    ON _s1.city_name = location.city_name
), _s7 AS (
  SELECT
    _s3.region,
    SUM(IIF(NOT restaurant.rating IS NULL, 1, 0)) AS sum_expr1,
    SUM(restaurant.rating) AS sum_rating
  FROM main.location AS location
  LEFT JOIN _s1 AS _s3
    ON _s3.city_name = location.city_name
  JOIN main.restaurant AS restaurant
    ON location.restaurant_id = restaurant.id
  GROUP BY
    1
)
SELECT
  _s6.region AS region_name,
  CAST(_s7.sum_rating AS REAL) / _s7.sum_expr1 AS avg_rating
FROM _s6 AS _s6
JOIN _s7 AS _s7
  ON _s6.region = _s7.region
ORDER BY
  1,
  2 DESC
