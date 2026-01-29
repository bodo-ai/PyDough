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
    COUNT_IF(NOT restaurant.rating IS NULL) AS sum_present_rating,
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
  _s7.sum_rating / _s7.sum_present_rating AS avg_rating
FROM _s6 AS _s6
JOIN _s7 AS _s7
  ON _s6.region = _s7.region
ORDER BY
  1 NULLS FIRST,
  2 DESC NULLS LAST
