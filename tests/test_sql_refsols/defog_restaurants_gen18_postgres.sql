WITH _s0 AS (
  SELECT
    city_name,
    region
  FROM main.geographic
), _s1 AS (
  SELECT DISTINCT
    city_name
  FROM main.restaurant
), _s6 AS (
  SELECT DISTINCT
    _s0.region
  FROM _s0 AS _s0
  JOIN _s1 AS _s1
    ON _s0.city_name = _s1.city_name
), _s7 AS (
  SELECT
    _s2.region,
    AVG(CAST(restaurant.rating AS DECIMAL)) AS avg_rating
  FROM _s0 AS _s2
  JOIN _s1 AS _s3
    ON _s2.city_name = _s3.city_name
  JOIN main.restaurant AS restaurant
    ON _s2.city_name = restaurant.city_name
  GROUP BY
    1
)
SELECT
  _s6.region AS rest_region,
  _s7.avg_rating
FROM _s6 AS _s6
LEFT JOIN _s7 AS _s7
  ON _s6.region = _s7.region
ORDER BY
  1 NULLS FIRST
