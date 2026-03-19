WITH _s0 AS (
  SELECT
    city_name,
    region
  FROM main.geographic
), _s1 AS (
  SELECT
    city_name
  FROM main.restaurant
), _s6 AS (
  SELECT DISTINCT
    _s0.region
  FROM _s0 AS _s0
  JOIN _s1 AS _s1
    ON _s0.city_name = _s1.city_name
), _s5 AS (
  SELECT
    city_name,
    COUNT(rating) AS count_rating,
    SUM(rating) AS sum_rating
  FROM main.restaurant
  GROUP BY
    1
), _s7 AS (
  SELECT
    SUM(_s5.sum_rating) / SUM(_s5.count_rating) AS avg_rating,
    _s2.region
  FROM _s0 AS _s2
  JOIN _s1 AS _s3
    ON _s2.city_name = _s3.city_name
  JOIN _s5 AS _s5
    ON _s2.city_name = _s5.city_name
  GROUP BY
    2
)
SELECT
  _s6.region AS rest_region,
  _s7.avg_rating
FROM _s6 AS _s6
LEFT JOIN _s7 AS _s7
  ON _s6.region = _s7.region
ORDER BY
  1
