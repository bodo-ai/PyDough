WITH _s0 AS (
  SELECT
    city_name,
    region
  FROM main.geographic
), _s1 AS (
  SELECT
    city_name
  FROM main.restaurant
), _u_0 AS (
  SELECT
    city_name AS _u_1
  FROM _s1
  GROUP BY
    1
), _s6 AS (
  SELECT DISTINCT
    _s0.region
  FROM _s0 AS _s0
  LEFT JOIN _u_0 AS _u_0
    ON _s0.city_name = _u_0._u_1
  WHERE
    NOT _u_0._u_1 IS NULL
), _u_2 AS (
  SELECT
    city_name AS _u_3
  FROM _s1
  GROUP BY
    1
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
    CAST(SUM(_s5.sum_rating) AS DOUBLE PRECISION) / SUM(_s5.count_rating) AS avg_rating,
    _s2.region
  FROM _s0 AS _s2
  LEFT JOIN _u_2 AS _u_2
    ON _s2.city_name = _u_2._u_3
  JOIN _s5 AS _s5
    ON _s2.city_name = _s5.city_name
  WHERE
    NOT _u_2._u_3 IS NULL
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
  1 NULLS FIRST
