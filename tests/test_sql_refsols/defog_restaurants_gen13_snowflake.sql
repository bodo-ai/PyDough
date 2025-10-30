WITH _s0 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM main.restaurant
  WHERE
    LOWER(city_name) = 'new york' AND rating > 4.0
), _s1 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM main.restaurant
  WHERE
    LOWER(city_name) = 'new york' AND rating < 4.0
)
SELECT
  _s0.n_rows / CASE WHEN _s1.n_rows > 0 THEN _s1.n_rows ELSE NULL END AS ratio
FROM _s0 AS _s0
CROSS JOIN _s1 AS _s1
