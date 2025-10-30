WITH _s0 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM main.restaurant
  WHERE
    LOWER(city_name) = 'san francisco' AND LOWER(food_type) = 'vegan'
), _s1 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM main.restaurant
  WHERE
    LOWER(city_name) = 'san francisco' AND LOWER(food_type) <> 'vegan'
)
SELECT
  CAST(_s0.n_rows AS DOUBLE PRECISION) / CASE WHEN _s1.n_rows > 0 THEN _s1.n_rows ELSE NULL END AS ratio
FROM _s0 AS _s0
CROSS JOIN _s1 AS _s1
