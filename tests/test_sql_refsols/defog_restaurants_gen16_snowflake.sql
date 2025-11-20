WITH _t0 AS (
  SELECT
    city_name,
    name,
    COUNT(*) AS n_rows
  FROM main.restaurant
  GROUP BY
    1,
    2
)
SELECT
  city_name,
  name,
  n_rows AS n_restaurants
FROM _t0
WHERE
  n_rows > 1
