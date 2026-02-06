WITH _T0 AS (
  SELECT
    city_name AS CITY_NAME,
    name AS NAME,
    COUNT(*) AS N_ROWS
  FROM MAIN.RESTAURANT
  GROUP BY
    city_name,
    name
)
SELECT
  CITY_NAME AS city_name,
  NAME AS name,
  N_ROWS AS n_restaurants
FROM _T0
WHERE
  N_ROWS > 1
