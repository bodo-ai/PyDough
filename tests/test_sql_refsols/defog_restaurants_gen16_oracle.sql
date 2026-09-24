WITH "_t0" AS (
  SELECT
    CITY_NAME,
    NAME,
    COUNT(*) AS N_ROWS
  FROM MAIN.RESTAURANT
  GROUP BY
    CITY_NAME,
    NAME
)
SELECT
  CITY_NAME AS city_name,
  NAME AS name,
  N_ROWS AS n_restaurants
FROM "_t0"
WHERE
  N_ROWS > 1
