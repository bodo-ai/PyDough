WITH "_u_0" AS (
  SELECT
    CAR_ID AS "_u_1"
  FROM MAIN.SALES
  GROUP BY
    CAR_ID
)
SELECT
  CARS."_id",
  CARS.MAKE AS make,
  CARS.MODEL AS model,
  CARS.YEAR AS year
FROM MAIN.CARS CARS
LEFT JOIN "_u_0" "_u_0"
  ON CARS."_id" = "_u_0"."_u_1"
WHERE
  "_u_0"."_u_1" IS NULL
