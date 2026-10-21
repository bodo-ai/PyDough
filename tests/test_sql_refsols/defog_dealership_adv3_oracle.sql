WITH "_S2" AS (
  SELECT DISTINCT
    make AS MAKE,
    model AS MODEL
  FROM MAIN.CARS
  WHERE
    LOWER(vin_number) LIKE '%m5%'
), "_S1" AS (
  SELECT
    car_id AS CAR_ID,
    COUNT(*) AS N_ROWS
  FROM MAIN.SALES
  GROUP BY
    car_id
), "_S3" AS (
  SELECT
    CARS.make AS MAKE,
    CARS.model AS MODEL,
    SUM("_S1".N_ROWS) AS SUM_N_ROWS
  FROM MAIN.CARS CARS
  JOIN "_S1" "_S1"
    ON CARS."_id" = "_S1".CAR_ID
  WHERE
    LOWER(CARS.vin_number) LIKE '%m5%'
  GROUP BY
    CARS.make,
    CARS.model
)
SELECT
  "_S2".MAKE AS make,
  "_S2".MODEL AS model,
  COALESCE("_S3".SUM_N_ROWS, 0) AS num_sales
FROM "_S2" "_S2"
LEFT JOIN "_S3" "_S3"
  ON "_S2".MAKE = "_S3".MAKE AND "_S2".MODEL = "_S3".MODEL
