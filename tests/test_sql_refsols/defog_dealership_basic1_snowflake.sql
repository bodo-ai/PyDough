WITH _S1 AS (
  SELECT
    SALES.car_id AS CAR_ID
  FROM MAIN.SALES AS SALES
), _S0 AS (
  SELECT
    CARS._id AS _ID,
    CARS.make AS MAKE,
    CARS.model AS MODEL,
    CARS.year AS YEAR
  FROM MAIN.CARS AS CARS
)
SELECT
  _S0._ID AS _id,
  _S0.MAKE AS make,
  _S0.MODEL AS model,
  _S0.YEAR AS year
FROM _S0 AS _S0
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM _S1 AS _S1
    WHERE
      _S0._ID = _S1.CAR_ID
  )
