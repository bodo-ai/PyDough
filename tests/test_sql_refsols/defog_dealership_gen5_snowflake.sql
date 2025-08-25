WITH _T0 AS (
  SELECT
    car_id AS CAR_ID
  FROM MAIN.INVENTORY_SNAPSHOTS
  WHERE
    MONTH(CAST(snapshot_date AS TIMESTAMP)) = 3
    AND YEAR(CAST(snapshot_date AS TIMESTAMP)) = 2023
  QUALIFY
    is_in_inventory AND RANK() OVER (ORDER BY snapshot_date DESC) = 1
)
SELECT
  CARS._id,
  CARS.make,
  CARS.model,
  CARS.year
FROM _T0 AS _T0
JOIN MAIN.CARS AS CARS
  ON CARS._id = _T0.CAR_ID
