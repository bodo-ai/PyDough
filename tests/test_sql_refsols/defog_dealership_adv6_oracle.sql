WITH "_T" AS (
  SELECT
    car_id AS CAR_ID,
    is_in_inventory AS IS_IN_INVENTORY,
    ROW_NUMBER() OVER (PARTITION BY car_id ORDER BY snapshot_date DESC) AS "_W"
  FROM MAIN.INVENTORY_SNAPSHOTS
)
SELECT
  ANY_VALUE(CARS.make) AS make,
  ANY_VALUE(CARS.model) AS model,
  MAX(SALES.sale_price) AS highest_sale_price
FROM MAIN.CARS CARS
JOIN "_T" "_T"
  ON CARS."_id" = "_T".CAR_ID AND NOT "_T".IS_IN_INVENTORY AND "_T"."_W" = 1
LEFT JOIN MAIN.SALES SALES
  ON CARS."_id" = SALES.car_id
GROUP BY
  CARS."_id"
ORDER BY
  3 DESC NULLS LAST
