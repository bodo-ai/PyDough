WITH _t1 AS (
  SELECT
    cars._id AS _id_1,
    ANY_VALUE(sales.car_id) AS anything_car_id,
    ANY_VALUE(cars.make) AS anything_make,
    ANY_VALUE(cars.model) AS anything_model,
    ANY_VALUE(sales.sale_price) AS anything_sale_price
  FROM main.sales AS sales
  JOIN main.cars AS cars
    ON cars._id = sales.car_id
  JOIN main.inventory_snapshots AS inventory_snapshots
    ON cars._id = inventory_snapshots.car_id
    AND inventory_snapshots.is_in_inventory = 0
    AND inventory_snapshots.snapshot_date = sales.sale_date
  GROUP BY
    sales._id,
    1
)
SELECT
  anything_make AS make,
  anything_model AS model,
  anything_sale_price AS sale_price
FROM _t1
WHERE
  _id_1 = anything_car_id
ORDER BY
  3 DESC
LIMIT 1
