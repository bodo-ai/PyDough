WITH _t1 AS (
  SELECT
    MAX(cars._id) AS anything__id_1,
    MAX(sales.car_id) AS anything_car_id,
    MAX(cars.make) AS anything_make,
    MAX(cars.model) AS anything_model,
    MAX(sales.sale_price) AS anything_sale_price
  FROM main.sales AS sales
  JOIN main.cars AS cars
    ON cars._id = sales.car_id
  JOIN main.inventory_snapshots AS inventory_snapshots
    ON cars._id = inventory_snapshots.car_id
    AND inventory_snapshots.is_in_inventory = 0
    AND inventory_snapshots.snapshot_date = sales.sale_date
  GROUP BY
    sales._id,
    cars._id
)
SELECT
  anything_make AS make,
  anything_model AS model,
  anything_sale_price AS sale_price
FROM _t1
WHERE
  anything__id_1 = anything_car_id
ORDER BY
  anything_sale_price DESC
LIMIT 1
