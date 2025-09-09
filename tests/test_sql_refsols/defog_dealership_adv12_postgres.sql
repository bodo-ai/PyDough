WITH _t1 AS (
  SELECT
    MAX(sales.car_id) AS anything_car_id,
    MAX(cars.make) AS anything_make,
    MAX(cars.model) AS anything_model,
    MAX(sales.sale_price) AS anything_sale_price,
    cars._id AS _id_1
  FROM main.sales AS sales
  JOIN main.cars AS cars
    ON cars._id = sales.car_id
  JOIN main.inventory_snapshots AS inventory_snapshots
    ON NOT inventory_snapshots.is_in_inventory
    AND cars._id = inventory_snapshots.car_id
    AND inventory_snapshots.snapshot_date = sales.sale_date
  GROUP BY
    sales._id,
    5
)
SELECT
  anything_make AS make,
  anything_model AS model,
  anything_sale_price AS sale_price
FROM _t1
WHERE
  _id_1 = anything_car_id
ORDER BY
  3 DESC NULLS LAST
LIMIT 1
