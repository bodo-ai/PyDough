WITH _t0_2 AS (
  SELECT
    cars_2.make AS make_19,
    cars_2.model AS model_20,
    sales.sale_price AS ordering_0,
    sales.sale_price
  FROM main.sales AS sales
  JOIN main.cars AS cars
    ON cars._id = sales.car_id
  JOIN main.inventory_snapshots AS inventory_snapshots
    ON cars._id = inventory_snapshots.car_id
    AND inventory_snapshots.is_in_inventory = 0
    AND inventory_snapshots.snapshot_date = sales.sale_date
  JOIN main.cars AS cars_2
    ON cars_2._id = inventory_snapshots.car_id
  ORDER BY
    ordering_0 DESC
  LIMIT 1
)
SELECT
  make_19 AS make,
  model_20 AS model,
  sale_price
FROM _t0_2
ORDER BY
  ordering_0 DESC
