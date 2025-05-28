SELECT
  cars_2.make,
  cars_2.model,
  sales.sale_price
FROM main.sales AS sales
JOIN main.cars AS cars
  ON cars._id = sales.car_id
JOIN main.inventory_snapshots AS inventory_snapshots
  ON cars._id = inventory_snapshots.car_id AND inventory_snapshots.is_in_inventory = 0
JOIN main.cars AS cars_2
  ON cars_2._id = inventory_snapshots.car_id
WHERE
  inventory_snapshots.snapshot_date = sales.sale_date
ORDER BY
  sale_price DESC
LIMIT 1
